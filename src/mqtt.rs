//! One long-lived MQTT connection per printer: subscribe to the report
//! topic, request a full `pushall` on every (re)connect and every few
//! minutes, merge deltas into the cached state, reconnect with backoff.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS, TlsConfiguration, Transport};
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use crate::status;
use crate::store::PrinterConfig;
use crate::tls;

pub const MQTT_PORT: u16 = 8883;
const PUSHALL_EVERY: Duration = Duration::from_secs(300);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Live state shared between the MQTT task and the HTTP handlers.
#[derive(Default)]
pub struct Live {
    pub state: Mutex<Value>,
    pub connected: AtomicBool,
    client: Mutex<Option<AsyncClient>>,
    seq: AtomicU64,
}

#[derive(Debug)]
pub enum CommandError {
    Offline,
    Other(String),
}

impl Live {
    pub fn next_seq(&self) -> String {
        self.seq.fetch_add(1, Ordering::Relaxed).to_string()
    }

    /// Publish a request. Refused while disconnected: rumqttc would queue it
    /// and replay it after reconnect, and a `stop` delivered minutes late is
    /// worse than an error now.
    pub fn publish(&self, serial: &str, payload: &Value) -> Result<(), CommandError> {
        if !self.connected.load(Ordering::Acquire) {
            return Err(CommandError::Offline);
        }
        let client = self
            .client
            .lock()
            .unwrap()
            .clone()
            .ok_or(CommandError::Offline)?;
        client
            .try_publish(
                format!("device/{serial}/request"),
                QoS::AtMostOnce,
                false,
                serde_json::to_vec(payload).expect("json"),
            )
            .map_err(|e| CommandError::Other(e.to_string()))
    }
}

fn pushall(seq: String) -> Value {
    json!({"pushing": {"sequence_id": seq, "command": "pushall",
        "version": 1, "push_target": 1}})
}

/// Spawn the connection task. Abort the handle to disconnect.
pub fn spawn(id: String, cfg: PrinterConfig, live: Arc<Live>) -> JoinHandle<()> {
    tokio::spawn(async move { run(id, cfg, live).await })
}

async fn run(id: String, cfg: PrinterConfig, live: Arc<Live>) {
    let client_id = format!(
        "print-bridge-{id}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() % 1_000_000)
            .unwrap_or(0)
    );
    let mut opts = MqttOptions::new(client_id, cfg.host.clone(), MQTT_PORT);
    opts.set_credentials("bblp", cfg.access_code.clone());
    opts.set_keep_alive(Duration::from_secs(30));
    // A full pushall report with AMS data runs to tens of KB.
    opts.set_max_packet_size(4 * 1024 * 1024, 256 * 1024);
    opts.set_transport(Transport::tls_with_config(TlsConfiguration::Rustls(
        tls::client_config(),
    )));

    let (client, mut events) = AsyncClient::new(opts, 32);
    *live.client.lock().unwrap() = Some(client.clone());
    let report_topic = format!("device/{}/report", cfg.serial);
    let request_topic = format!("device/{}/request", cfg.serial);

    let mut backoff = Duration::from_secs(1);
    let mut pushall_timer = tokio::time::interval(PUSHALL_EVERY);
    pushall_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            ev = events.poll() => match ev {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    tracing::info!(printer = %id, host = %cfg.host, "MQTT connected");
                    backoff = Duration::from_secs(1);
                    if let Err(e) = client.try_subscribe(&report_topic, QoS::AtMostOnce) {
                        tracing::warn!(printer = %id, "subscribe failed: {e}");
                    }
                    let _ = client.try_publish(
                        &request_topic,
                        QoS::AtMostOnce,
                        false,
                        serde_json::to_vec(&pushall(live.next_seq())).unwrap(),
                    );
                    live.connected.store(true, Ordering::Release);
                }
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    match serde_json::from_slice::<Value>(&p.payload) {
                        Ok(report) => {
                            let mut st = live.state.lock().unwrap();
                            status::apply_report(&mut st, &report);
                            if let Some(print) = report.get("print")
                                && print.get("result").and_then(Value::as_str)
                                    .is_some_and(|r| r.eq_ignore_ascii_case("failed"))
                            {
                                tracing::warn!(printer = %id, reply = %print, "printer rejected command");
                            }
                        }
                        Err(e) => tracing::debug!(printer = %id, "non-JSON report: {e}"),
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    if live.connected.swap(false, Ordering::AcqRel) {
                        tracing::warn!(printer = %id, "MQTT disconnected: {e}");
                    } else {
                        tracing::debug!(printer = %id, "MQTT connect failed: {e}");
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            },
            _ = pushall_timer.tick() => {
                if live.connected.load(Ordering::Acquire) {
                    let _ = client.try_publish(
                        &request_topic,
                        QoS::AtMostOnce,
                        false,
                        serde_json::to_vec(&pushall(live.next_seq())).unwrap(),
                    );
                }
            }
        }
    }
}
