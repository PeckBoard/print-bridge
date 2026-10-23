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
    /// Why the last connect attempt failed; cleared on connect. Shown as the
    /// status `message` while offline so a failure is never silent.
    pub last_error: Mutex<Option<String>>,
    /// The serial read from the printer's TLS certificate when it differs
    /// from the configured one. Topics use it instead: a wrong serial makes
    /// the printer drop the connection right after login.
    pub detected_serial: Mutex<Option<String>>,
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
        let serial = self
            .detected_serial
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| serial.to_owned());
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
    // A ping is declared lost when its ack hasn't arrived by the next ping;
    // on weak printer Wi-Fi a 30 s interval dropped the link every few
    // minutes ("Last pingreq isn't acked"). The printer pushes reports every
    // second while printing, so the session stays verified anyway.
    opts.set_keep_alive(Duration::from_secs(60));
    // A full pushall report with AMS data runs to tens of KB.
    opts.set_max_packet_size(4 * 1024 * 1024, 256 * 1024);
    let cert_cn: tls::CnSink = Default::default();
    opts.set_transport(Transport::tls_with_config(TlsConfiguration::Rustls(
        tls::client_config_with_cn(cert_cn.clone()),
    )));

    let (client, mut events) = AsyncClient::new(opts, 32);
    // The printer's TLS handshake is slow; rumqttc's 5 s default times out
    // on roughly every other attempt.
    events.network_options.set_connection_timeout(20);
    *live.client.lock().unwrap() = Some(client.clone());
    let mut serial = cfg.serial.clone();
    let mut report_topic = format!("device/{serial}/report");
    let mut request_topic = format!("device/{serial}/request");
    let mut backoff = Duration::from_secs(1);
    let mut pushall_timer = tokio::time::interval(PUSHALL_EVERY);
    pushall_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            ev = events.poll() => match ev {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    tracing::info!(printer = %id, host = %cfg.host, "MQTT connected");
                    let cn = cert_cn.lock().unwrap().clone();
                    if let Some(cn) = cn.filter(|cn| !cn.is_empty() && *cn != serial) {
                        tracing::warn!(
                            printer = %id,
                            "configured serial {} does not match the printer's certificate ({cn}); using {cn}",
                            cfg.serial
                        );
                        serial = cn.clone();
                        report_topic = format!("device/{serial}/report");
                        request_topic = format!("device/{serial}/request");
                        *live.detected_serial.lock().unwrap() = Some(cn);
                    }
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
                    *live.last_error.lock().unwrap() = None;
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
                    let msg = format!("MQTT connection to {}:{MQTT_PORT} failed: {e}", cfg.host);
                    if live.connected.swap(false, Ordering::AcqRel) {
                        tracing::warn!(printer = %id, "MQTT disconnected: {e}");
                    } else {
                        // Warn once per distinct error, not every backoff tick.
                        let prev = live.last_error.lock().unwrap().clone();
                        if prev.as_deref() != Some(msg.as_str()) {
                            tracing::warn!(printer = %id, "{msg}");
                        }
                    }
                    *live.last_error.lock().unwrap() = Some(msg);
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
