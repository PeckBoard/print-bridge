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

/// How long a command waits for the printer's reply before it is reported
/// as sent-but-unconfirmed.
pub const REPLY_WAIT: Duration = Duration::from_secs(6);

/// Where requests go: the live MQTT client, or a recording sink in tests.
#[derive(Clone)]
enum Publisher {
    Mqtt(AsyncClient),
    #[cfg(test)]
    Fake(Arc<Mutex<Vec<Value>>>),
}

/// A command waiting for the report that echoes its `command` and
/// `sequence_id`.
struct Pending {
    command: String,
    seq: String,
    tx: tokio::sync::oneshot::Sender<Value>,
}

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
    /// Main firmware version from the `info.get_version` reply.
    pub firmware: Mutex<Option<String>>,
    client: Mutex<Option<Publisher>>,
    pending: Mutex<Vec<Pending>>,
    seq: AtomicU64,
}

#[derive(Debug)]
pub enum CommandError {
    Offline,
    Other(String),
    /// The printer answered `result: failed`.
    Refused {
        reason: Option<String>,
        err_code: Option<u64>,
        /// The refusal is the LAN-mode signature check: the printer needs
        /// LAN Only + Developer Mode to accept unsigned commands.
        needs_developer_mode: bool,
    },
}

/// A command the printer did not refuse. `confirmed` is false when no reply
/// arrived within [`REPLY_WAIT`] (sent, but the outcome is unknown).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Ack {
    pub confirmed: bool,
}

/// `mqtt message verify failed` / malformed signature.
const SIGNATURE_ERRORS: [u64; 2] = [0x0502_4007, 0x0502_4009];

/// Turn a reply (the inner object, e.g. `report.print`) into the command's
/// outcome.
pub fn classify_reply(reply: &Value) -> Result<Ack, CommandError> {
    let result = reply.get("result").and_then(Value::as_str).unwrap_or("");
    if !result.eq_ignore_ascii_case("failed") && !result.eq_ignore_ascii_case("fail") {
        return Ok(Ack { confirmed: true });
    }
    let reason = reply
        .get("reason")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);
    let err_code = match reply.get("err_code") {
        Some(Value::Number(n)) => n.as_u64(),
        Some(Value::String(s)) => s.trim().parse().ok(),
        _ => None,
    };
    let needs_developer_mode = err_code.is_some_and(|c| SIGNATURE_ERRORS.contains(&c))
        || reason
            .as_deref()
            .is_some_and(|r| r.to_ascii_lowercase().contains("verify failed"));
    Err(CommandError::Refused {
        reason,
        err_code,
        needs_developer_mode,
    })
}

fn seq_str(v: &Value) -> Option<String> {
    match v.get("sequence_id")? {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
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
        match client {
            Publisher::Mqtt(client) => client
                .try_publish(
                    format!("device/{serial}/request"),
                    QoS::AtMostOnce,
                    false,
                    serde_json::to_vec(payload).expect("json"),
                )
                .map_err(|e| CommandError::Other(e.to_string())),
            #[cfg(test)]
            Publisher::Fake(sent) => {
                sent.lock().unwrap().push(payload.clone());
                Ok(())
            }
        }
    }

    /// Publish `payload` (`{"print"|"system": {command, sequence_id, ...}}`)
    /// and wait up to `wait` for the report echoing its command and
    /// sequence id.
    pub async fn command(
        &self,
        serial: &str,
        payload: &Value,
        wait: Duration,
    ) -> Result<Ack, CommandError> {
        let inner = payload
            .as_object()
            .and_then(|o| o.values().next())
            .cloned()
            .unwrap_or(Value::Null);
        let command = inner.get("command").and_then(Value::as_str).unwrap_or("");
        let Some(seq) = seq_str(&inner) else {
            self.publish(serial, payload)?;
            return Ok(Ack { confirmed: false });
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.pending.lock().unwrap().push(Pending {
            command: command.to_owned(),
            seq: seq.clone(),
            tx,
        });
        let drop_pending = || {
            self.pending
                .lock()
                .unwrap()
                .retain(|p| !(p.seq == seq && p.command == command));
        };
        if let Err(e) = self.publish(serial, payload) {
            drop_pending();
            return Err(e);
        }
        match tokio::time::timeout(wait, rx).await {
            Ok(Ok(reply)) => classify_reply(&reply),
            _ => {
                drop_pending();
                Ok(Ack { confirmed: false })
            }
        }
    }

    /// Hand a report to the command waiting for it, if any. Returns whether
    /// it matched.
    pub fn dispatch_reply(&self, report: &Value) -> bool {
        let Some(obj) = report.as_object() else {
            return false;
        };
        for inner in obj.values() {
            let (Some(command), Some(seq)) =
                (inner.get("command").and_then(Value::as_str), seq_str(inner))
            else {
                continue;
            };
            let mut pending = self.pending.lock().unwrap();
            if let Some(i) = pending
                .iter()
                .position(|p| p.command == command && p.seq == seq)
            {
                let p = pending.swap_remove(i);
                let _ = p.tx.send(inner.clone());
                return true;
            }
        }
        false
    }

    /// Route publishes into `sent` and mark the link up (tests).
    #[cfg(test)]
    pub fn fake_connected(&self) -> Arc<Mutex<Vec<Value>>> {
        let sent = Arc::new(Mutex::new(Vec::new()));
        *self.client.lock().unwrap() = Some(Publisher::Fake(sent.clone()));
        self.connected.store(true, Ordering::Release);
        sent
    }
}

fn pushall(seq: String) -> Value {
    json!({"pushing": {"sequence_id": seq, "command": "pushall",
        "version": 1, "push_target": 1}})
}

fn get_version(seq: String) -> Value {
    json!({"info": {"sequence_id": seq, "command": "get_version"}})
}

/// The main (`ota`) firmware version from a `get_version` reply.
pub fn firmware_from_info(report: &Value) -> Option<String> {
    let info = report.get("info")?;
    if info.get("command").and_then(Value::as_str) != Some("get_version") {
        return None;
    }
    info.get("module")?
        .as_array()?
        .iter()
        .find(|m| m.get("name").and_then(Value::as_str) == Some("ota"))?
        .get("sw_ver")?
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
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
    *live.client.lock().unwrap() = Some(Publisher::Mqtt(client.clone()));
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
                    for req in [pushall(live.next_seq()), get_version(live.next_seq())] {
                        let _ = client.try_publish(
                            &request_topic,
                            QoS::AtMostOnce,
                            false,
                            serde_json::to_vec(&req).unwrap(),
                        );
                    }
                    live.connected.store(true, Ordering::Release);
                    *live.last_error.lock().unwrap() = None;
                }
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    match serde_json::from_slice::<Value>(&p.payload) {
                        Ok(report) => {
                            {
                                let mut st = live.state.lock().unwrap();
                                status::apply_report(&mut st, &report);
                            }
                            if let Some(fw) = firmware_from_info(&report) {
                                *live.firmware.lock().unwrap() = Some(fw);
                            }
                            live.dispatch_reply(&report);
                            if let Some(obj) = report.as_object() {
                                for inner in obj.values() {
                                    if inner.get("result").and_then(Value::as_str)
                                        .is_some_and(|r| r.eq_ignore_ascii_case("failed"))
                                    {
                                        tracing::warn!(printer = %id, reply = %inner, "printer rejected command");
                                    }
                                }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Wait until the fake sink has `n` payloads, then return the last.
    async fn sent_nth(sent: &Arc<Mutex<Vec<Value>>>, n: usize) -> Value {
        for _ in 0..200 {
            if let Some(v) = sent.lock().unwrap().get(n - 1) {
                return v.clone();
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("nothing published");
    }

    #[tokio::test]
    async fn refusal_reply_is_matched_by_command_and_sequence() {
        let live = Arc::new(Live::default());
        let sent = live.fake_connected();
        let l = live.clone();
        let task = tokio::spawn(async move {
            let payload = json!({"print": {"sequence_id": "3", "command": "project_file"}});
            l.command("SERIAL", &payload, Duration::from_secs(5)).await
        });
        sent_nth(&sent, 1).await;
        // Unrelated traffic first: a status push and another command's echo.
        assert!(
            !live.dispatch_reply(&json!({"print": {"command": "push_status", "sequence_id": "3"}}))
        );
        assert!(!live.dispatch_reply(
            &json!({"print": {"command": "project_file", "sequence_id": "2", "result": "success"}})
        ));
        assert!(
            live.dispatch_reply(&json!({"print": {"command": "project_file",
            "sequence_id": "3", "result": "failed", "reason": "mqtt message verify failed",
            "err_code": 84033543u64}}))
        );
        match task.await.unwrap() {
            Err(CommandError::Refused {
                reason,
                err_code,
                needs_developer_mode,
            }) => {
                assert_eq!(reason.as_deref(), Some("mqtt message verify failed"));
                assert_eq!(err_code, Some(0x0502_4007));
                assert!(needs_developer_mode);
            }
            other => panic!("{other:?}"),
        }
        assert!(live.pending.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn success_system_reply_and_timeout() {
        let live = Arc::new(Live::default());
        let sent = live.fake_connected();
        let l = live.clone();
        let task = tokio::spawn(async move {
            let payload = json!({"system": {"sequence_id": "9", "command": "ledctrl"}});
            l.command("S", &payload, Duration::from_secs(5)).await
        });
        sent_nth(&sent, 1).await;
        // The printer may echo the sequence id as a number.
        live.dispatch_reply(
            &json!({"system": {"command": "ledctrl", "sequence_id": 9, "result": "success"}}),
        );
        assert_eq!(task.await.unwrap().unwrap(), Ack { confirmed: true });

        let payload = json!({"print": {"sequence_id": "10", "command": "pause"}});
        let ack = live
            .command("S", &payload, Duration::from_millis(30))
            .await
            .unwrap();
        assert_eq!(ack, Ack { confirmed: false });
        assert!(
            live.pending.lock().unwrap().is_empty(),
            "timed-out entry dropped"
        );
    }

    #[test]
    fn other_failures_do_not_ask_for_developer_mode() {
        match classify_reply(&json!({"result": "failed", "reason": "busy", "err_code": "1234"})) {
            Err(CommandError::Refused {
                needs_developer_mode,
                err_code,
                ..
            }) => {
                assert!(!needs_developer_mode);
                assert_eq!(err_code, Some(1234));
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            classify_reply(&json!({"result": "failed", "err_code": 0x0502_4009u64})),
            Err(CommandError::Refused {
                needs_developer_mode: true,
                ..
            })
        ));
    }

    #[test]
    fn offline_commands_are_refused() {
        let live = Live::default();
        let r = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(live.command(
                "S",
                &json!({"print": {"sequence_id": "1", "command": "stop"}}),
                REPLY_WAIT,
            ));
        assert!(matches!(r, Err(CommandError::Offline)));
        assert!(live.pending.lock().unwrap().is_empty());
    }

    #[test]
    fn firmware_version_from_info() {
        let r = json!({"info": {"command": "get_version", "sequence_id": "1", "module": [
            {"name": "mc", "sw_ver": "00.00.01"},
            {"name": "ota", "sw_ver": "01.07.00.00"}]}});
        assert_eq!(firmware_from_info(&r).as_deref(), Some("01.07.00.00"));
        assert_eq!(firmware_from_info(&json!({"print": {}})), None);
    }
}
