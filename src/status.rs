//! Report-delta merging and status normalisation.
//!
//! Bambu printers publish `{"print": {"command": "push_status", ...}}` on
//! `device/{serial}/report`. X1 firmware tends to send full objects; P1/A1
//! send a full object only in answer to `pushing.pushall` and partial deltas
//! afterwards. We keep the merged `print` object as the cached full state.

use serde::Serialize;
use serde_json::{Map, Value};

/// Deep-merge `delta` into `base`: objects merge key by key, everything else
/// (scalars, arrays) replaces. Arrays replace wholesale — `hms` in particular
/// must be able to shrink back to `[]` once an error is cleared.
pub fn merge(base: &mut Value, delta: &Value) {
    match (base, delta) {
        (Value::Object(b), Value::Object(d)) => {
            for (k, v) in d {
                match b.get_mut(k) {
                    Some(existing) if existing.is_object() && v.is_object() => merge(existing, v),
                    _ => {
                        b.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        (b, d) => *b = d.clone(),
    }
}

/// Apply one MQTT report payload to the cached `print` state. Only
/// `push_status` reports carry printer state; command acknowledgements
/// (`project_file`, `pause`, ...) share the `print` key but would pollute
/// it with request fields, so they are ignored. Returns whether the state
/// changed.
pub fn apply_report(state: &mut Value, report: &Value) -> bool {
    let Some(print) = report.get("print") else {
        return false;
    };
    if print.get("command").and_then(Value::as_str) != Some("push_status") {
        return false;
    }
    if !state.is_object() {
        *state = Value::Object(Map::new());
    }
    merge(state, print);
    true
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Temp {
    pub actual: f64,
    pub target: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct Temps {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nozzle: Option<Temp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bed: Option<Temp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chamber: Option<Temp>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Status {
    pub state: &'static str,
    pub progress: f64,
    pub file: Option<String>,
    pub elapsed_s: Option<u64>,
    pub eta_s: Option<u64>,
    pub layer: Option<u64>,
    pub total_layers: Option<u64>,
    pub temps: Temps,
    pub message: Option<String>,
}

/// Numbers arrive as JSON numbers or numeric strings depending on field and
/// firmware (`"mc_print_stage": "1"`, `"gcode_start_time": "1700000000"`).
fn num(v: &Value, key: &str) -> Option<f64> {
    match v.get(key)? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn non_empty_str(v: &Value, key: &str) -> Option<String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

fn temp(v: &Value, actual: &str, target: &str) -> Option<Temp> {
    Some(Temp {
        actual: num(v, actual)?,
        target: num(v, target),
    })
}

/// `0x0300_4000`-style Bambu error code, as shown on the printer screen.
fn fmt_error(code: u64) -> String {
    format!("{:04X}_{:04X}", (code >> 16) & 0xFFFF, code & 0xFFFF)
}

fn message(p: &Value) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(code) = num(p, "print_error").map(|c| c as u64).filter(|c| *c != 0) {
        parts.push(format!("print_error {}", fmt_error(code)));
    }
    if let Some(hms) = p.get("hms").and_then(Value::as_array) {
        for h in hms {
            let (Some(attr), Some(code)) = (num(h, "attr"), num(h, "code")) else {
                continue;
            };
            let (attr, code) = (attr as u64, code as u64);
            parts.push(format!(
                "HMS_{:04X}_{:04X}_{:04X}_{:04X}",
                (attr >> 16) & 0xFFFF,
                attr & 0xFFFF,
                (code >> 16) & 0xFFFF,
                code & 0xFFFF
            ));
        }
    }
    (!parts.is_empty()).then(|| parts.join("; "))
}

/// Normalise the cached `print` state into the bridge's status shape.
/// `connected` is the MQTT link state; `now_s` is the current unix time
/// (a parameter so tests are deterministic).
pub fn normalise(p: &Value, connected: bool, now_s: u64) -> Status {
    let gcode_state = p.get("gcode_state").and_then(Value::as_str).unwrap_or("");
    let state = if !connected {
        "offline"
    } else {
        match gcode_state {
            "RUNNING" | "PREPARE" | "SLICING" => "printing",
            "PAUSE" => "paused",
            "FINISH" => "finished",
            "FAILED" => "error",
            _ => "idle",
        }
    };
    let active = matches!(gcode_state, "RUNNING" | "PREPARE" | "SLICING" | "PAUSE");

    let progress = (num(p, "mc_percent").unwrap_or(0.0) / 100.0).clamp(0.0, 1.0);
    let file = non_empty_str(p, "subtask_name").or_else(|| non_empty_str(p, "gcode_file"));
    let elapsed_s = num(p, "gcode_start_time")
        .map(|t| t as u64)
        .filter(|t| active && *t > 0 && *t <= now_s)
        .map(|t| now_s - t);
    let eta_s = num(p, "mc_remaining_time")
        .filter(|_| active)
        .map(|m| (m.max(0.0) * 60.0) as u64);
    let layer = num(p, "layer_num").map(|n| n as u64);
    let total_layers = num(p, "total_layer_num")
        .map(|n| n as u64)
        .filter(|n| *n > 0);

    let temps = Temps {
        nozzle: temp(p, "nozzle_temper", "nozzle_target_temper"),
        bed: temp(p, "bed_temper", "bed_target_temper"),
        chamber: temp(p, "chamber_temper", "chamber_target_temper"),
    };

    Status {
        state,
        progress,
        file,
        elapsed_s,
        eta_s,
        layer,
        total_layers,
        temps,
        message: message(p),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Trimmed full `pushall` answer from a P1S (shape per OpenBambuAPI).
    fn full_report() -> Value {
        json!({"print": {
            "command": "push_status",
            "sequence_id": "2021",
            "msg": 0,
            "gcode_state": "RUNNING",
            "gcode_file": "/data/Metadata/plate_1.gcode",
            "subtask_name": "benchy",
            "gcode_start_time": "1700000000",
            "mc_percent": 42,
            "mc_remaining_time": 37,
            "mc_print_stage": "2",
            "layer_num": 61,
            "total_layer_num": 240,
            "nozzle_temper": 219.8,
            "nozzle_target_temper": 220.0,
            "bed_temper": 54.9,
            "bed_target_temper": 55.0,
            "chamber_temper": 31.0,
            "print_error": 0,
            "hms": [],
            "lights_report": [{"node": "chamber_light", "mode": "on"}],
            "ipcam": {"ipcam_dev": "1", "timelapse": "disable", "resolution": "1080p"},
            "upgrade_state": {"status": "IDLE", "progress": "0"}
        }})
    }

    #[test]
    fn delta_merges_into_full_state() {
        let mut state = Value::Null;
        assert!(apply_report(&mut state, &full_report()));
        // P1-style delta: a handful of fields plus a nested partial object.
        let delta = json!({"print": {
            "command": "push_status",
            "sequence_id": "2022",
            "mc_percent": 43,
            "layer_num": 62,
            "nozzle_temper": 220.1,
            "ipcam": {"timelapse": "enable"}
        }});
        assert!(apply_report(&mut state, &delta));
        assert_eq!(state["mc_percent"], 43);
        assert_eq!(state["layer_num"], 62);
        assert_eq!(state["bed_temper"], 54.9, "untouched fields survive");
        assert_eq!(state["subtask_name"], "benchy");
        assert_eq!(state["ipcam"]["timelapse"], "enable");
        assert_eq!(
            state["ipcam"]["resolution"], "1080p",
            "nested merge keeps siblings"
        );
    }

    #[test]
    fn arrays_replace_so_cleared_hms_clears() {
        let mut state = Value::Null;
        apply_report(&mut state, &full_report());
        let err = json!({"print": {"command": "push_status",
            "hms": [{"attr": 0x0300_0100u64, "code": 0x0002_0001u64}]}});
        apply_report(&mut state, &err);
        assert_eq!(
            normalise(&state, true, 1_700_000_600).message.as_deref(),
            Some("HMS_0300_0100_0002_0001")
        );
        let clear = json!({"print": {"command": "push_status", "hms": []}});
        apply_report(&mut state, &clear);
        assert_eq!(normalise(&state, true, 1_700_000_600).message, None);
    }

    #[test]
    fn command_acks_do_not_pollute_state() {
        let mut state = Value::Null;
        apply_report(&mut state, &full_report());
        let ack = json!({"print": {"command": "project_file", "sequence_id": "7",
            "param": "Metadata/plate_1.gcode", "result": "success", "gcode_state": "BOGUS"}});
        assert!(!apply_report(&mut state, &ack));
        assert_eq!(state["gcode_state"], "RUNNING");
        assert!(state.get("param").is_none());
        // Non-print topics (info/system) are ignored too.
        assert!(!apply_report(
            &mut state,
            &json!({"info": {"command": "get_version"}})
        ));
    }

    #[test]
    fn normalises_printing_state() {
        let mut state = Value::Null;
        apply_report(&mut state, &full_report());
        let s = normalise(&state, true, 1_700_000_600);
        assert_eq!(s.state, "printing");
        assert!((s.progress - 0.42).abs() < 1e-9);
        assert_eq!(s.file.as_deref(), Some("benchy"));
        assert_eq!(s.elapsed_s, Some(600));
        assert_eq!(s.eta_s, Some(37 * 60));
        assert_eq!((s.layer, s.total_layers), (Some(61), Some(240)));
        assert_eq!(
            s.temps.nozzle,
            Some(Temp {
                actual: 219.8,
                target: Some(220.0)
            })
        );
        assert_eq!(
            s.temps.bed,
            Some(Temp {
                actual: 54.9,
                target: Some(55.0)
            })
        );
        assert_eq!(
            s.temps.chamber,
            Some(Temp {
                actual: 31.0,
                target: None
            })
        );
        assert_eq!(s.message, None);

        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(v["temps"]["chamber"]["target"], Value::Null);
        assert_eq!(v["state"], "printing");
    }

    #[test]
    fn maps_every_gcode_state() {
        for (raw, want) in [
            ("IDLE", "idle"),
            ("PREPARE", "printing"),
            ("RUNNING", "printing"),
            ("PAUSE", "paused"),
            ("FINISH", "finished"),
            ("FAILED", "error"),
            ("", "idle"),
        ] {
            let p = json!({"gcode_state": raw});
            assert_eq!(normalise(&p, true, 0).state, want, "{raw}");
        }
        assert_eq!(
            normalise(&json!({"gcode_state": "RUNNING"}), false, 0).state,
            "offline"
        );
    }

    #[test]
    fn idle_and_failed_details() {
        // Idle A1 mini: no chamber sensor, stale remaining time ignored,
        // zero layers reported as null.
        let p = json!({"gcode_state": "IDLE", "mc_percent": "100", "mc_remaining_time": 5,
            "gcode_file": "", "subtask_name": "", "total_layer_num": 0,
            "nozzle_temper": 24, "nozzle_target_temper": 0, "bed_temper": 23.5,
            "bed_target_temper": 0});
        let s = normalise(&p, true, 1_700_000_000);
        assert_eq!(s.state, "idle");
        assert_eq!(s.progress, 1.0);
        assert_eq!(
            (s.file, s.eta_s, s.elapsed_s, s.total_layers),
            (None, None, None, None)
        );
        assert!(s.temps.chamber.is_none());

        let failed = json!({"gcode_state": "FAILED", "print_error": 50348044u64});
        let s = normalise(&failed, true, 0);
        assert_eq!(s.state, "error");
        assert_eq!(s.message.as_deref(), Some("print_error 0300_400C"));
    }
}
