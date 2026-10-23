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
/// must be able to shrink back to `[]` once an error is cleared. The AMS
/// unit list (`ams.ams`) and each unit's `tray` list are the exception: P1/A1
/// deltas carry only the units/trays (and fields) that changed, so those
/// merge element by element on their `id`.
pub fn merge(base: &mut Value, delta: &Value) {
    match (base, delta) {
        (Value::Object(b), Value::Object(d)) => {
            for (k, v) in d {
                match b.get_mut(k) {
                    Some(existing) if existing.is_object() && v.is_object() => merge(existing, v),
                    Some(Value::Array(existing)) if (k == "ams" || k == "tray") && v.is_array() => {
                        merge_by_id(existing, v.as_array().unwrap());
                    }
                    _ => {
                        b.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        (b, d) => *b = d.clone(),
    }
}

/// Merge `delta` elements into `base` by their `id`; new ids append, ids
/// absent from `delta` are kept. Elements without an id replace by position.
fn merge_by_id(base: &mut Vec<Value>, delta: &[Value]) {
    for (i, d) in delta.iter().enumerate() {
        let pos = match d.get("id") {
            Some(id) => base.iter().position(|b| b.get("id") == Some(id)),
            None => (i < base.len()).then_some(i),
        };
        match pos {
            Some(p) => merge(&mut base[p], d),
            None => base.push(d.clone()),
        }
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

/// One loaded AMS tray. `slot` is the global tray index Bambu's
/// `ams_mapping` uses (`unit * 4 + tray`).
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AmsTray {
    pub slot: u32,
    pub unit: u32,
    pub tray: u32,
    #[serde(rename = "type")]
    pub kind: String,
    pub color: Option<String>,
}

/// The external spool holder (`vt_tray`).
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Spool {
    #[serde(rename = "type")]
    pub kind: String,
    pub color: Option<String>,
}

/// Loaded filament: AMS trays (empty trays omitted) and the external spool.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Ams {
    pub trays: Vec<AmsTray>,
    pub external: Option<Spool>,
    /// The spool feeding the toolhead (`tray_now`): a global slot number,
    /// `"external"`, or `null` (255 = none / unknown).
    pub active: Option<Value>,
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
    /// `null` when the printer reports no AMS and no external spool.
    pub ams: Option<Ams>,
    /// Speed profile: 1 silent, 2 standard, 3 sport, 4 ludicrous.
    pub speed_level: Option<u8>,
    pub fans: Fans,
    /// Chamber light, `"on"` / `"off"`.
    pub light: Option<&'static str>,
    pub wifi_signal: Option<String>,
    /// Filled from the `get_version` reply by the HTTP layer.
    pub firmware: Option<String>,
    /// `null` unless the printer reports its `fun` bitfield.
    pub developer_mode: Option<bool>,
    /// Active HMS errors, then a nonzero `print_error`.
    pub errors: Vec<PrinterError>,
}

/// Fan speeds in percent; absent fans are omitted.
#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct Fans {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub part: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aux: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chamber: Option<u8>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PrinterError {
    /// `0500_0500_0001_0007` (HMS) or `0502_4007` (print_error).
    pub code: String,
    /// Known description, else `null` (see `url`).
    pub text: Option<String>,
    pub severity: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
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
/// Bambu bitmasks (`ams_exist_bits`, `tray_exist_bits`) arrive as hex strings.
fn hex_bits(v: &Value, key: &str) -> Option<u64> {
    match v.get(key)? {
        Value::String(s) => u64::from_str_radix(s.trim(), 16).ok(),
        Value::Number(n) => n.as_u64(),
        _ => None,
    }
}

fn index(v: &Value) -> Option<u32> {
    num(v, "id").map(|n| n as u32)
}

/// Loaded filament from the cached state. A tray counts as loaded when it
/// reports a `tray_type` and, when the printer sends `tray_exist_bits`, its
/// bit is set — merged deltas can leave a stale type on an emptied tray.
fn ams(p: &Value) -> Option<Ams> {
    let block = p.get("ams");
    let unit_bits = block.and_then(|a| hex_bits(a, "ams_exist_bits"));
    let tray_bits = block.and_then(|a| hex_bits(a, "tray_exist_bits"));
    let mut trays = Vec::new();
    let units = block
        .and_then(|a| a.get("ams"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    for u in units {
        let Some(unit) = index(u) else { continue };
        if unit_bits.is_some_and(|b| unit >= 64 || b & (1 << unit) == 0) {
            continue;
        }
        let unit_trays = u.get("tray").and_then(Value::as_array);
        for t in unit_trays.map(Vec::as_slice).unwrap_or_default() {
            let (Some(tray), Some(kind)) = (index(t), non_empty_str(t, "tray_type")) else {
                continue;
            };
            let slot = unit * 4 + tray;
            if tray_bits.is_some_and(|b| slot >= 64 || b & (1 << slot) == 0) {
                continue;
            }
            trays.push(AmsTray {
                slot,
                unit,
                tray,
                kind,
                color: non_empty_str(t, "tray_color"),
            });
        }
    }
    trays.sort_by_key(|t| t.slot);
    let external = p.get("vt_tray").and_then(|v| {
        Some(Spool {
            kind: non_empty_str(v, "tray_type")?,
            color: non_empty_str(v, "tray_color"),
        })
    });
    let active = block
        .and_then(|a| num(a, "tray_now"))
        .and_then(|n| match n as u64 {
            255 => None,
            254 => Some(Value::from("external")),
            s if s < 64 => Some(Value::from(s)),
            _ => None,
        });
    (!trays.is_empty() || external.is_some()).then_some(Ams {
        trays,
        external,
        active,
    })
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
        ams: ams(p),
        speed_level: num(p, "spd_lvl")
            .map(|n| n as u8)
            .filter(|n| (1..=4).contains(n)),
        fans: Fans {
            part: fan_pct(p, "cooling_fan_speed"),
            aux: fan_pct(p, "big_fan1_speed"),
            chamber: fan_pct(p, "big_fan2_speed"),
        },
        light: light(p),
        wifi_signal: non_empty_str(p, "wifi_signal"),
        firmware: None,
        developer_mode: developer_mode(p),
        errors: errors(p),
    }
}

/// Bambu fan speeds are 0-15 steps (numbers or numeric strings) → percent.
fn fan_pct(p: &Value, key: &str) -> Option<u8> {
    num(p, key).map(|v| ((v.clamp(0.0, 15.0) / 15.0) * 100.0).round() as u8)
}

fn light(p: &Value) -> Option<&'static str> {
    let lights = p.get("lights_report")?.as_array()?;
    let chamber = lights
        .iter()
        .find(|l| l.get("node").and_then(Value::as_str) == Some("chamber_light"))?;
    match chamber.get("mode").and_then(Value::as_str)? {
        "on" | "flashing" => Some("on"),
        "off" => Some("off"),
        _ => None,
    }
}

/// `fun` bit 29 means the printer requires signed MQTT commands (as
/// ha-bambulab reads it): Developer Mode is off. `None` when not reported.
fn developer_mode(p: &Value) -> Option<bool> {
    let fun = hex_bits(p, "fun")?;
    Some(fun & 0x2000_0000 == 0)
}

/// Built-in descriptions for the codes we know how to explain.
const KNOWN_ERRORS: [(&str, &str); 4] = [
    (
        "0500_0500_0001_0007",
        "MQTT command verification failed — enable LAN Only + Developer Mode on the printer",
    ),
    (
        "0502_4007",
        "Printer refused an unsigned command — enable LAN Only + Developer Mode on the printer",
    ),
    (
        "0502_4009",
        "Printer refused a command with a malformed signature — enable LAN Only + Developer Mode on the printer",
    ),
    ("0300_400C", "The print was cancelled"),
];

fn known_text(code: &str) -> Option<String> {
    KNOWN_ERRORS
        .iter()
        .find(|(c, _)| *c == code)
        .map(|(_, t)| (*t).to_owned())
}

/// HMS severity from the high word of `code` (Bambu: 1 fatal, 2 serious,
/// 3 common, 4 info).
fn hms_severity(code: u64) -> &'static str {
    match (code >> 16) & 0xFFFF {
        1 => "fatal",
        2 => "serious",
        3 => "common",
        4 => "info",
        _ => "unknown",
    }
}

fn errors(p: &Value) -> Vec<PrinterError> {
    let mut out = Vec::new();
    if let Some(hms) = p.get("hms").and_then(Value::as_array) {
        for h in hms {
            let (Some(attr), Some(code)) = (num(h, "attr"), num(h, "code")) else {
                continue;
            };
            let (attr, code) = (attr as u64, code as u64);
            let id = format!(
                "{:04X}_{:04X}_{:04X}_{:04X}",
                (attr >> 16) & 0xFFFF,
                attr & 0xFFFF,
                (code >> 16) & 0xFFFF,
                code & 0xFFFF
            );
            out.push(PrinterError {
                text: known_text(&id),
                url: Some(format!(
                    "https://wiki.bambulab.com/en/x1/troubleshooting/hmscode/{id}"
                )),
                code: id,
                severity: hms_severity(code),
            });
        }
    }
    if let Some(code) = num(p, "print_error").map(|c| c as u64).filter(|c| *c != 0) {
        let id = fmt_error(code);
        out.push(PrinterError {
            text: known_text(&id),
            url: None,
            code: id,
            severity: "serious",
        });
    }
    out
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

    /// The user's P1S: one AMS with four PLA trays, ASA on the external spool.
    fn ams_report() -> Value {
        let tray = |id: &str, ty: &str, color: &str| json!({"id": id, "tray_type": ty, "tray_color": color, "remain": 80});
        json!({"print": {
            "command": "push_status",
            "ams": {
                "ams_exist_bits": "1",
                "tray_exist_bits": "f",
                "tray_now": "255",
                "ams": [{"id": "0", "humidity": "4", "tray": [
                    tray("0", "PLA", "FF9016FF"),
                    tray("1", "PLA", "F7E6DEFF"),
                    tray("2", "PLA", "161616FF"),
                    tray("3", "PLA", "E8DBB7FF"),
                ]}]
            },
            "vt_tray": {"id": "254", "tray_type": "ASA", "tray_color": "FFFFFFFF"}
        }})
    }

    #[test]
    fn ams_trays_and_external_spool() {
        let mut state = Value::Null;
        apply_report(&mut state, &ams_report());
        let a = normalise(&state, true, 0).ams.expect("ams");
        assert_eq!(a.trays.len(), 4);
        assert_eq!(
            a.trays[2],
            AmsTray {
                slot: 2,
                unit: 0,
                tray: 2,
                kind: "PLA".into(),
                color: Some("161616FF".into())
            }
        );
        assert_eq!(a.external.as_ref().unwrap().kind, "ASA");
        let v = serde_json::to_value(normalise(&state, true, 0)).unwrap();
        assert_eq!(
            v["ams"]["trays"][0],
            json!({"slot": 0, "unit": 0, "tray": 0, "type": "PLA", "color": "FF9016FF"})
        );
        assert_eq!(v["ams"]["external"]["type"], "ASA");
        // No AMS, no external spool: null.
        assert_eq!(normalise(&json!({}), true, 0).ams, None);
    }

    #[test]
    fn ams_survives_partial_deltas() {
        let mut state = Value::Null;
        apply_report(&mut state, &ams_report());
        // A delta without the ams block leaves it alone.
        apply_report(
            &mut state,
            &json!({"print": {"command": "push_status", "mc_percent": 3}}),
        );
        assert_eq!(normalise(&state, true, 0).ams.unwrap().trays.len(), 4);
        // P1 delta: only tray 1 changed, and only some of its fields.
        apply_report(
            &mut state,
            &json!({"print": {"command": "push_status", "ams": {"ams": [{"id": "0",
                "tray": [{"id": "1", "tray_type": "PETG", "tray_color": "00FF00FF"}]}]}}}),
        );
        let a = normalise(&state, true, 0).ams.unwrap();
        assert_eq!(a.trays.len(), 4, "other trays survive");
        assert_eq!(a.trays[1].kind, "PETG");
        assert_eq!(a.trays[0].color.as_deref(), Some("FF9016FF"));
        assert_eq!(state["ams"]["ams"][0]["humidity"], "4", "unit fields kept");
        assert_eq!(state["ams"]["ams"][0]["tray"][1]["remain"], 80);
        // Tray 3 unloaded: its exist bit clears though the stale type stays.
        apply_report(
            &mut state,
            &json!({"print": {"command": "push_status", "ams": {"tray_exist_bits": "7",
                "ams": [{"id": "0", "tray": [{"id": "3"}]}]}}}),
        );
        let a = normalise(&state, true, 0).ams.unwrap();
        assert_eq!(
            a.trays.iter().map(|t| t.slot).collect::<Vec<_>>(),
            [0, 1, 2]
        );
        // Empty external spool reports an empty type.
        apply_report(
            &mut state,
            &json!({"print": {"command": "push_status", "vt_tray": {"tray_type": ""}}}),
        );
        assert_eq!(normalise(&state, true, 0).ams.unwrap().external, None);
    }

    #[test]
    fn control_fields_and_errors() {
        let mut state = Value::Null;
        apply_report(&mut state, &full_report());
        apply_report(
            &mut state,
            &json!({"print": {"command": "push_status", "spd_lvl": 2,
                "cooling_fan_speed": "15", "big_fan1_speed": "0", "big_fan2_speed": "10",
                "wifi_signal": "-48dBm", "fun": "3EC1AFFF9CFF",
                "print_error": 0x0502_4007u64,
                "hms": [{"attr": 0x0500_0500u64, "code": 0x0001_0007u64},
                        {"attr": 0x0700_2000u64, "code": 0x0002_0001u64}]}}),
        );
        let s = normalise(&state, true, 0);
        assert_eq!(s.speed_level, Some(2));
        assert_eq!(
            s.fans,
            Fans {
                part: Some(100),
                aux: Some(0),
                chamber: Some(67)
            }
        );
        assert_eq!(s.light, Some("on"));
        assert_eq!(s.wifi_signal.as_deref(), Some("-48dBm"));
        assert_eq!(
            s.developer_mode,
            Some(false),
            "bit 29 set: signing required"
        );
        assert_eq!(s.errors.len(), 3);
        assert_eq!(s.errors[0].code, "0500_0500_0001_0007");
        assert!(
            s.errors[0]
                .text
                .as_deref()
                .unwrap()
                .contains("Developer Mode")
        );
        assert_eq!(s.errors[0].severity, "fatal");
        assert_eq!(s.errors[1].text, None);
        assert_eq!(
            s.errors[1].url.as_deref(),
            Some("https://wiki.bambulab.com/en/x1/troubleshooting/hmscode/0700_2000_0002_0001")
        );
        assert_eq!(s.errors[2].code, "0502_4007");
        assert!(s.errors[2].text.is_some());

        let s = normalise(&json!({"fun": "8FFF9CFF"}), true, 0);
        assert_eq!(s.developer_mode, Some(true));
        let v = serde_json::to_value(normalise(&json!({}), true, 0)).unwrap();
        assert_eq!(v["developer_mode"], Value::Null);
        assert_eq!(v["fans"], json!({}));
        assert_eq!(v["errors"], json!([]));
        assert_eq!(v["light"], Value::Null);
    }
    #[test]
    fn active_tray_from_tray_now() {
        let mut state = Value::Null;
        apply_report(&mut state, &ams_report());
        assert_eq!(
            normalise(&state, true, 0).ams.unwrap().active,
            None,
            "255 = none"
        );
        for (raw, want) in [("2", json!(2)), ("254", json!("external"))] {
            apply_report(
                &mut state,
                &json!({"print": {"command": "push_status", "ams": {"tray_now": raw}}}),
            );
            let v = serde_json::to_value(normalise(&state, true, 0)).unwrap();
            assert_eq!(v["ams"]["active"], want, "{raw}");
        }
        // P1S without the fun field: developer_mode stays unknown.
        let p1s = json!({"home_flag": 24330512, "wifi_signal": "-44dBm", "spd_lvl": 2,
            "cooling_fan_speed": "0", "print_error": 0, "hms": []});
        let s = normalise(&p1s, true, 0);
        assert_eq!(s.developer_mode, None);
        assert_eq!((s.speed_level, s.fans.part), (Some(2), Some(0)));
        assert!(s.errors.is_empty());
    }
}
