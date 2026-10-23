//! Typed control commands (`POST /printers/{id}/command`) and the MQTT
//! payloads they become. Payload shapes follow ha-bambulab / bambulabs_api.

use serde::Deserialize;
use serde_json::{Value, json};

pub const MAX_NOZZLE_C: f64 = 300.0;
pub const MAX_BED_C: f64 = 110.0;
pub const MAX_MOVE_MM: f64 = 100.0;
/// Below this the hotend can't extrude (the firmware would refuse anyway).
pub const MIN_EXTRUDE_C: f64 = 170.0;
pub const MAX_GCODE_LEN: usize = 256;

#[derive(Debug, Clone, Copy, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Heater {
    Nozzle,
    Bed,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Fan {
    Part,
    Aux,
    Chamber,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Axis {
    X,
    Y,
    Z,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Slot {
    /// Global AMS tray index (`unit * 4 + tray`).
    Index(u32),
    /// `"external"`: the external spool holder.
    Named(String),
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    Speed { level: u8 },
    Temp { heater: Heater, target: f64 },
    Fan { fan: Fan, percent: f64 },
    Light { on: bool },
    Home,
    Jog { axis: Axis, distance: f64 },
    MotorsOff,
    Extrude { mm: f64 },
    Load { slot: Slot },
    Unload,
    Gcode { line: String },
}

/// What the builders need to know about the printer right now.
#[derive(Debug, Clone, Default)]
pub struct Ctx<'a> {
    /// Normalised state (`printing`, `paused`, `idle`, ...).
    pub state: &'a str,
    pub nozzle_actual: Option<f64>,
    pub nozzle_target: Option<f64>,
}

#[derive(Debug, PartialEq)]
pub enum BuildError {
    /// Invalid parameters (400).
    Invalid(String),
    /// Not allowed in the printer's current state (409).
    Conflict(String),
}

fn gcode(seq: String, lines: &str) -> Value {
    json!({"print": {"sequence_id": seq, "command": "gcode_line", "param": lines}})
}

fn finite(v: f64, what: &str) -> Result<f64, BuildError> {
    if v.is_finite() {
        Ok(v)
    } else {
        Err(BuildError::Invalid(format!("{what} must be a number")))
    }
}

/// `10.0` → `10`, `0.5` → `0.5`.
fn fmt_num(v: f64) -> String {
    let r = (v * 100.0).round() / 100.0;
    if r.fract() == 0.0 {
        format!("{}", r as i64)
    } else {
        format!("{r}")
    }
}

impl Command {
    /// Commands that move the toolhead or feed filament: refused mid-print.
    fn moves(&self) -> bool {
        matches!(
            self,
            Command::Home
                | Command::Jog { .. }
                | Command::MotorsOff
                | Command::Extrude { .. }
                | Command::Load { .. }
                | Command::Unload
                | Command::Gcode { .. }
        )
    }
}

/// Load/unload temperatures: the current target when the hotend is already
/// set for filament, else a PLA-safe default.
fn change_temps(ctx: &Ctx) -> (i64, i64) {
    let target = ctx
        .nozzle_target
        .filter(|t| *t >= MIN_EXTRUDE_C)
        .unwrap_or(220.0)
        .round() as i64;
    let current = ctx
        .nozzle_actual
        .map(|t| t.round() as i64)
        .unwrap_or(target);
    (current, target)
}

/// Build the MQTT request for `cmd` with sequence id `seq`.
pub fn build(cmd: &Command, seq: String, ctx: &Ctx) -> Result<Value, BuildError> {
    if cmd.moves() && matches!(ctx.state, "printing" | "paused") {
        return Err(BuildError::Conflict(format!(
            "not allowed while the printer is {}",
            ctx.state
        )));
    }
    Ok(match cmd {
        Command::Speed { level } => {
            if !(1..=4).contains(level) {
                return Err(BuildError::Invalid("speed level must be 1-4".into()));
            }
            json!({"print": {"sequence_id": seq, "command": "print_speed",
                "param": level.to_string()}})
        }
        Command::Temp { heater, target } => {
            let t = finite(*target, "target")?;
            if t < 0.0 {
                return Err(BuildError::Invalid("target must be >= 0".into()));
            }
            let (code, max) = match heater {
                Heater::Nozzle => ("M104", MAX_NOZZLE_C),
                Heater::Bed => ("M140", MAX_BED_C),
            };
            gcode(seq, &format!("{code} S{}\n", t.min(max).round() as i64))
        }
        Command::Fan { fan, percent } => {
            let p = finite(*percent, "percent")?.clamp(0.0, 100.0);
            let idx = match fan {
                Fan::Part => 1,
                Fan::Aux => 2,
                Fan::Chamber => 3,
            };
            let s = (p * 255.0 / 100.0).round() as i64;
            gcode(seq, &format!("M106 P{idx} S{s}\n"))
        }
        Command::Light { on } => json!({"system": {"sequence_id": seq, "command": "ledctrl",
            "led_node": "chamber_light", "led_mode": if *on { "on" } else { "off" },
            "led_on_time": 500, "led_off_time": 500, "loop_times": 0, "interval_time": 0}}),
        Command::Home => gcode(seq, "G28\n"),
        Command::Jog { axis, distance } => {
            let d = finite(*distance, "distance")?;
            if d == 0.0 || d.abs() > MAX_MOVE_MM {
                return Err(BuildError::Invalid(format!(
                    "distance must be non-zero and at most {MAX_MOVE_MM} mm"
                )));
            }
            let (a, feed) = match axis {
                Axis::X => ("X", 3000),
                Axis::Y => ("Y", 3000),
                Axis::Z => ("Z", 600),
            };
            gcode(seq, &format!("G91\nG1 {a}{} F{feed}\nG90\n", fmt_num(d)))
        }
        Command::MotorsOff => gcode(seq, "M18\n"),
        Command::Extrude { mm } => {
            let mm = finite(*mm, "mm")?;
            if mm == 0.0 || mm.abs() > MAX_MOVE_MM {
                return Err(BuildError::Invalid(format!(
                    "mm must be non-zero and at most {MAX_MOVE_MM}"
                )));
            }
            if ctx.nozzle_actual.is_none_or(|t| t < MIN_EXTRUDE_C) {
                return Err(BuildError::Conflict(format!(
                    "nozzle must be at least {MIN_EXTRUDE_C} °C to extrude"
                )));
            }
            gcode(seq, &format!("M83\nG1 E{} F300\n", fmt_num(mm)))
        }
        Command::Load { slot } => {
            let (target, ams_id, slot_id) = match slot {
                Slot::Index(i) if *i < 16 => (*i, *i / 4, *i % 4),
                Slot::Named(s) if s == "external" => (254, 255, 254),
                _ => {
                    return Err(BuildError::Invalid(
                        "slot must be an AMS tray index 0-15 or \"external\"".into(),
                    ));
                }
            };
            let (curr, tar) = change_temps(ctx);
            json!({"print": {"sequence_id": seq, "command": "ams_change_filament",
                "target": target, "ams_id": ams_id, "slot_id": slot_id,
                "curr_temp": curr, "tar_temp": tar}})
        }
        Command::Unload => {
            let (curr, tar) = change_temps(ctx);
            json!({"print": {"sequence_id": seq, "command": "ams_change_filament",
                "target": 255, "ams_id": 255, "slot_id": 255,
                "curr_temp": curr, "tar_temp": tar}})
        }
        Command::Gcode { line } => {
            let line = line.trim();
            if line.is_empty() || line.len() > MAX_GCODE_LEN || line.contains(['\r', '\n', '\0']) {
                return Err(BuildError::Invalid(format!(
                    "gcode must be one non-empty line of at most {MAX_GCODE_LEN} chars"
                )));
            }
            gcode(seq, &format!("{line}\n"))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn idle() -> Ctx<'static> {
        Ctx {
            state: "idle",
            nozzle_actual: Some(25.0),
            nozzle_target: Some(0.0),
        }
    }

    fn parse(v: Value) -> Command {
        serde_json::from_value(v).unwrap()
    }

    fn param(v: &Value) -> &str {
        v["print"]["param"].as_str().unwrap()
    }

    #[test]
    fn parses_typed_bodies() {
        assert_eq!(
            parse(json!({"type": "temp", "heater": "bed", "target": 60})),
            Command::Temp {
                heater: Heater::Bed,
                target: 60.0
            }
        );
        assert_eq!(parse(json!({"type": "motors_off"})), Command::MotorsOff);
        assert_eq!(
            parse(json!({"type": "load", "slot": "external"})),
            Command::Load {
                slot: Slot::Named("external".into())
            }
        );
        assert!(
            serde_json::from_value::<Command>(
                json!({"type": "temp", "heater": "chamber", "target": 40})
            )
            .is_err()
        );
        assert!(serde_json::from_value::<Command>(json!({"type": "explode"})).is_err());
    }

    #[test]
    fn builds_payloads() {
        let c = idle();
        let b = |cmd: Value| build(&parse(cmd), "7".into(), &c).unwrap();
        let v = b(json!({"type": "speed", "level": 3}));
        assert_eq!(v["print"]["command"], "print_speed");
        assert_eq!(v["print"]["param"], "3");
        assert_eq!(v["print"]["sequence_id"], "7");
        assert_eq!(
            param(&b(
                json!({"type": "temp", "heater": "nozzle", "target": 999})
            )),
            "M104 S300\n"
        );
        assert_eq!(
            param(&b(json!({"type": "temp", "heater": "bed", "target": 60.4}))),
            "M140 S60\n"
        );
        assert_eq!(
            param(&b(json!({"type": "fan", "fan": "aux", "percent": 50}))),
            "M106 P2 S128\n"
        );
        assert_eq!(
            param(&b(json!({"type": "fan", "fan": "chamber", "percent": 200}))),
            "M106 P3 S255\n"
        );
        let l = b(json!({"type": "light", "on": false}));
        assert_eq!(l["system"]["command"], "ledctrl");
        assert_eq!(l["system"]["led_mode"], "off");
        assert_eq!(param(&b(json!({"type": "home"}))), "G28\n");
        assert_eq!(
            param(&b(json!({"type": "jog", "axis": "z", "distance": -0.5}))),
            "G91\nG1 Z-0.5 F600\nG90\n"
        );
        assert_eq!(
            param(&b(json!({"type": "jog", "axis": "x", "distance": 10}))),
            "G91\nG1 X10 F3000\nG90\n"
        );
        assert_eq!(param(&b(json!({"type": "motors_off"}))), "M18\n");
        assert_eq!(
            param(&b(json!({"type": "gcode", "line": " M115 "}))),
            "M115\n"
        );
        let load = b(json!({"type": "load", "slot": 5}));
        assert_eq!(load["print"]["command"], "ams_change_filament");
        assert_eq!(
            (
                load["print"]["target"].as_u64(),
                load["print"]["ams_id"].as_u64(),
                load["print"]["slot_id"].as_u64()
            ),
            (Some(5), Some(1), Some(1))
        );
        assert_eq!(load["print"]["tar_temp"], 220);
        assert_eq!(
            b(json!({"type": "load", "slot": "external"}))["print"]["target"],
            254
        );
        assert_eq!(b(json!({"type": "unload"}))["print"]["target"], 255);

        let hot = Ctx {
            nozzle_actual: Some(215.0),
            ..idle()
        };
        let e = build(
            &parse(json!({"type": "extrude", "mm": -5})),
            "1".into(),
            &hot,
        )
        .unwrap();
        assert_eq!(param(&e), "M83\nG1 E-5 F300\n");
    }

    #[test]
    fn refuses_bad_or_unsafe_commands() {
        let c = idle();
        let b = |cmd: Value| build(&parse(cmd), "1".into(), &c).unwrap_err();
        assert!(matches!(
            b(json!({"type": "speed", "level": 5})),
            BuildError::Invalid(_)
        ));
        assert!(matches!(
            b(json!({"type": "jog", "axis": "y", "distance": 101})),
            BuildError::Invalid(_)
        ));
        assert!(matches!(
            b(json!({"type": "gcode", "line": "G28\nM18"})),
            BuildError::Invalid(_)
        ));
        assert!(matches!(
            b(json!({"type": "gcode", "line": "x".repeat(257)})),
            BuildError::Invalid(_)
        ));
        assert!(matches!(
            b(json!({"type": "load", "slot": 16})),
            BuildError::Invalid(_)
        ));
        assert!(matches!(
            b(json!({"type": "temp", "heater": "bed", "target": -1})),
            BuildError::Invalid(_)
        ));
        // Cold nozzle: no extrusion.
        assert!(matches!(
            b(json!({"type": "extrude", "mm": 5})),
            BuildError::Conflict(_)
        ));

        let printing = Ctx {
            state: "printing",
            nozzle_actual: Some(220.0),
            nozzle_target: Some(220.0),
        };
        for cmd in [
            json!({"type": "home"}),
            json!({"type": "jog", "axis": "x", "distance": 1}),
            json!({"type": "extrude", "mm": 1}),
            json!({"type": "unload"}),
            json!({"type": "gcode", "line": "M115"}),
        ] {
            assert!(matches!(
                build(&parse(cmd), "1".into(), &printing),
                Err(BuildError::Conflict(_))
            ));
        }
        // Tuning a running print is fine.
        assert!(
            build(
                &parse(json!({"type": "speed", "level": 4})),
                "1".into(),
                &printing
            )
            .is_ok()
        );
        assert!(
            build(
                &parse(json!({"type": "fan", "fan": "part", "percent": 30})),
                "1".into(),
                &printing
            )
            .is_ok()
        );
    }
}
