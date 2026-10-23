//! HTTP API. Every route requires `Authorization: Bearer <token>`.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{DefaultBodyLimit, Path, Query, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::camera::{self, SnapshotError};
use crate::ftps;
use crate::mqtt::{self, CommandError, Live};
use crate::status;
use crate::store::{PrinterConfig, Store};

const MAX_UPLOAD: usize = 1024 * 1024 * 1024;

pub struct Printer {
    pub cfg: PrinterConfig,
    pub live: Arc<Live>,
    task: Option<JoinHandle<()>>,
}

impl Printer {
    /// A printer with a running MQTT connection.
    pub fn connect(id: &str, cfg: PrinterConfig) -> Self {
        let live = Arc::new(Live::default());
        let task = mqtt::spawn(id.to_owned(), cfg.clone(), live.clone());
        Self {
            cfg,
            live,
            task: Some(task),
        }
    }

    /// A printer with no connection (tests drive `live` by hand).
    pub fn detached(cfg: PrinterConfig, live: Arc<Live>) -> Self {
        Self {
            cfg,
            live,
            task: None,
        }
    }
}

impl Drop for Printer {
    fn drop(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
        self.live.connected.store(false, Ordering::Release);
    }
}

pub struct AppState {
    pub token: String,
    pub store: Option<Store>,
    /// Open MQTT connections for registered printers (off in route tests).
    pub connect: bool,
    pub printers: RwLock<HashMap<String, Arc<Printer>>>,
}

type Shared = Arc<AppState>;

pub fn router(state: Shared) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/printers", get(list))
        .route("/printers/{id}", put(upsert).delete(remove))
        .route("/printers/{id}/status", get(status_route))
        .route(
            "/printers/{id}/upload",
            post(upload).layer(DefaultBodyLimit::max(MAX_UPLOAD)),
        )
        .route("/printers/{id}/start", post(start))
        .route("/printers/{id}/pause", post(pause))
        .route("/printers/{id}/resume", post(resume))
        .route("/printers/{id}/stop", post(stop))
        .route("/printers/{id}/snapshot", get(snapshot))
        .layer(middleware::from_fn_with_state(state.clone(), auth))
        .with_state(state)
}

fn err(code: StatusCode, msg: impl Into<String>) -> Response {
    (code, Json(json!({"ok": false, "error": msg.into()}))).into_response()
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

async fn auth(State(st): State<Shared>, headers: HeaderMap, req: Request, next: Next) -> Response {
    let ok = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|t| ct_eq(t.trim().as_bytes(), st.token.as_bytes()));
    if !ok {
        return err(StatusCode::UNAUTHORIZED, "missing or invalid bearer token");
    }
    next.run(req).await
}

async fn get_printer(st: &AppState, id: &str) -> Result<Arc<Printer>, Response> {
    st.printers
        .read()
        .await
        .get(id)
        .cloned()
        .ok_or_else(|| err(StatusCode::NOT_FOUND, format!("unknown printer {id:?}")))
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

async fn health() -> Json<Value> {
    Json(json!({"ok": true, "version": env!("CARGO_PKG_VERSION")}))
}

async fn list(State(st): State<Shared>) -> Json<Value> {
    let printers = st.printers.read().await;
    let mut out: Vec<Value> = printers
        .iter()
        .map(|(id, p)| {
            json!({
                "id": id,
                "host": p.cfg.host,
                "serial": p.cfg.serial,
                "model": p.cfg.model,
                "connected": p.live.connected.load(Ordering::Acquire),
            })
        })
        .collect();
    out.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    Json(json!({"printers": out}))
}

async fn persist(st: &AppState, printers: &HashMap<String, Arc<Printer>>) -> Result<(), Response> {
    let Some(store) = &st.store else {
        return Ok(());
    };
    let map: BTreeMap<String, PrinterConfig> = printers
        .iter()
        .map(|(k, p)| (k.clone(), p.cfg.clone()))
        .collect();
    store.save(&map).map_err(|e| {
        err(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("saving printers: {e:#}"),
        )
    })
}

async fn upsert(
    State(st): State<Shared>,
    Path(id): Path<String>,
    Json(cfg): Json<PrinterConfig>,
) -> Response {
    if !valid_id(&id) {
        return err(
            StatusCode::BAD_REQUEST,
            "id must be 1-64 chars of [A-Za-z0-9_-]",
        );
    }
    if cfg.host.trim().is_empty() || cfg.serial.trim().is_empty() || cfg.access_code.is_empty() {
        return err(
            StatusCode::BAD_REQUEST,
            "host, serial and access_code are required",
        );
    }
    let mut printers = st.printers.write().await;
    // Replacing drops the old Printer, which aborts its MQTT task.
    let printer = if st.connect {
        Printer::connect(&id, cfg)
    } else {
        Printer::detached(cfg, Arc::new(Live::default()))
    };
    printers.insert(id, Arc::new(printer));
    if let Err(r) = persist(&st, &printers).await {
        return r;
    }
    Json(json!({"ok": true})).into_response()
}

async fn remove(State(st): State<Shared>, Path(id): Path<String>) -> Response {
    let mut printers = st.printers.write().await;
    if printers.remove(&id).is_none() {
        return err(StatusCode::NOT_FOUND, format!("unknown printer {id:?}"));
    }
    if let Err(r) = persist(&st, &printers).await {
        return r;
    }
    Json(json!({"ok": true})).into_response()
}

async fn status_route(State(st): State<Shared>, Path(id): Path<String>) -> Response {
    let p = match get_printer(&st, &id).await {
        Ok(p) => p,
        Err(r) => return r,
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let connected = p.live.connected.load(Ordering::Acquire);
    let mut s = status::normalise(&p.live.state.lock().unwrap(), connected, now);
    if !connected && s.message.is_none() {
        s.message = p.live.last_error.lock().unwrap().clone();
    }
    Json(s).into_response()
}

fn valid_file_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 200
        && !name.contains(['/', '\\', '\r', '\n', '\0'])
        && name != "."
        && name != ".."
}

#[derive(Deserialize)]
struct UploadQuery {
    name: String,
}

async fn upload(
    State(st): State<Shared>,
    Path(id): Path<String>,
    Query(q): Query<UploadQuery>,
    body: axum::body::Bytes,
) -> Response {
    let p = match get_printer(&st, &id).await {
        Ok(p) => p,
        Err(r) => return r,
    };
    if !valid_file_name(&q.name) {
        return err(StatusCode::BAD_REQUEST, "invalid file name");
    }
    match ftps::upload(&p.cfg.host, &p.cfg.access_code, &q.name, &body).await {
        Ok(()) => Json(json!({"ok": true})).into_response(),
        Err(e) => err(StatusCode::BAD_GATEWAY, format!("upload failed: {e:#}")),
    }
}

#[derive(Deserialize)]
struct StartBody {
    file: String,
    plate: Option<u32>,
    use_ams: Option<bool>,
    bed_leveling: Option<bool>,
    ams_mapping: Option<Vec<i32>>,
}

/// Build the `print` request that starts `body.file`. `.3mf` projects go
/// through `project_file`; plain `.gcode` through `gcode_file`.
pub fn start_payload(
    seq: String,
    file: &str,
    plate: u32,
    use_ams: bool,
    bed_leveling: bool,
    ams_mapping: Option<&[i32]>,
) -> Option<Value> {
    let lower = file.to_ascii_lowercase();
    if lower.ends_with(".3mf") {
        let mut print = json!({
            "sequence_id": seq,
            "command": "project_file",
            "param": format!("Metadata/plate_{plate}.gcode"),
            "project_id": "0",
            "profile_id": "0",
            "task_id": "0",
            "subtask_id": "0",
            "subtask_name": file,
            "file": file,
            "url": format!("ftp:///{file}"),
            "md5": "",
            "timelapse": false,
            "bed_type": "auto",
            // Firmware/tooling disagree on the spelling; send both.
            "bed_levelling": bed_leveling,
            "bed_leveling": bed_leveling,
            "flow_cali": false,
            "vibration_cali": false,
            "layer_inspect": false,
            "use_ams": use_ams,
        });
        if let Some(m) = ams_mapping {
            print["ams_mapping"] = json!(m);
        }
        Some(json!({"print": print}))
    } else if lower.ends_with(".gcode") {
        Some(json!({"print": {
            "sequence_id": seq,
            "command": "gcode_file",
            "param": format!("/sdcard/{file}"),
        }}))
    } else {
        None
    }
}

fn command_result(r: Result<(), CommandError>) -> Response {
    match r {
        Ok(()) => Json(json!({"ok": true})).into_response(),
        Err(CommandError::Offline) => err(StatusCode::SERVICE_UNAVAILABLE, "printer offline"),
        Err(CommandError::Other(e)) => err(StatusCode::BAD_GATEWAY, e),
    }
}

async fn start(
    State(st): State<Shared>,
    Path(id): Path<String>,
    Json(b): Json<StartBody>,
) -> Response {
    let p = match get_printer(&st, &id).await {
        Ok(p) => p,
        Err(r) => return r,
    };
    if !valid_file_name(&b.file) {
        return err(StatusCode::BAD_REQUEST, "invalid file name");
    }
    let Some(payload) = start_payload(
        p.live.next_seq(),
        &b.file,
        b.plate.unwrap_or(1).max(1),
        b.use_ams.unwrap_or(false),
        b.bed_leveling.unwrap_or(true),
        b.ams_mapping.as_deref(),
    ) else {
        return err(StatusCode::BAD_REQUEST, "file must end in .3mf or .gcode");
    };
    command_result(p.live.publish(&p.cfg.serial, &payload))
}

async fn simple_command(st: &AppState, id: &str, command: &str) -> Response {
    let p = match get_printer(st, id).await {
        Ok(p) => p,
        Err(r) => return r,
    };
    let payload =
        json!({"print": {"sequence_id": p.live.next_seq(), "command": command, "param": ""}});
    command_result(p.live.publish(&p.cfg.serial, &payload))
}

async fn pause(State(st): State<Shared>, Path(id): Path<String>) -> Response {
    simple_command(&st, &id, "pause").await
}

async fn resume(State(st): State<Shared>, Path(id): Path<String>) -> Response {
    simple_command(&st, &id, "resume").await
}

async fn stop(State(st): State<Shared>, Path(id): Path<String>) -> Response {
    simple_command(&st, &id, "stop").await
}

async fn snapshot(State(st): State<Shared>, Path(id): Path<String>) -> Response {
    let p = match get_printer(&st, &id).await {
        Ok(p) => p,
        Err(r) => return r,
    };
    if p.cfg.model.as_deref().is_some_and(camera::is_rtsp_model) {
        return err(
            StatusCode::NOT_FOUND,
            "snapshot unsupported: this model streams RTSPS on port 322",
        );
    }
    match camera::snapshot(&p.cfg.host, &p.cfg.access_code).await {
        Ok(jpeg) => (
            [
                (header::CONTENT_TYPE, "image/jpeg"),
                (header::CACHE_CONTROL, "no-store"),
            ],
            jpeg,
        )
            .into_response(),
        Err(SnapshotError::Unsupported) => err(
            StatusCode::NOT_FOUND,
            "snapshot unsupported: no camera service on port 6000",
        ),
        Err(SnapshotError::Other(e)) => {
            err(StatusCode::BAD_GATEWAY, format!("snapshot failed: {e:#}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn app_with_fake() -> (Router, Arc<Live>) {
        let live = Arc::new(Live::default());
        let cfg = PrinterConfig {
            host: "192.0.2.10".into(),
            serial: "01P00A000000000".into(),
            access_code: "secret99".into(),
            model: Some("P1S".into()),
        };
        let mut printers = HashMap::new();
        printers.insert(
            "p1s".to_string(),
            Arc::new(Printer::detached(cfg, live.clone())),
        );
        let st = Arc::new(AppState {
            token: "tok".into(),
            store: None,
            connect: false,
            printers: RwLock::new(printers),
        });
        (router(st), live)
    }

    async fn call(
        app: &Router,
        method: &str,
        uri: &str,
        token: Option<&str>,
    ) -> (StatusCode, Value) {
        let mut req = Request::builder().method(method).uri(uri);
        if let Some(t) = token {
            req = req.header(header::AUTHORIZATION, format!("Bearer {t}"));
        }
        let res = app
            .clone()
            .oneshot(req.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let code = res.status();
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        (code, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
    }

    #[tokio::test]
    async fn auth_is_required_everywhere() {
        let (app, _) = app_with_fake();
        assert_eq!(
            call(&app, "GET", "/health", None).await.0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            call(&app, "GET", "/printers/p1s/status", Some("nope"))
                .await
                .0,
            StatusCode::UNAUTHORIZED
        );
        let (code, body) = call(&app, "GET", "/health", Some("tok")).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(body["ok"], true);
    }

    #[tokio::test]
    async fn status_offline_then_printing() {
        let (app, live) = app_with_fake();
        let (code, body) = call(&app, "GET", "/printers/p1s/status", Some("tok")).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(body["state"], "offline");
        assert_eq!(body["file"], Value::Null);

        status::apply_report(
            &mut live.state.lock().unwrap(),
            &json!({"print": {"command": "push_status", "gcode_state": "PAUSE",
                "mc_percent": 50, "mc_remaining_time": 10, "subtask_name": "cube",
                "layer_num": 5, "total_layer_num": 10, "nozzle_temper": 200.0,
                "nozzle_target_temper": 210.0, "bed_temper": 60, "bed_target_temper": 60}}),
        );
        live.connected.store(true, Ordering::Release);
        let (_, body) = call(&app, "GET", "/printers/p1s/status", Some("tok")).await;
        assert_eq!(body["state"], "paused");
        assert_eq!(body["progress"], 0.5);
        assert_eq!(body["file"], "cube");
        assert_eq!(body["eta_s"], 600);
        assert_eq!(body["layer"], 5);
        assert_eq!(body["total_layers"], 10);
        assert_eq!(
            body["temps"]["nozzle"],
            json!({"actual": 200.0, "target": 210.0})
        );
        assert!(body["temps"].get("chamber").is_none());
        assert_eq!(body["message"], Value::Null);

        assert_eq!(
            call(&app, "GET", "/printers/nope/status", Some("tok"))
                .await
                .0,
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn list_hides_access_code_and_commands_need_connection() {
        let (app, _) = app_with_fake();
        let (_, body) = call(&app, "GET", "/printers", Some("tok")).await;
        assert_eq!(body["printers"][0]["id"], "p1s");
        assert!(!body.to_string().contains("secret99"));
        // Not connected: commands are refused, not queued.
        assert_eq!(
            call(&app, "POST", "/printers/p1s/pause", Some("tok"))
                .await
                .0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn start_payloads() {
        let p = start_payload(
            "3".into(),
            "benchy.gcode.3mf",
            2,
            true,
            false,
            Some(&[0, -1]),
        )
        .unwrap();
        assert_eq!(p["print"]["command"], "project_file");
        assert_eq!(p["print"]["param"], "Metadata/plate_2.gcode");
        assert_eq!(p["print"]["url"], "ftp:///benchy.gcode.3mf");
        assert_eq!(p["print"]["use_ams"], true);
        assert_eq!(p["print"]["bed_leveling"], false);
        assert_eq!(p["print"]["ams_mapping"], json!([0, -1]));
        let g = start_payload("4".into(), "cal.gcode", 1, false, true, None).unwrap();
        assert_eq!(g["print"]["command"], "gcode_file");
        assert_eq!(g["print"]["param"], "/sdcard/cal.gcode");
        assert!(start_payload("5".into(), "x.stl", 1, false, true, None).is_none());
    }
}
