//! HTTP API. Every route requires `Authorization: Bearer <token>`.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

use crate::camera::{self, Camera, SnapshotError};
use crate::commands::{self, BuildError, Command};
use crate::ftps;
use crate::mqtt::{self, Ack, CommandError, Live};
use crate::status;
use crate::store::{PrinterConfig, Store};

const MAX_UPLOAD: usize = 1024 * 1024 * 1024;

pub struct Printer {
    pub cfg: PrinterConfig,
    pub live: Arc<Live>,
    pub camera: Arc<Camera>,
    task: Option<JoinHandle<()>>,
}

impl Printer {
    /// A printer with a running MQTT connection.
    pub fn connect(id: &str, cfg: PrinterConfig) -> Self {
        let live = Arc::new(Live::default());
        let task = mqtt::spawn(id.to_owned(), cfg.clone(), live.clone());
        Self {
            camera: Camera::new(&cfg.host, &cfg.access_code),
            cfg,
            live,
            task: Some(task),
        }
    }

    /// A printer with no connection (tests drive `live` by hand).
    pub fn detached(cfg: PrinterConfig, live: Arc<Live>) -> Self {
        Self {
            camera: Camera::new(&cfg.host, &cfg.access_code),
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
        self.camera.stop();
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
        .route("/printers/{id}/command", post(command_route))
        .route("/printers/{id}/files", get(files_list).delete(files_delete))
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
                // Serial read from the printer's certificate when the configured
                // one is wrong, so clients can correct their record.
                "detected_serial": p.live.detected_serial.lock().unwrap().clone(),
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
    // Re-registering the same config must not drop a live connection: the
    // printer's TLS handshake takes 5-40 s, so a reconnect on every save or
    // connection test reads as "offline".
    if printers.get(&id).is_some_and(|p| p.cfg == cfg) {
        return Json(json!({"ok": true, "changed": false})).into_response();
    }
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

#[derive(Deserialize)]
struct StatusQuery {
    /// Long-poll: wait up to this many seconds (max 60) for the printer to
    /// connect and send its first report before answering.
    wait: Option<u64>,
}

async fn status_route(
    State(st): State<Shared>,
    Path(id): Path<String>,
    Query(q): Query<StatusQuery>,
) -> Response {
    let p = match get_printer(&st, &id).await {
        Ok(p) => p,
        Err(r) => return r,
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(q.wait.unwrap_or(0).min(60));
    // Connected alone is not enough: the first pushall report follows the
    // ConnAck by a second or two, and until then the state is empty.
    while !(p.live.connected.load(Ordering::Acquire) && !p.live.state.lock().unwrap().is_null())
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let connected = p.live.connected.load(Ordering::Acquire);
    let mut s = status::normalise(&p.live.state.lock().unwrap(), connected, now);
    if !connected && s.message.is_none() {
        s.message = p.live.last_error.lock().unwrap().clone();
    }
    s.firmware = p.live.firmware.lock().unwrap().clone();
    Json(s).into_response()
}

async fn command_route(
    State(st): State<Shared>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    let p = match get_printer(&st, &id).await {
        Ok(p) => p,
        Err(r) => return r,
    };
    let cmd: Command = match serde_json::from_value(body) {
        Ok(c) => c,
        Err(e) => return err(StatusCode::BAD_REQUEST, format!("invalid command: {e}")),
    };
    let connected = p.live.connected.load(Ordering::Acquire);
    if !connected {
        return err(StatusCode::SERVICE_UNAVAILABLE, "printer offline");
    }
    let s = status::normalise(&p.live.state.lock().unwrap(), connected, 0);
    let ctx = commands::Ctx {
        state: s.state,
        nozzle_actual: s.temps.nozzle.as_ref().map(|t| t.actual),
        nozzle_target: s.temps.nozzle.as_ref().and_then(|t| t.target),
    };
    match commands::build(&cmd, p.live.next_seq(), &ctx) {
        Ok(payload) => send(&p, &payload).await,
        Err(BuildError::Invalid(m)) => err(StatusCode::BAD_REQUEST, m),
        Err(BuildError::Conflict(m)) => err(StatusCode::CONFLICT, m),
    }
}

fn valid_file_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 200
        && !name.contains(['/', '\\', '\r', '\n', '\0'])
        && name != "."
        && name != ".."
}

/// A storage path from the files listing: a plain name, or `cache/<name>`.
fn valid_storage_path(name: &str) -> bool {
    valid_file_name(name.strip_prefix("cache/").unwrap_or(name))
}

async fn files_list(State(st): State<Shared>, Path(id): Path<String>) -> Response {
    let p = match get_printer(&st, &id).await {
        Ok(p) => p,
        Err(r) => return r,
    };
    match ftps::list_files(&p.cfg.host, &p.cfg.access_code).await {
        Ok(files) => Json(json!({"files": files})).into_response(),
        Err(e) => err(StatusCode::BAD_GATEWAY, format!("listing failed: {e:#}")),
    }
}

#[derive(Deserialize)]
struct NameQuery {
    name: String,
}

async fn files_delete(
    State(st): State<Shared>,
    Path(id): Path<String>,
    Query(q): Query<NameQuery>,
) -> Response {
    let p = match get_printer(&st, &id).await {
        Ok(p) => p,
        Err(r) => return r,
    };
    if !valid_storage_path(&q.name) {
        return err(StatusCode::BAD_REQUEST, "invalid file name");
    }
    match ftps::delete_file(&p.cfg.host, &p.cfg.access_code, &q.name).await {
        Ok(()) => Json(json!({"ok": true})).into_response(),
        Err(e) => err(StatusCode::BAD_GATEWAY, format!("delete failed: {e:#}")),
    }
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

fn command_result(r: Result<Ack, CommandError>) -> Response {
    match r {
        Ok(ack) => Json(json!({"ok": true, "confirmed": ack.confirmed})).into_response(),
        Err(CommandError::Offline) => err(StatusCode::SERVICE_UNAVAILABLE, "printer offline"),
        Err(CommandError::Other(e)) => err(StatusCode::BAD_GATEWAY, e),
        Err(CommandError::Refused {
            reason,
            err_code,
            needs_developer_mode,
        }) => {
            tracing::warn!(
                ?reason,
                ?err_code,
                needs_developer_mode,
                "printer refused command"
            );
            let (code, error) = if needs_developer_mode {
                (
                    StatusCode::CONFLICT,
                    "printer refused the command: enable LAN Only Mode and Developer Mode on the printer"
                        .to_owned(),
                )
            } else {
                (
                    StatusCode::BAD_GATEWAY,
                    format!(
                        "printer refused the command: {}",
                        reason.as_deref().unwrap_or("unknown reason")
                    ),
                )
            };
            let mut body = json!({"ok": false, "error": error, "reason": reason,
                "err_code": err_code});
            if needs_developer_mode {
                body["needs_developer_mode"] = json!(true);
            }
            (code, Json(body)).into_response()
        }
    }
}

/// Publish and wait for the printer's verdict.
async fn send(p: &Printer, payload: &Value) -> Response {
    command_result(
        p.live
            .command(&p.cfg.serial, payload, mqtt::REPLY_WAIT)
            .await,
    )
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
    send(&p, &payload).await
}

async fn simple_command(st: &AppState, id: &str, command: &str) -> Response {
    let p = match get_printer(st, id).await {
        Ok(p) => p,
        Err(r) => return r,
    };
    let payload =
        json!({"print": {"sequence_id": p.live.next_seq(), "command": command, "param": ""}});
    send(&p, &payload).await
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

#[derive(Deserialize)]
struct SnapshotQuery {
    /// Seconds to wait for a fresh frame while the stream connects.
    wait: Option<u64>,
}

async fn snapshot(
    State(st): State<Shared>,
    Path(id): Path<String>,
    Query(q): Query<SnapshotQuery>,
) -> Response {
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
    // Default stays under callers' usual 15 s HTTP timeout.
    let wait = Duration::from_secs(q.wait.unwrap_or(12).min(120));
    match p.camera.frame(Duration::from_secs(5), wait).await {
        Ok(f) => (
            [
                (header::CONTENT_TYPE, "image/jpeg".to_owned()),
                (header::CACHE_CONTROL, "no-store".to_owned()),
                (
                    header::HeaderName::from_static("x-frame-age-ms"),
                    f.at.elapsed().as_millis().to_string(),
                ),
            ],
            f.jpeg.as_ref().clone(),
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

    async fn put_json(app: &Router, uri: &str, body: Value) -> (StatusCode, Value) {
        let req = Request::builder()
            .method("PUT")
            .uri(uri)
            .header(header::AUTHORIZATION, "Bearer tok")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        let code = res.status();
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        (code, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
    }

    #[tokio::test]
    async fn re_registering_same_config_keeps_the_connection() {
        let (app, live) = app_with_fake();
        live.connected.store(true, Ordering::Release);
        let same = json!({"host": "192.0.2.10", "serial": "01P00A000000000",
            "access_code": "secret99", "model": "P1S"});
        let (code, body) = put_json(&app, "/printers/p1s", same).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(body["changed"], false);
        assert!(live.connected.load(Ordering::Acquire), "connection dropped");

        let changed = json!({"host": "192.0.2.11", "serial": "01P00A000000000",
            "access_code": "secret99", "model": "P1S"});
        put_json(&app, "/printers/p1s", changed).await;
        assert!(
            !live.connected.load(Ordering::Acquire),
            "old connection kept"
        );
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
        assert!(body["printers"][0]["detected_serial"].is_null());
        // Not connected: commands are refused, not queued.
        assert_eq!(
            call(&app, "POST", "/printers/p1s/pause", Some("tok"))
                .await
                .0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    async fn post_json(app: &Router, uri: &str, body: Value) -> (StatusCode, Value) {
        let req = Request::builder()
            .method("POST")
            .uri(uri)
            .header(header::AUTHORIZATION, "Bearer tok")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        let code = res.status();
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        (code, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
    }

    /// Answer the next published command the way `reply` says.
    fn answer_next(live: Arc<Live>, sent: Arc<std::sync::Mutex<Vec<Value>>>, reply: Value) {
        tokio::spawn(async move {
            loop {
                let last = sent.lock().unwrap().last().cloned();
                if let Some(req) = last {
                    let (key, inner) = req.as_object().unwrap().iter().next().unwrap();
                    let mut r = reply.clone();
                    r["command"] = inner["command"].clone();
                    r["sequence_id"] = inner["sequence_id"].clone();
                    live.dispatch_reply(&json!({ key.clone(): r }));
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
    }

    #[tokio::test]
    async fn developer_mode_refusal_is_a_409() {
        let (app, live) = app_with_fake();
        let sent = live.fake_connected();
        answer_next(
            live.clone(),
            sent.clone(),
            json!({"result": "failed", "reason": "mqtt message verify failed",
                "err_code": 84033543u64}),
        );
        let (code, body) =
            post_json(&app, "/printers/p1s/start", json!({"file": "cube.3mf"})).await;
        assert_eq!(code, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["needs_developer_mode"], true);
        assert_eq!(body["err_code"], 84033543u64);

        sent.lock().unwrap().clear();
        answer_next(
            live.clone(),
            sent.clone(),
            json!({"result": "failed", "reason": "busy", "err_code": 1}),
        );
        let (code, body) = post_json(
            &app,
            "/printers/p1s/command",
            json!({"type": "light", "on": true}),
        )
        .await;
        assert_eq!(code, StatusCode::BAD_GATEWAY);
        assert!(body.get("needs_developer_mode").is_none());
        assert_eq!(sent.lock().unwrap()[0]["system"]["led_mode"], "on");

        sent.lock().unwrap().clear();
        answer_next(live.clone(), sent.clone(), json!({"result": "success"}));
        let (code, body) = post_json(&app, "/printers/p1s/command", json!({"type": "home"})).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(body, json!({"ok": true, "confirmed": true}));
    }

    #[tokio::test]
    async fn command_route_validates_and_refuses_motion_mid_print() {
        let (app, live) = app_with_fake();
        let (code, _) = post_json(&app, "/printers/p1s/command", json!({"type": "home"})).await;
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE, "offline");
        let sent = live.fake_connected();
        status::apply_report(
            &mut live.state.lock().unwrap(),
            &json!({"print": {"command": "push_status", "gcode_state": "RUNNING"}}),
        );
        let (code, body) = post_json(
            &app,
            "/printers/p1s/command",
            json!({"type": "jog", "axis": "x", "distance": 5}),
        )
        .await;
        assert_eq!(code, StatusCode::CONFLICT, "{body}");
        let (code, _) = post_json(&app, "/printers/p1s/command", json!({"type": "warp"})).await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert!(sent.lock().unwrap().is_empty(), "nothing published");
    }

    #[test]
    fn storage_paths() {
        assert!(valid_storage_path("a.3mf"));
        assert!(valid_storage_path("cache/a.3mf"));
        assert!(!valid_storage_path("../a.3mf"));
        assert!(!valid_storage_path("cache/../x"));
        assert!(!valid_storage_path("cache/"));
        assert!(!valid_storage_path(".."));
        assert!(!valid_storage_path("/etc/passwd"));
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
