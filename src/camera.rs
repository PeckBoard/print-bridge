//! Camera frames from the P1/A1 camera service: TLS on port 6000, an
//! 80-byte auth packet, then a stream of `[16-byte header][JPEG]` frames
//! (all integers little-endian). X1/H2/P2S use RTSPS on 322 instead, which
//! needs an RTSP + H.264 decoder; the bridge does not support those.
//!
//! The printer's TLS handshake takes 3–30 s (worse on weak Wi-Fi), so a
//! fresh connection per snapshot often timed out. [`Camera`] keeps one stream
//! open while snapshots are being asked for and serves the latest frame from
//! memory; it hangs up after [`IDLE`] without requests, because the printer
//! allows a single camera client (Bambu Handy / Studio need it back).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::tls;

pub const CAMERA_PORT: u16 = 6000;
const MAX_FRAME: usize = 8 * 1024 * 1024;
/// Hang up after this long without a snapshot request.
pub const IDLE: Duration = Duration::from_secs(60);
/// Reconnect when the stream delivers no bytes at all for this long. Not a
/// per-frame limit: TLS hands over whole records (up to 16 KiB) and a lossy
/// printer Wi-Fi link was measured at ~1 KB/s, so one frame can take minutes.
const STALL_TIMEOUT: Duration = Duration::from_secs(45);
const STALLED: &str = "camera connected but sent no data (LAN Only Liveview off on the printer, or its Wi-Fi is too weak)";
const RETRY_DELAY: Duration = Duration::from_secs(3);

#[derive(Clone)]
pub struct Frame {
    pub jpeg: Arc<Vec<u8>>,
    pub at: Instant,
}

#[derive(Default)]
struct Meta {
    running: bool,
    unsupported: bool,
    last_request: Option<Instant>,
    last_error: Option<String>,
}

/// One printer's camera: a lazily started, self-stopping frame stream.
pub struct Camera {
    host: String,
    port: u16,
    access_code: String,
    meta: Mutex<Meta>,
    frames: watch::Sender<Option<Frame>>,
    stopped: AtomicBool,
}

impl Camera {
    pub fn new(host: &str, access_code: &str) -> Arc<Self> {
        Self::with_port(host, CAMERA_PORT, access_code)
    }

    fn with_port(host: &str, port: u16, access_code: &str) -> Arc<Self> {
        Arc::new(Self {
            host: host.to_owned(),
            port,
            access_code: access_code.to_owned(),
            meta: Mutex::new(Meta::default()),
            frames: watch::channel(None).0,
            stopped: AtomicBool::new(false),
        })
    }

    /// Stop streaming now (the printer was removed or reconfigured).
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }

    /// A frame no older than `max_age`, waiting up to `wait` for the stream to
    /// deliver one. Falls back to an older frame rather than nothing.
    pub async fn frame(
        self: &Arc<Self>,
        max_age: Duration,
        wait: Duration,
    ) -> Result<Frame, SnapshotError> {
        {
            let mut m = self.meta.lock().unwrap();
            if m.unsupported {
                return Err(SnapshotError::Unsupported);
            }
            m.last_request = Some(Instant::now());
            if !m.running {
                m.running = true;
                tokio::spawn(self.clone().run());
            }
        }
        let mut rx = self.frames.subscribe();
        let fresh = |f: &Option<Frame>| f.as_ref().is_some_and(|f| f.at.elapsed() <= max_age);
        let got = tokio::time::timeout(wait, rx.wait_for(fresh)).await;
        if let Ok(Ok(f)) = got {
            return Ok(f.clone().expect("wait_for matched a frame"));
        }
        if let Some(f) = self.frames.borrow().clone() {
            return Ok(f);
        }
        let m = self.meta.lock().unwrap();
        if m.unsupported {
            return Err(SnapshotError::Unsupported);
        }
        Err(SnapshotError::Other(anyhow::anyhow!(
            "{}",
            m.last_error
                .clone()
                .unwrap_or_else(|| "camera is connecting; on weak printer Wi-Fi the first frame can take a minute or two".into())
        )))
    }

    /// Whether to keep streaming; clears `running` under the lock when not,
    /// so a concurrent `frame()` starts a new stream instead of waiting.
    fn keep_going(&self) -> bool {
        let mut m = self.meta.lock().unwrap();
        let idle = m.last_request.is_none_or(|t| t.elapsed() > IDLE);
        if idle || m.unsupported || self.stopped.load(Ordering::Acquire) {
            m.running = false;
            return false;
        }
        true
    }

    fn set_error(&self, e: impl std::fmt::Display) {
        self.meta.lock().unwrap().last_error = Some(e.to_string());
    }

    async fn run(self: Arc<Self>) {
        while self.keep_going() {
            let mut s = match open(&self.host, self.port, &self.access_code).await {
                Ok(s) => s,
                Err(SnapshotError::Unsupported) => {
                    self.meta.lock().unwrap().unsupported = true;
                    continue;
                }
                Err(SnapshotError::Other(e)) => {
                    tracing::warn!(host = %self.host, "camera connect failed: {e:#}");
                    self.set_error(format!("{e:#}"));
                    tokio::time::sleep(RETRY_DELAY).await;
                    continue;
                }
            };
            tracing::info!(host = %self.host, "camera stream up");
            while self.keep_going() {
                match read_frame(&mut s).await {
                    Ok(jpeg) => {
                        self.meta.lock().unwrap().last_error = None;
                        self.frames.send_replace(Some(Frame {
                            jpeg: Arc::new(jpeg),
                            at: Instant::now(),
                        }));
                    }
                    Err(e) => {
                        tracing::warn!(host = %self.host, "camera stream broke: {e:#}");
                        self.set_error(format!("{e:#}"));
                        break;
                    }
                }
            }
        }
        tracing::info!(host = %self.host, "camera stream closed");
    }
}

/// Encode the auth packet: payload size 0x40, type 0x3000, flags 0, 0,
/// then username and access code as NUL-padded 32-byte ASCII fields.
pub fn auth_packet(username: &str, access_code: &str) -> [u8; 80] {
    let mut p = [0u8; 80];
    p[0..4].copy_from_slice(&0x40u32.to_le_bytes());
    p[4..8].copy_from_slice(&0x3000u32.to_le_bytes());
    // bytes 8..16: flags + reserved, both zero.
    let user = username.as_bytes();
    let pass = access_code.as_bytes();
    p[16..16 + user.len().min(32)].copy_from_slice(&user[..user.len().min(32)]);
    p[48..48 + pass.len().min(32)].copy_from_slice(&pass[..pass.len().min(32)]);
    p
}

/// Whether a model string names a printer family that streams RTSPS rather
/// than the port-6000 JPEG service.
pub fn is_rtsp_model(model: &str) -> bool {
    let m = model.to_ascii_uppercase();
    m.starts_with("X1") || m.starts_with("H2") || m.starts_with("P2")
}

#[derive(Debug)]
pub enum SnapshotError {
    /// Nothing listens on 6000 (likely an RTSP-only model).
    Unsupported,
    Other(anyhow::Error),
}

/// Connect, handshake and authenticate.
async fn open(
    host: &str,
    port: u16,
    access_code: &str,
) -> Result<TlsStream<TcpStream>, SnapshotError> {
    let tcp = match tokio::time::timeout(Duration::from_secs(10), TcpStream::connect((host, port)))
        .await
    {
        Err(_) => {
            return Err(SnapshotError::Other(anyhow::anyhow!(
                "camera connect timed out"
            )));
        }
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
            return Err(SnapshotError::Unsupported);
        }
        Ok(Err(e)) => return Err(SnapshotError::Other(e.into())),
        Ok(Ok(s)) => s,
    };
    let handshake = async {
        let mut s = TlsConnector::from(tls::client_config())
            .connect(tls::server_name(host)?, tcp)
            .await
            .context("camera TLS handshake")?;
        s.write_all(&auth_packet("bblp", access_code)).await?;
        s.flush().await?;
        anyhow::Ok(s)
    };
    tokio::time::timeout(Duration::from_secs(40), handshake)
        .await
        .map_err(|_| SnapshotError::Other(anyhow::anyhow!("camera TLS handshake timed out")))?
        .map_err(SnapshotError::Other)
}
/// Fill `buf`, failing only when no bytes arrive for [`STALL_TIMEOUT`] — a
/// frame over weak printer Wi-Fi can take minutes, but it keeps moving.
async fn read_full(s: &mut TlsStream<TcpStream>, buf: &mut [u8], what: &str) -> anyhow::Result<()> {
    let mut got = 0;
    while got < buf.len() {
        let n = tokio::time::timeout(STALL_TIMEOUT, s.read(&mut buf[got..]))
            .await
            .map_err(|_| anyhow::anyhow!("{STALLED}"))?
            .with_context(|| what.to_owned())?;
        if n == 0 {
            bail!("{what}: the printer closed the stream");
        }
        got += n;
    }
    Ok(())
}

async fn read_frame(s: &mut TlsStream<TcpStream>) -> anyhow::Result<Vec<u8>> {
    let mut header = [0u8; 16];
    read_full(s, &mut header, "camera header").await?;
    let size = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
    if size == 0 || size > MAX_FRAME {
        bail!("implausible camera frame size {size} (wrong access code?)");
    }
    let mut jpeg = vec![0u8; size];
    read_full(s, &mut jpeg, "camera frame").await?;
    if !jpeg.starts_with(&[0xFF, 0xD8]) || !jpeg.ends_with(&[0xFF, 0xD9]) {
        bail!("camera frame is not a JPEG");
    }
    Ok(jpeg)
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_packet_layout() {
        let p = auth_packet("bblp", "12345678");
        assert_eq!(&p[0..4], &[0x40, 0, 0, 0]);
        assert_eq!(&p[4..8], &[0x00, 0x30, 0, 0]);
        assert_eq!(&p[8..16], &[0u8; 8]);
        assert_eq!(&p[16..20], b"bblp");
        assert!(p[20..48].iter().all(|b| *b == 0));
        assert_eq!(&p[48..56], b"12345678");
        assert!(p[56..80].iter().all(|b| *b == 0));
    }

    #[test]
    fn auth_packet_truncates_long_fields() {
        let long = "x".repeat(40);
        let p = auth_packet(&long, &long);
        assert!(p[16..48].iter().all(|b| *b == b'x'));
        assert!(p[48..80].iter().all(|b| *b == b'x'));
    }

    #[test]
    fn rtsp_models() {
        assert!(is_rtsp_model("X1C"));
        assert!(is_rtsp_model("x1e"));
        assert!(is_rtsp_model("H2D"));
        assert!(!is_rtsp_model("P1S"));
        assert!(!is_rtsp_model("A1 mini"));
    }

    #[tokio::test]
    async fn snapshot_reads_one_frame() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let acceptor = tokio_rustls::TlsAcceptor::from(tls::test_server_config(&[
            &rustls::version::TLS12,
            &rustls::version::TLS13,
        ]));
        let jpeg: Vec<u8> = [&[0xFF, 0xD8][..], &[7u8; 5000][..], &[0xFF, 0xD9][..]].concat();
        let frame = jpeg.clone();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut s = acceptor.accept(tcp).await.unwrap();
            let mut auth = [0u8; 80];
            s.read_exact(&mut auth).await.unwrap();
            assert_eq!(auth, auth_packet("bblp", "12345678"));
            let mut header = [0u8; 16];
            header[0..4].copy_from_slice(&(frame.len() as u32).to_le_bytes());
            header[8..12].copy_from_slice(&1u32.to_le_bytes());
            // Split writes, as the printer delivers ~4 KiB chunks.
            s.write_all(&header).await.unwrap();
            for chunk in frame.chunks(4096) {
                s.write_all(chunk).await.unwrap();
                s.flush().await.unwrap();
            }
            // Keep streaming until the client hangs up.
            let _ = s.read(&mut [0u8; 1]).await;
        });
        let cam = Camera::with_port("127.0.0.1", port, "12345678");
        let got = cam
            .frame(Duration::from_secs(5), Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(*got.jpeg, jpeg);
        // Served from memory while the stream stays up.
        let again = cam
            .frame(Duration::from_secs(60), Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(*again.jpeg, jpeg);
        cam.stop();
        server.abort();
    }

    #[tokio::test]
    async fn snapshot_refused_is_unsupported() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let cam = Camera::with_port("127.0.0.1", port, "x");
        assert!(matches!(
            cam.frame(Duration::from_secs(5), Duration::from_secs(5))
                .await,
            Err(SnapshotError::Unsupported)
        ));
    }
}
