//! Single-frame snapshot from the P1/A1 camera service: TLS on port 6000,
//! an 80-byte auth packet, then a stream of `[16-byte header][JPEG]` frames
//! (all integers little-endian). X1/H2/P2S use RTSPS on 322 instead, which
//! needs an RTSP + H.264 decoder; the bridge does not support those.

use std::time::Duration;

use anyhow::{Context, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::tls;

pub const CAMERA_PORT: u16 = 6000;
const MAX_FRAME: usize = 8 * 1024 * 1024;

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

pub async fn snapshot(host: &str, access_code: &str) -> Result<Vec<u8>, SnapshotError> {
    snapshot_from(host, CAMERA_PORT, access_code).await
}

async fn snapshot_from(host: &str, port: u16, access_code: &str) -> Result<Vec<u8>, SnapshotError> {
    let tcp = match tokio::time::timeout(Duration::from_secs(5), TcpStream::connect((host, port)))
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
    tokio::time::timeout(Duration::from_secs(10), read_frame(tcp, host, access_code))
        .await
        .map_err(|_| SnapshotError::Other(anyhow::anyhow!("camera frame timed out")))?
        .map_err(SnapshotError::Other)
}

async fn read_frame(tcp: TcpStream, host: &str, access_code: &str) -> anyhow::Result<Vec<u8>> {
    let mut s = TlsConnector::from(tls::client_config())
        .connect(tls::server_name(host)?, tcp)
        .await
        .context("camera TLS handshake")?;
    s.write_all(&auth_packet("bblp", access_code)).await?;
    s.flush().await?;
    let mut header = [0u8; 16];
    s.read_exact(&mut header).await.context("camera header")?;
    let size = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
    if size == 0 || size > MAX_FRAME {
        bail!("implausible camera frame size {size} (wrong access code?)");
    }
    let mut jpeg = vec![0u8; size];
    s.read_exact(&mut jpeg).await.context("camera frame")?;
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
        let got = snapshot_from("127.0.0.1", port, "12345678").await.unwrap();
        assert_eq!(got, jpeg);
        server.abort();
    }

    #[tokio::test]
    async fn snapshot_refused_is_unsupported() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        assert!(matches!(
            snapshot_from("127.0.0.1", port, "x").await,
            Err(SnapshotError::Unsupported)
        ));
    }
}
