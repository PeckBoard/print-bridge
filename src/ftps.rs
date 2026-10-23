//! Minimal implicit-FTPS uploader (port 990) for the printer's vsftpd.
//!
//! Hand-rolled instead of an FTP crate because the one requirement that
//! breaks most clients is `require_ssl_reuse`: the data channel's TLS
//! session must resume the control channel's. rustls resumes automatically
//! when both connections use the same `Arc<ClientConfig>` (shared session
//! cache) and the same `ServerName`, so we keep both under our control.
//! Sequence mirrors Python ftplib's FTP_TLS: TCP-connect the PASV port,
//! send STOR, wait for 150, then do the data TLS handshake.

use std::time::Duration;

use anyhow::{Context, bail};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::tls;

pub const FTPS_PORT: u16 = 990;
const IO_TIMEOUT: Duration = Duration::from_secs(30);

struct Control<S> {
    io: BufReader<S>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Control<S> {
    /// Read one (possibly multi-line) reply; returns (code, text).
    async fn reply(&mut self) -> anyhow::Result<(u16, String)> {
        let mut text = String::new();
        let mut first = String::new();
        tokio::time::timeout(IO_TIMEOUT, self.io.read_line(&mut first))
            .await
            .context("FTP reply timed out")??;
        if first.len() < 4 {
            bail!("FTP connection closed or short reply: {first:?}");
        }
        let code: u16 = first[..3].parse().context("bad FTP reply code")?;
        text.push_str(first.trim_end());
        if first.as_bytes()[3] == b'-' {
            let end = format!("{} ", &first[..3]);
            loop {
                let mut line = String::new();
                let n = tokio::time::timeout(IO_TIMEOUT, self.io.read_line(&mut line))
                    .await
                    .context("FTP reply timed out")??;
                if n == 0 {
                    bail!("FTP connection closed mid-reply");
                }
                text.push('\n');
                text.push_str(line.trim_end());
                if line.starts_with(&end) {
                    break;
                }
            }
        }
        Ok((code, text))
    }

    async fn cmd(&mut self, line: &str, expect: &[u16]) -> anyhow::Result<String> {
        // One write = one TLS record. The printer's FTP server parses each
        // record as a whole command: `PASS x` and `\r\n` sent separately get
        // a bare `332` instead of `230`.
        let w = self.io.get_mut();
        w.write_all(format!("{line}\r\n").as_bytes()).await?;
        w.flush().await?;
        let (code, text) = self.reply().await?;
        if !expect.contains(&code) {
            let shown = if line.starts_with("PASS ") {
                "PASS ****"
            } else {
                line
            };
            bail!("FTP `{shown}` failed: {text}");
        }
        Ok(text)
    }
}

/// Parse the port out of `227 Entering Passive Mode (h1,h2,h3,h4,p1,p2)`.
/// The address is ignored: the printer sometimes advertises an internal one,
/// and we dial the host we already reached.
pub fn parse_pasv_port(text: &str) -> anyhow::Result<u16> {
    let inner = text
        .split_once('(')
        .and_then(|(_, r)| r.split_once(')'))
        .map(|(i, _)| i)
        .context("no (h,h,h,h,p,p) in PASV reply")?;
    let nums: Vec<u16> = inner
        .split(',')
        .map(|s| s.trim().parse::<u16>())
        .collect::<Result<_, _>>()
        .context("bad PASV numbers")?;
    if nums.len() != 6 || nums[4] > 255 || nums[5] > 255 {
        bail!("bad PASV reply: {text}");
    }
    Ok(nums[4] * 256 + nums[5])
}

/// Upload `data` as `/{name}` on the printer.
pub async fn upload(host: &str, access_code: &str, name: &str, data: &[u8]) -> anyhow::Result<()> {
    upload_to(host, FTPS_PORT, access_code, name, data).await
}

async fn upload_to(
    host: &str,
    port: u16,
    access_code: &str,
    name: &str,
    data: &[u8],
) -> anyhow::Result<()> {
    let config = tls::client_config();
    let connector = TlsConnector::from(config);
    let server = tls::server_name(host)?;

    let tcp = tokio::time::timeout(IO_TIMEOUT, TcpStream::connect((host, port)))
        .await
        .context("FTPS connect timed out")??;
    let tls = connector
        .connect(server.clone(), tcp)
        .await
        .context("FTPS control TLS handshake")?;
    let mut ctl = Control {
        io: BufReader::new(tls),
    };

    let (code, text) = ctl.reply().await?;
    if code != 220 {
        bail!("unexpected FTP greeting: {text}");
    }
    ctl.cmd("USER bblp", &[331, 230]).await?;
    ctl.cmd(&format!("PASS {access_code}"), &[230]).await?;
    ctl.cmd("PBSZ 0", &[200]).await?;
    ctl.cmd("PROT P", &[200]).await?;
    ctl.cmd("TYPE I", &[200]).await?;
    let pasv = ctl.cmd("PASV", &[227]).await?;
    let port = parse_pasv_port(&pasv)?;

    let data_tcp = tokio::time::timeout(IO_TIMEOUT, TcpStream::connect((host, port)))
        .await
        .context("FTPS data connect timed out")??;
    ctl.cmd(&format!("STOR {name}"), &[125, 150]).await?;
    let mut data_tls = connector
        .connect(server, data_tcp)
        .await
        .context("FTPS data TLS handshake (session reuse)")?;
    let resumed = data_tls.get_ref().1.handshake_kind();
    tracing::debug!(?resumed, "FTPS data channel handshake");

    data_tls.write_all(data).await.context("FTPS data write")?;
    data_tls.shutdown().await.context("FTPS data close")?;
    // Drain until the server closes its side. Under TLS 1.3 the server sends
    // session tickets on the data channel too; closing a socket with unread
    // bytes makes the kernel send RST, which can discard the tail of the
    // upload before the server has read it.
    let mut sink = [0u8; 4096];
    let _ = tokio::time::timeout(IO_TIMEOUT, async {
        while matches!(data_tls.read(&mut sink).await, Ok(n) if n > 0) {}
    })
    .await;
    drop(data_tls);

    let (code, text) = ctl.reply().await?;
    if code != 226 && code != 250 {
        bail!("FTP transfer failed: {text}");
    }
    let _ = ctl.cmd("QUIT", &[221]).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pasv_port() {
        assert_eq!(
            parse_pasv_port("227 Entering Passive Mode (192,168,1,50,195,80).").unwrap(),
            195 * 256 + 80
        );
        assert!(parse_pasv_port("227 nonsense").is_err());
        assert!(parse_pasv_port("227 (1,2,3,4,999,1)").is_err());
    }

    #[tokio::test]
    async fn multiline_reply() {
        let (client, mut server) = tokio::io::duplex(1024);
        server
            .write_all(b"230-Welcome\r\n line two\r\n230 Login successful.\r\n")
            .await
            .unwrap();
        let mut ctl = Control {
            io: BufReader::new(client),
        };
        let (code, text) = ctl.reply().await.unwrap();
        assert_eq!(code, 230);
        assert!(text.ends_with("230 Login successful."));
    }

    /// Records each `poll_write` call and answers every command with `230`.
    #[derive(Default)]
    struct Writes {
        writes: Vec<Vec<u8>>,
        replies: std::collections::VecDeque<u8>,
    }

    impl AsyncWrite for Writes {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            self.writes.push(buf.to_vec());
            self.replies.extend(b"230 \r\n");
            std::task::Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    impl AsyncRead for Writes {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            while buf.remaining() > 0 {
                let Some(b) = self.replies.pop_front() else {
                    break;
                };
                buf.put_slice(&[b]);
            }
            std::task::Poll::Ready(Ok(()))
        }
    }

    /// The P1S FTP server treats each TLS record as a complete command, so a
    /// command and its CRLF must go out in one write.
    #[tokio::test]
    async fn command_and_crlf_are_one_write() {
        let mut ctl = Control {
            io: BufReader::new(Writes::default()),
        };
        ctl.cmd("PASS 12345678", &[230]).await.unwrap();
        assert_eq!(ctl.io.get_ref().writes, vec![b"PASS 12345678\r\n".to_vec()]);
    }

    /// Fake implicit-FTPS server with vsftpd's `require_ssl_reuse`: the
    /// data channel is refused unless its TLS session resumed the control
    /// channel's. Returns the uploaded (name, bytes).
    async fn fake_vsftpd(
        listener: tokio::net::TcpListener,
        config: std::sync::Arc<rustls::ServerConfig>,
    ) -> anyhow::Result<(String, Vec<u8>)> {
        let acceptor = tokio_rustls::TlsAcceptor::from(config);
        let (tcp, _) = listener.accept().await?;
        let mut ctl = BufReader::new(acceptor.accept(tcp).await?);
        async fn say<S: AsyncRead + AsyncWrite + Unpin>(
            s: &mut BufReader<S>,
            m: &str,
        ) -> anyhow::Result<()> {
            s.get_mut().write_all(format!("{m}\r\n").as_bytes()).await?;
            Ok(s.get_mut().flush().await?)
        }
        say(&mut ctl, "220-print-bridge test\r\n220 ready").await?;
        let mut data_listener = None;
        loop {
            let mut line = String::new();
            if ctl.read_line(&mut line).await? == 0 {
                bail!("client hung up");
            }
            let line = line.trim_end();
            let (cmd, arg) = line.split_once(' ').unwrap_or((line, ""));
            match cmd {
                "USER" => say(&mut ctl, "331 Please specify the password.").await?,
                "PASS" if arg == "12345678" => say(&mut ctl, "230 Login successful.").await?,
                "PASS" => say(&mut ctl, "530 Login incorrect.").await?,
                "PBSZ" | "PROT" | "TYPE" => say(&mut ctl, "200 OK.").await?,
                "PASV" => {
                    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
                    let p = l.local_addr()?.port();
                    data_listener = Some(l);
                    // Advertise a bogus address, as some firmware does.
                    say(
                        &mut ctl,
                        &format!(
                            "227 Entering Passive Mode (10,9,8,7,{},{}).",
                            p / 256,
                            p % 256
                        ),
                    )
                    .await?;
                }
                "STOR" => {
                    let (tcp, _) = data_listener.take().unwrap().accept().await?;
                    say(&mut ctl, "150 Ok to send data.").await?;
                    let mut data = acceptor.accept(tcp).await?;
                    let kind = data.get_ref().1.handshake_kind();
                    if kind != Some(rustls::HandshakeKind::Resumed) {
                        say(
                            &mut ctl,
                            "522 SSL connection failed: session reuse required",
                        )
                        .await?;
                        bail!("data channel did not resume: {kind:?}");
                    }
                    let mut buf = Vec::new();
                    data.read_to_end(&mut buf).await?;
                    // Like vsftpd: close the data channel, then report.
                    let _ = data.shutdown().await;
                    drop(data);
                    say(&mut ctl, "226 Transfer complete.").await?;
                    let _ = ctl.read_line(&mut String::new()).await;
                    let _ = say(&mut ctl, "221 Goodbye.").await;
                    return Ok((arg.to_owned(), buf));
                }
                _ => say(&mut ctl, "500 Unknown command.").await?,
            }
        }
    }

    #[tokio::test]
    async fn upload_resumes_tls_session_on_data_channel() {
        for versions in [
            &[&rustls::version::TLS12][..],
            &[&rustls::version::TLS13][..],
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let server = tokio::spawn(fake_vsftpd(
                listener,
                crate::tls::test_server_config(versions),
            ));
            let payload: Vec<u8> = (0..200_000u32).map(|i| i as u8).collect();
            let up = upload_to("127.0.0.1", port, "12345678", "benchy.gcode.3mf", &payload).await;
            let res = server.await.unwrap();
            if let Err(e) = &up {
                panic!("{versions:?}: client {e:#}; server {res:?}");
            }
            let (name, got) = res.unwrap();
            assert_eq!(name, "benchy.gcode.3mf");
            assert_eq!(got, payload, "{versions:?}");
        }
    }

    #[tokio::test]
    async fn upload_reports_bad_access_code() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(fake_vsftpd(
            listener,
            crate::tls::test_server_config(&[&rustls::version::TLS12]),
        ));
        let e = upload_to("127.0.0.1", port, "wrong", "a.3mf", b"x")
            .await
            .unwrap_err();
        assert!(e.to_string().contains("PASS ****"), "{e}");
        assert!(!e.to_string().contains("wrong"));
        server.abort();
    }
}
