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

/// A logged-in control connection plus what the data channel needs to
/// resume its TLS session.
struct Session {
    ctl: Control<tokio_rustls::client::TlsStream<TcpStream>>,
    connector: TlsConnector,
    server: rustls::pki_types::ServerName<'static>,
    host: String,
}

async fn login(host: &str, port: u16, access_code: &str) -> anyhow::Result<Session> {
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
    Ok(Session {
        ctl,
        connector,
        server,
        host: host.to_owned(),
    })
}

impl Session {
    /// PASV, TCP-connect the data port, send `line`, then do the data TLS
    /// handshake (resuming the control session).
    async fn open_data(
        &mut self,
        line: &str,
    ) -> anyhow::Result<tokio_rustls::client::TlsStream<TcpStream>> {
        let pasv = self.ctl.cmd("PASV", &[227]).await?;
        let port = parse_pasv_port(&pasv)?;
        let data_tcp =
            tokio::time::timeout(IO_TIMEOUT, TcpStream::connect((self.host.as_str(), port)))
                .await
                .context("FTPS data connect timed out")??;
        self.ctl.cmd(line, &[125, 150]).await?;
        let data_tls = self
            .connector
            .connect(self.server.clone(), data_tcp)
            .await
            .context("FTPS data TLS handshake (session reuse)")?;
        let resumed = data_tls.get_ref().1.handshake_kind();
        tracing::debug!(?resumed, "FTPS data channel handshake");
        Ok(data_tls)
    }

    async fn transfer_done(&mut self) -> anyhow::Result<()> {
        let (code, text) = self.ctl.reply().await?;
        if code != 226 && code != 250 {
            bail!("FTP transfer failed: {text}");
        }
        Ok(())
    }

    /// `LIST dir`; returns the raw listing.
    async fn list(&mut self, dir: &str) -> anyhow::Result<String> {
        let mut data = self.open_data(&format!("LIST {dir}")).await?;
        let mut buf = Vec::new();
        let mut chunk = [0u8; 8192];
        // vsftpd may close the data socket without a TLS close_notify; keep
        // whatever arrived before the (unexpected) EOF.
        let _ = tokio::time::timeout(IO_TIMEOUT, async {
            while let Ok(n) = data.read(&mut chunk).await {
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
        })
        .await;
        drop(data);
        self.transfer_done().await?;
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }

    async fn quit(mut self) {
        let _ = self.ctl.cmd("QUIT", &[221]).await;
    }
}

async fn upload_to(
    host: &str,
    port: u16,
    access_code: &str,
    name: &str,
    data: &[u8],
) -> anyhow::Result<()> {
    let mut s = login(host, port, access_code).await?;
    let mut data_tls = s.open_data(&format!("STOR {name}")).await?;

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

    s.transfer_done().await?;
    s.quit().await;
    Ok(())
}

/// One file on the printer's storage.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct RemoteFile {
    /// Path relative to the storage root (`benchy.3mf`, `cache/x.3mf`).
    pub name: String,
    pub size: u64,
    /// Unix seconds; `null` when the listing's date can't be parsed.
    pub mtime: Option<u64>,
}

/// Directories listed by [`list_files`], with the prefix their entries get.
const LIST_DIRS: [(&str, &str); 2] = [("/", ""), ("/cache", "cache/")];

/// List the files in `/` and `/cache` (a missing `/cache` is skipped).
pub async fn list_files(host: &str, access_code: &str) -> anyhow::Result<Vec<RemoteFile>> {
    list_files_at(host, FTPS_PORT, access_code, now_unix()).await
}

async fn list_files_at(
    host: &str,
    port: u16,
    access_code: &str,
    now: u64,
) -> anyhow::Result<Vec<RemoteFile>> {
    let mut s = login(host, port, access_code).await?;
    let mut out = Vec::new();
    for (i, (dir, prefix)) in LIST_DIRS.iter().enumerate() {
        match s.list(dir).await {
            Ok(text) => out.extend(parse_list(&text, prefix, now)),
            // The root must list; the cache dir may not exist.
            Err(e) if i == 0 => return Err(e),
            Err(e) => tracing::debug!("LIST {dir} failed: {e:#}"),
        }
    }
    s.quit().await;
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

/// Delete `name` (a path from [`list_files`]) on the printer.
pub async fn delete_file(host: &str, access_code: &str, name: &str) -> anyhow::Result<()> {
    delete_file_at(host, FTPS_PORT, access_code, name).await
}

async fn delete_file_at(
    host: &str,
    port: u16,
    access_code: &str,
    name: &str,
) -> anyhow::Result<()> {
    let mut s = login(host, port, access_code).await?;
    s.ctl.cmd(&format!("DELE /{name}"), &[250]).await?;
    s.quit().await;
    Ok(())
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

const MONTHS: [&str; 12] = [
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
];

/// Days since 1970-01-01 for a proleptic Gregorian date.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn year_of(unix: u64) -> i64 {
    // Good enough to pick the listing's implied year.
    let days = (unix / 86_400) as i64;
    let mut y = 1970 + days / 365;
    while days_from_civil(y, 1, 1) > days {
        y -= 1;
    }
    while days_from_civil(y + 1, 1, 1) <= days {
        y += 1;
    }
    y
}

/// `ls -l` date: `Oct 22 10:00` (this year, or last year when that would be
/// in the future) or `Oct 22  2023`.
fn parse_ls_date(mon: &str, day: &str, time_or_year: &str, now: u64) -> Option<u64> {
    let m = MONTHS.iter().position(|x| x.eq_ignore_ascii_case(mon))? as u32 + 1;
    let d: u32 = day.parse().ok().filter(|d| (1..=31).contains(d))?;
    let (year, secs) = if let Some((h, min)) = time_or_year.split_once(':') {
        let (h, min): (u64, u64) = (h.parse().ok()?, min.parse().ok()?);
        let secs = h * 3600 + min * 60;
        let this = year_of(now);
        let t = days_from_civil(this, m, d) * 86_400 + secs as i64;
        // More than a day ahead means the entry is from last year.
        if t > now as i64 + 86_400 {
            (this - 1, secs)
        } else {
            (this, secs)
        }
    } else {
        (time_or_year.parse().ok()?, 0)
    };
    let t = days_from_civil(year, m, d) * 86_400 + secs as i64;
    u64::try_from(t).ok()
}

/// Parse a unix `LIST` reply. Regular files only; `prefix` is prepended to
/// each name.
pub fn parse_list(text: &str, prefix: &str, now: u64) -> Vec<RemoteFile> {
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim_end_matches('\r');
        if !line.starts_with('-') {
            continue; // directories, links, "total N"
        }
        // perms links owner group size mon day time|year name...
        let mut rest = line;
        let mut fields = Vec::with_capacity(8);
        for _ in 0..8 {
            rest = rest.trim_start();
            let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
            fields.push(&rest[..end]);
            rest = &rest[end..];
        }
        let name = rest.strip_prefix(' ').unwrap_or(rest).trim_start();
        let Ok(size) = fields[4].parse::<u64>() else {
            continue;
        };
        if name.is_empty() || name == "." || name == ".." {
            continue;
        }
        out.push(RemoteFile {
            name: format!("{prefix}{name}"),
            size,
            mtime: parse_ls_date(fields[5], fields[6], fields[7], now),
        });
    }
    out
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
        let mut deleted = String::new();
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
                "LIST" if arg == "/cache" => say(&mut ctl, "550 Failed to open directory.").await?,
                "LIST" => {
                    let (tcp, _) = data_listener.take().unwrap().accept().await?;
                    say(&mut ctl, "150 Here comes the directory listing.").await?;
                    let mut data = acceptor.accept(tcp).await?;
                    data.write_all(
                        b"drwxr-xr-x    2 root root     4096 Jan 01  2024 timelapse\r\n\
                          -rw-r--r--    1 root root  1234567 Mar 05  2024 my benchy.3mf\r\n",
                    )
                    .await?;
                    let _ = data.shutdown().await;
                    drop(data);
                    say(&mut ctl, "226 Directory send OK.").await?;
                }
                "DELE" => {
                    deleted = arg.to_owned();
                    say(&mut ctl, "250 Delete operation successful.").await?;
                }
                "QUIT" => {
                    let _ = say(&mut ctl, "221 Goodbye.").await;
                    return Ok((deleted, Vec::new()));
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

    #[test]
    fn parses_unix_listing() {
        let now = (days_from_civil(2026, 10, 23) * 86_400) as u64;
        let text = "total 8\r\n\
            drwxr-xr-x    2 root     root         4096 Jan 01  2024 cache\r\n\
            -rw-r--r--    1 root     root      1234567 Oct 22 10:00 benchy.gcode.3mf\r\n\
            -rw-r--r--    1 root     root           42 Dec 25 08:30 old one.gcode\r\n\
            -rw-r--r--    1 root     root            7 Mar 05  2024 x.3mf\r\n\
            lrwxrwxrwx    1 root     root            4 Mar 05  2024 link -> x\r\n";
        let files = parse_list(text, "cache/", now);
        assert_eq!(files.len(), 3);
        assert_eq!(files[0].name, "cache/benchy.gcode.3mf");
        assert_eq!(files[0].size, 1_234_567);
        assert_eq!(
            files[0].mtime,
            Some((days_from_civil(2026, 10, 22) * 86_400 + 36_000) as u64)
        );
        assert_eq!(files[1].name, "cache/old one.gcode", "spaces kept");
        assert_eq!(
            files[1].mtime,
            Some((days_from_civil(2025, 12, 25) * 86_400 + 30_600) as u64),
            "future date means last year"
        );
        assert_eq!(
            files[2].mtime,
            Some((days_from_civil(2024, 3, 5) * 86_400) as u64)
        );
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(year_of(now), 2026);
    }

    #[tokio::test]
    async fn lists_and_deletes_over_ftps() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(fake_vsftpd(
            listener,
            crate::tls::test_server_config(&[&rustls::version::TLS12]),
        ));
        let files = list_files_at("127.0.0.1", port, "12345678", now_unix())
            .await
            .unwrap();
        assert_eq!(
            files.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
            ["my benchy.3mf"]
        );
        server.await.unwrap().unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(fake_vsftpd(
            listener,
            crate::tls::test_server_config(&[&rustls::version::TLS12]),
        ));
        delete_file_at("127.0.0.1", port, "12345678", "cache/a.3mf")
            .await
            .unwrap();
        assert_eq!(server.await.unwrap().unwrap().0, "/cache/a.3mf");
    }
}
