# print-bridge

A small local daemon that talks to Bambu Lab printers in **LAN mode** and
exposes them over a simple HTTP API (used by the Peckboard 3D-printing
plugin). One static binary, no runtime dependencies (no OpenSSL, no ffmpeg).

- **Status**: one MQTT-over-TLS connection per printer (port 8883), with
  auto-reconnect and exponential backoff (1 s → 60 s). Asks for a full
  `pushall` on every connect and every 5 minutes; merges partial delta
  reports into a cached full state.
- **Upload**: implicit FTPS (port 990) with TLS session reuse on the data
  channel, as the printer's vsftpd requires.
- **Print control**: `project_file` / `gcode_file`, pause, resume, stop.
- **Camera**: single JPEG snapshot from the P1/A1 camera service (port 6000).

## Install

Download `print-bridge-linux-x86_64` or `print-bridge-linux-aarch64` from
the releases page (static musl builds), or build it:

```bash
cargo build --release   # target/release/print-bridge
```

Run it as a systemd **user** service — see `print-bridge.service` for the
install commands. In short:

```bash
install -Dm755 print-bridge ~/.local/bin/print-bridge
install -Dm644 print-bridge.service ~/.config/systemd/user/print-bridge.service
mkdir -p ~/.config/print-bridge
head -c 32 /dev/urandom | base64 > ~/.config/print-bridge/token
chmod 600 ~/.config/print-bridge/token
systemctl --user daemon-reload && systemctl --user enable --now print-bridge
```

## Flags

| Flag           | Env                       | Default                       |
| -------------- | ------------------------- | ----------------------------- |
| `--bind`       | `PRINT_BRIDGE_BIND`       | `127.0.0.1:7781`              |
| `--token`      | `PRINT_BRIDGE_TOKEN`      | — (one of token / token-file) |
| `--token-file` | `PRINT_BRIDGE_TOKEN_FILE` | —                             |
| `--data-dir`   | `PRINT_BRIDGE_DATA_DIR`   | `~/.local/share/print-bridge` |

A token is mandatory and must be at least 16 characters. Printers (including
access codes) persist in `{data-dir}/printers.json`, written mode `0600`.
Logging is controlled with `RUST_LOG` (e.g. `RUST_LOG=print_bridge=debug`).

Keep the default loopback bind unless you need remote access: the API is
plain HTTP, so anything beyond localhost should sit behind a TLS proxy.

## Printer Setup

On the printer: **Settings → Network → LAN Only Mode** on (or at least LAN
access enabled), then note the **IP address** and the 8-character **access
code**. The serial number is under **Settings → Device**.

### Firmware Caveats — Authorization Control / Developer Mode

Since early 2025 Bambu firmware ships an **Authorization Control** system:
on updated X1, P1, A1 and H2 firmware, third-party LAN clients can still
**read** status over MQTT, but **control commands** (start, pause, stop) and
in some cases FTPS uploads are rejected unless the printer is in
**LAN Only Mode with Developer Mode enabled** (Settings → Network → LAN Only
Mode → Developer Mode). Developer Mode disables cloud features and Bambu
Handy for that printer. If commands return `{ok:true}` but nothing happens,
check the bridge log for `printer rejected command` and enable Developer
Mode. Older firmware is not affected.

Other caveats:

- **Camera**: P1/A1 printers serve JPEG frames on TLS port 6000 —
  `/snapshot` returns one frame. X1, H2 and P2S stream **RTSPS** on port 322
  (`rtsps://bblp:<code>@<ip>:322/streaming/live/1`); decoding that needs
  H.264 + RTSP, so `/snapshot` returns **404** for those models (set
  `model` when registering so the bridge can tell; otherwise it tries port
  6000 and returns 404 if nothing listens). H2 series also need *LAN Mode
  Liveview* enabled on the printer.
- **Certificates**: printers present a per-device certificate signed by
  Bambu's private CA with the serial as its name, which can't be checked
  against an IP. The bridge accepts any certificate (handshake signatures
  are still verified). Run it on a trusted LAN.
- **Upload location**: files go to the FTPS root (the SD card on P1/X1;
  internal storage on A1) and start with `url: ftp:///<file>`.
- **Temperatures**: H2D-style dual-nozzle firmware reports extruder
  temperatures in a different structure; only the classic
  `nozzle_temper` / `bed_temper` / `chamber_temper` fields are mapped.

## HTTP API

Every request needs `Authorization: Bearer <token>`; otherwise `401`.
Errors are `{"ok": false, "error": "..."}`.

| Method & path                   | Body / query                                                    | Result                                                                                     |
| ------------------------------- | --------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `GET /health`                   |                                                                 | `{ok: true, version}`                                                                      |
| `GET /printers`                 |                                                                 | `{printers: [{id, host, serial, model, connected}]}` — never includes access codes         |
| `PUT /printers/{id}`            | `{host, serial, access_code, model?}`                           | `{ok: true}`; (re)connects. `id`: 1–64 chars of `[A-Za-z0-9_-]`                            |
| `DELETE /printers/{id}`         |                                                                 | `{ok: true}`; disconnects                                                                  |
| `GET /printers/{id}/status`     |                                                                 | normalised status (below)                                                                  |
| `POST /printers/{id}/upload`    | `?name=<file>`, raw body (≤ 1 GiB)                              | `{ok: true}` once the FTPS transfer completes; `502` on FTP/TLS failure                    |
| `POST /printers/{id}/start`     | `{file, plate?: 1, use_ams?: false, bed_leveling?: true, ams_mapping?: [int]}` | `{ok: true}`; `.3mf` → `project_file` (`Metadata/plate_{plate}.gcode`), `.gcode` → `gcode_file` |
| `POST /printers/{id}/pause`     |                                                                 | `{ok: true}`                                                                               |
| `POST /printers/{id}/resume`    |                                                                 | `{ok: true}`                                                                               |
| `POST /printers/{id}/stop`      |                                                                 | `{ok: true}`                                                                               |
| `GET /printers/{id}/snapshot`   |                                                                 | `image/jpeg`; `404` if the model has no port-6000 camera                                   |

Commands return `503` while the MQTT link is down (they are not queued, so a
`stop` can never arrive minutes late) and `404` for an unknown printer.
`{ok:true}` means the command was **sent**; the printer acknowledges
asynchronously — watch `/status` for the effect.

### Status Shape

```json
{
  "state": "idle | printing | paused | finished | error | offline",
  "progress": 0.42,
  "file": "benchy",
  "elapsed_s": 600,
  "eta_s": 2220,
  "layer": 61,
  "total_layers": 240,
  "temps": {
    "nozzle": { "actual": 219.8, "target": 220.0 },
    "bed": { "actual": 54.9, "target": 55.0 },
    "chamber": { "actual": 31.0, "target": null }
  },
  "message": null
}
```

- `state`: `offline` while MQTT is disconnected; otherwise from
  `gcode_state` — `RUNNING`/`PREPARE`/`SLICING` → `printing`, `PAUSE` →
  `paused`, `FINISH` → `finished`, `FAILED` → `error`, anything else → `idle`.
- `progress`: `mc_percent / 100`.
- `file`: `subtask_name`, falling back to `gcode_file`.
- `elapsed_s`: now − `gcode_start_time`, only while printing/paused.
- `eta_s`: `mc_remaining_time` (minutes) × 60, only while printing/paused.
- `layer` / `total_layers`: `layer_num` / `total_layer_num` (`total_layers`
  null when 0).
- `temps`: a key is omitted when the printer doesn't report it (A1/P1 have
  no chamber sensor).
- `message`: `print_error` as `print_error XXXX_XXXX` and each HMS entry as
  `HMS_XXXX_XXXX_XXXX_XXXX` (searchable on the Bambu wiki), `; `-joined.

### Example

```bash
T=$(cat ~/.config/print-bridge/token); B=http://127.0.0.1:7781
curl -H "Authorization: Bearer $T" -X PUT $B/printers/p1s \
  -H 'content-type: application/json' \
  -d '{"host":"192.168.1.50","serial":"01P00A000000000","access_code":"12345678","model":"P1S"}'
curl -H "Authorization: Bearer $T" "$B/printers/p1s/upload?name=benchy.gcode.3mf" \
  --data-binary @benchy.gcode.3mf
curl -H "Authorization: Bearer $T" -X POST $B/printers/p1s/start \
  -H 'content-type: application/json' -d '{"file":"benchy.gcode.3mf","plate":1}'
curl -H "Authorization: Bearer $T" $B/printers/p1s/status
curl -H "Authorization: Bearer $T" $B/printers/p1s/snapshot -o snap.jpg
```

## Protocol References

- [OpenBambuAPI](https://github.com/Doridian/OpenBambuAPI) — MQTT, FTPS and
  camera protocol docs.
- [ha-bambulab](https://github.com/greghesp/ha-bambulab) and
  [bambulabs_api](https://github.com/BambuTools/bambulabs_api).

## Development

```bash
cargo test     # merge/normalise, camera packet, fake FTPS + camera servers, HTTP routes
cargo clippy --all-targets -- -D warnings
```

Releases: push a `v*` tag; `.github/workflows/release.yml` builds static
musl binaries for x86_64 and aarch64 with `cross` and attaches them.
