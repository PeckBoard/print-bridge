//! print-bridge: expose Bambu Lab printers (LAN mode) over a small local
//! HTTP API. See README.md.

mod api;
mod camera;
mod commands;
mod ftps;
mod mqtt;
mod status;
mod store;
mod tls;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, bail};
use clap::Parser;
use tokio::sync::RwLock;

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Local HTTP bridge for Bambu Lab printers in LAN mode"
)]
struct Args {
    /// Address to listen on.
    #[arg(long, env = "PRINT_BRIDGE_BIND", default_value = "127.0.0.1:7781")]
    bind: SocketAddr,
    /// Bearer token clients must send.
    #[arg(
        long,
        env = "PRINT_BRIDGE_TOKEN",
        hide_env_values = true,
        conflicts_with = "token_file"
    )]
    token: Option<String>,
    /// File holding the bearer token (trailing whitespace trimmed).
    #[arg(long, env = "PRINT_BRIDGE_TOKEN_FILE")]
    token_file: Option<PathBuf>,
    /// Where printers.json lives. Default: $XDG_DATA_HOME/print-bridge or
    /// ~/.local/share/print-bridge.
    #[arg(long, env = "PRINT_BRIDGE_DATA_DIR")]
    data_dir: Option<PathBuf>,
}

fn default_data_dir() -> anyhow::Result<PathBuf> {
    if let Some(x) = std::env::var_os("XDG_DATA_HOME").filter(|v| !v.is_empty()) {
        return Ok(PathBuf::from(x).join("print-bridge"));
    }
    let home = std::env::var_os("HOME").context("HOME not set; pass --data-dir")?;
    Ok(PathBuf::from(home).join(".local/share/print-bridge"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "print_bridge=info".into()),
        )
        .init();
    tls::install_default_provider();
    let args = Args::parse();

    let token = match (&args.token, &args.token_file) {
        (Some(t), _) => t.trim().to_owned(),
        (None, Some(f)) => std::fs::read_to_string(f)
            .with_context(|| format!("reading token file {}", f.display()))?
            .trim()
            .to_owned(),
        (None, None) => {
            bail!("a token is required: pass --token, --token-file or PRINT_BRIDGE_TOKEN")
        }
    };
    if token.len() < 16 {
        bail!("token must be at least 16 characters");
    }

    let data_dir = match args.data_dir {
        Some(d) => d,
        None => default_data_dir()?,
    };
    let store = store::Store::new(&data_dir);
    let saved = store.load()?;
    let mut printers = HashMap::new();
    for (id, cfg) in saved {
        tracing::info!(printer = %id, host = %cfg.host, "loaded printer");
        printers.insert(id.clone(), Arc::new(api::Printer::connect(&id, cfg)));
    }

    let state = Arc::new(api::AppState {
        token,
        store: Some(store),
        connect: true,
        printers: RwLock::new(printers),
    });
    let listener = tokio::net::TcpListener::bind(args.bind)
        .await
        .with_context(|| format!("binding {}", args.bind))?;
    tracing::info!(bind = %args.bind, data_dir = %data_dir.display(), "print-bridge listening");
    axum::serve(listener, api::router(state))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
