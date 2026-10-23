//! Persisted printer registry: `{data_dir}/printers.json`, written 0600
//! because it holds LAN access codes.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PrinterConfig {
    pub host: String,
    pub serial: String,
    pub access_code: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

pub struct Store {
    path: PathBuf,
}

impl Store {
    pub fn new(data_dir: &Path) -> Self {
        Self {
            path: data_dir.join("printers.json"),
        }
    }

    pub fn load(&self) -> anyhow::Result<BTreeMap<String, PrinterConfig>> {
        match std::fs::read(&self.path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .with_context(|| format!("parsing {}", self.path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
            Err(e) => Err(e).with_context(|| format!("reading {}", self.path.display())),
        }
    }

    /// Atomic write (temp file + rename) so a crash never leaves a torn file.
    pub fn save(&self, printers: &BTreeMap<String, PrinterConfig>) -> anyhow::Result<()> {
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let tmp = self.path.with_extension("json.tmp");
        let body = serde_json::to_vec_pretty(printers)?;
        {
            use std::io::Write;
            let mut opts = std::fs::OpenOptions::new();
            opts.write(true).create(true).truncate(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                opts.mode(0o600);
            }
            let mut f = opts.open(&tmp)?;
            f.write_all(&body)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let dir = std::env::temp_dir().join(format!("print-bridge-store-{}", std::process::id()));
        let store = Store::new(&dir);
        assert!(store.load().unwrap().is_empty());
        let mut m = BTreeMap::new();
        m.insert(
            "p1s".to_string(),
            PrinterConfig {
                host: "192.168.1.50".into(),
                serial: "01P00A000000000".into(),
                access_code: "12345678".into(),
                model: Some("P1S".into()),
            },
        );
        store.save(&m).unwrap();
        assert_eq!(store.load().unwrap(), m);
        let _ = std::fs::remove_dir_all(dir);
    }
}
