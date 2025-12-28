use std::path::Path;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

use crate::schema::FILE_RUN_META_JSON;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunMeta {
    pub run_id: String,
    pub schema_version: String,
    pub git_sha: String,
    pub start_ts_unix_ms: u64,
    pub config_path: String,
    pub trade_ts_source: String,
    pub notes_enum_version: String,
}

impl RunMeta {
    pub fn write_to_dir(&self, run_dir: &Path) -> anyhow::Result<()> {
        let out_path = run_dir.join(FILE_RUN_META_JSON);
        let json = serde_json::to_vec_pretty(self).context("serialize run_meta.json")?;
        std::fs::write(&out_path, json).with_context(|| format!("write {}", out_path.display()))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn read_from_dir(run_dir: &Path) -> anyhow::Result<Self> {
        let path = run_dir.join(FILE_RUN_META_JSON);
        let raw = std::fs::read(&path).with_context(|| format!("read {}", path.display()))?;
        serde_json::from_slice(&raw).context("decode run_meta.json")
    }
}

pub fn env_git_sha() -> String {
    std::env::var("GIT_SHA").unwrap_or_else(|_| "unknown".to_string())
}
