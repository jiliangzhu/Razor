use std::path::Path;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

use crate::schema::FILE_RUN_META_JSON;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SimStressProfile {
    #[serde(default)]
    pub force_chase_fail: bool,
    #[serde(default)]
    pub latency_spike_ms: u64,
    #[serde(default)]
    pub latency_spike_every: u64,
    #[serde(default)]
    pub drop_book_pct: f64,
    #[serde(default)]
    pub http_429_every: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunMeta {
    pub run_id: String,
    pub schema_version: String,
    pub git_sha: String,
    pub start_ts_unix_ms: u64,
    pub config_path: String,
    pub trade_ts_source: String,
    pub notes_enum_version: String,
    #[serde(default)]
    pub trade_poll_taker_only: Option<bool>,
    #[serde(default)]
    pub sim_stress: SimStressProfile,
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
    std::env::var("GIT_SHA")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .or_else(read_git_commit)
        .unwrap_or_else(|| "unknown".to_string())
}

fn read_git_commit() -> Option<String> {
    let head = std::fs::read_to_string(".git/HEAD").ok()?;
    let head = head.trim();

    if let Some(rest) = head.strip_prefix("ref:") {
        let reference = rest.trim();
        let ref_path = Path::new(".git").join(reference);
        if let Ok(commit) = std::fs::read_to_string(&ref_path) {
            return Some(commit.trim().to_string());
        }
        // Fallback: packed-refs (best-effort).
        let packed = std::fs::read_to_string(".git/packed-refs").ok()?;
        for line in packed.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with('^') {
                continue;
            }
            let mut parts = line.split_whitespace();
            let commit = parts.next()?;
            let r = parts.next()?;
            if r == reference {
                return Some(commit.to_string());
            }
        }
        None
    } else {
        Some(head.to_string())
    }
}
