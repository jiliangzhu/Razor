use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Context as _;
use serde::Serialize;

pub const SCHEMA_VERSION: &str = "v1.3.2a-prA";

pub const FILE_TICKS: &str = "ticks.csv";
pub const FILE_TRADES: &str = "trades.csv";
pub const FILE_SHADOW_LOG: &str = "shadow_log.csv";
pub const FILE_REPORT_JSON: &str = "report.json";
pub const FILE_REPORT_MD: &str = "report.md";
pub const FILE_SCHEMA_VERSION: &str = "schema_version.json";

pub const DUMP_SLIPPAGE_ASSUMED: f64 = 0.05;

pub const SHADOW_HEADER: [&str; 38] = [
    "run_id",
    "schema_version",
    "signal_id",
    "signal_ts_unix_ms",
    "window_start_ms",
    "window_end_ms",
    "market_id",
    "strategy",
    "bucket",
    "worst_leg_token_id",
    "q_req",
    "legs_n",
    "q_set",
    "leg0_token_id",
    "leg0_p_limit",
    "leg0_best_bid",
    "leg0_v_mkt",
    "leg0_q_fill",
    "leg1_token_id",
    "leg1_p_limit",
    "leg1_best_bid",
    "leg1_v_mkt",
    "leg1_q_fill",
    "leg2_token_id",
    "leg2_p_limit",
    "leg2_best_bid",
    "leg2_v_mkt",
    "leg2_q_fill",
    "cost_set",
    "proceeds_set",
    "pnl_set",
    "pnl_left_total",
    "total_pnl",
    "q_fill_avg",
    "set_ratio",
    "fill_share_p25_used",
    "dump_slippage_assumed",
    "notes",
];

#[derive(Debug, Serialize)]
struct SchemaVersionFile {
    schema_version: String,
    generated_at_unix_ms: u64,
    files: BTreeMap<String, String>,
}

pub fn write_schema_version_json(
    data_dir: &Path,
    schema_version: &str,
    generated_at_unix_ms: u64,
) -> anyhow::Result<()> {
    let mut files = BTreeMap::new();
    files.insert(FILE_TICKS.to_string(), "v1".to_string());
    files.insert(FILE_TRADES.to_string(), "v1".to_string());
    files.insert(FILE_SHADOW_LOG.to_string(), "v2".to_string());
    files.insert(FILE_REPORT_JSON.to_string(), "v1".to_string());
    files.insert(FILE_REPORT_MD.to_string(), "v1".to_string());

    let payload = SchemaVersionFile {
        schema_version: schema_version.to_string(),
        generated_at_unix_ms,
        files,
    };

    let out_path = data_dir.join(FILE_SCHEMA_VERSION);
    let json = serde_json::to_vec_pretty(&payload).context("serialize schema_version.json")?;
    std::fs::write(&out_path, json).with_context(|| format!("write {}", out_path.display()))?;
    Ok(())
}

pub fn make_run_id(start_unix_ms: u64) -> String {
    format!("run_{start_unix_ms}")
}
