use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Context as _;
use serde::Serialize;

pub const SCHEMA_VERSION: &str = "1.3.2a";

pub const FILE_TICKS: &str = "ticks.csv";
pub const FILE_TRADES: &str = "trades.csv";
pub const FILE_SHADOW_LOG: &str = "shadow_log.csv";
pub const FILE_REPORT_JSON: &str = "report.json";
pub const FILE_REPORT_MD: &str = "report.md";
pub const FILE_SCHEMA_VERSION: &str = "schema_version.json";
pub const FILE_RUN_CONFIG: &str = "config.toml";
pub const FILE_META_JSON: &str = "meta.json";
pub const FILE_RUN_META_JSON: &str = "run_meta.json";
pub const FILE_HEALTH_JSONL: &str = "health.jsonl";
pub const FILE_RAW_WS_JSONL: &str = "raw_ws.jsonl";
pub const FILE_TRADE_LOG: &str = "trade_log.csv";
pub const FILE_CALIBRATION_LOG: &str = "calibration_log.csv";
pub const FILE_CALIBRATION_SUGGEST: &str = "calibration_suggest.toml";

pub const DUMP_SLIPPAGE_ASSUMED: f64 = 0.05;

pub const TRADES_HEADER: [&str; 8] = [
    "ts_ms",
    "market_id",
    "token_id",
    "price",
    "size",
    "trade_id",
    "ingest_ts_ms",
    "exchange_ts_ms",
];

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

#[allow(dead_code)]
pub const TRADE_LOG_HEADER: [&str; 16] = [
    "ts_ms",
    "signal_id",
    "market_id",
    "strategy",
    "bucket",
    "phase",
    "action",
    "leg_index",
    "token_id",
    "side",
    "limit_price",
    "req_qty",
    "fill_qty",
    "fill_status",
    "expected_net_bps",
    "notes",
];

#[allow(dead_code)]
pub const CALIBRATION_LOG_HEADER: [&str; 11] = [
    "ts_ms",
    "bucket",
    "market_id",
    "token_id",
    "side",
    "req_qty",
    "filled_qty",
    "market_ask_size_best",
    "market_bid_size_best",
    "sim_fill_share_used",
    "mode",
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
    files.insert(FILE_SCHEMA_VERSION.to_string(), "v1".to_string());
    files.insert(FILE_RUN_CONFIG.to_string(), "v1".to_string());
    files.insert(FILE_META_JSON.to_string(), "v1".to_string());
    files.insert(FILE_RUN_META_JSON.to_string(), "v1".to_string());
    files.insert(FILE_HEALTH_JSONL.to_string(), "v1".to_string());
    files.insert(FILE_RAW_WS_JSONL.to_string(), "v1".to_string());
    files.insert(FILE_TICKS.to_string(), "v1".to_string());
    files.insert(FILE_TRADES.to_string(), "v3".to_string());
    files.insert(FILE_SHADOW_LOG.to_string(), "v5".to_string());
    files.insert(FILE_REPORT_JSON.to_string(), "v1".to_string());
    files.insert(FILE_REPORT_MD.to_string(), "v1".to_string());
    files.insert(FILE_TRADE_LOG.to_string(), "v1".to_string());
    files.insert(FILE_CALIBRATION_LOG.to_string(), "v1".to_string());
    files.insert(FILE_CALIBRATION_SUGGEST.to_string(), "v1".to_string());

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
