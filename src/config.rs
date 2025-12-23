use std::path::{Path, PathBuf};

use anyhow::Context as _;
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub polymarket: PolymarketConfig,
    pub run: RunConfig,
    #[serde(default = "default_schema_version")]
    pub schema_version: String,
    #[serde(default)]
    pub brain: BrainConfig,
    #[serde(default)]
    pub buckets: BucketConfig,
    #[serde(default)]
    pub shadow: ShadowConfig,
    #[serde(default)]
    pub report: ReportConfig,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let raw =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let cfg: Config =
            toml::from_str(&raw).with_context(|| format!("parse {}", path.display()))?;
        Ok(cfg)
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct PolymarketConfig {
    #[serde(default = "default_gamma_base")]
    pub gamma_base: String,
    #[serde(default = "default_ws_base")]
    pub ws_base: String,
    #[serde(default = "default_data_api_base")]
    pub data_api_base: String,
}

impl Default for PolymarketConfig {
    fn default() -> Self {
        Self {
            gamma_base: default_gamma_base(),
            ws_base: default_ws_base(),
            data_api_base: default_data_api_base(),
        }
    }
}

fn default_gamma_base() -> String {
    "https://gamma-api.polymarket.com".to_string()
}

fn default_ws_base() -> String {
    "wss://ws-subscriptions-clob.polymarket.com".to_string()
}

fn default_data_api_base() -> String {
    "https://data-api.polymarket.com".to_string()
}

#[derive(Clone, Debug, Deserialize)]
pub struct RunConfig {
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    pub market_ids: Vec<String>,
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("data")
}

fn default_schema_version() -> String {
    crate::schema::SCHEMA_VERSION.to_string()
}

#[derive(Clone, Debug, Deserialize)]
pub struct BrainConfig {
    #[serde(default = "default_risk_premium_bps")]
    pub risk_premium_bps: i32,
    #[serde(default = "default_min_net_edge_bps")]
    pub min_net_edge_bps: i32,
    #[serde(default = "default_q_req")]
    pub q_req: f64,
    #[serde(default = "default_signal_cooldown_ms")]
    pub signal_cooldown_ms: u64,
}

impl Default for BrainConfig {
    fn default() -> Self {
        Self {
            risk_premium_bps: default_risk_premium_bps(),
            min_net_edge_bps: default_min_net_edge_bps(),
            q_req: default_q_req(),
            signal_cooldown_ms: default_signal_cooldown_ms(),
        }
    }
}

fn default_risk_premium_bps() -> i32 {
    80
}

fn default_min_net_edge_bps() -> i32 {
    10
}

fn default_q_req() -> f64 {
    10.0
}

fn default_signal_cooldown_ms() -> u64 {
    1000
}

#[derive(Clone, Debug, Deserialize)]
pub struct BucketConfig {
    #[serde(default = "default_fill_share_liquid_p25")]
    pub fill_share_liquid_p25: f64,
    #[serde(default = "default_fill_share_thin_p25")]
    pub fill_share_thin_p25: f64,
}

impl Default for BucketConfig {
    fn default() -> Self {
        Self {
            fill_share_liquid_p25: default_fill_share_liquid_p25(),
            fill_share_thin_p25: default_fill_share_thin_p25(),
        }
    }
}

fn default_fill_share_liquid_p25() -> f64 {
    0.30
}

fn default_fill_share_thin_p25() -> f64 {
    0.10
}

#[derive(Clone, Debug, Deserialize)]
pub struct ShadowConfig {
    #[serde(default = "default_window_start_ms")]
    pub window_start_ms: u64,
    #[serde(default = "default_window_end_ms")]
    pub window_end_ms: u64,
    #[serde(default = "default_trade_poll_interval_ms")]
    pub trade_poll_interval_ms: u64,
    #[serde(default = "default_trade_poll_limit")]
    pub trade_poll_limit: usize,
    #[serde(default = "default_trade_retention_ms")]
    pub trade_retention_ms: u64,
}

impl Default for ShadowConfig {
    fn default() -> Self {
        Self {
            window_start_ms: default_window_start_ms(),
            window_end_ms: default_window_end_ms(),
            trade_poll_interval_ms: default_trade_poll_interval_ms(),
            trade_poll_limit: default_trade_poll_limit(),
            trade_retention_ms: default_trade_retention_ms(),
        }
    }
}

fn default_window_start_ms() -> u64 {
    100
}

fn default_window_end_ms() -> u64 {
    1100
}

fn default_trade_poll_interval_ms() -> u64 {
    1000
}

fn default_trade_poll_limit() -> usize {
    500
}

fn default_trade_retention_ms() -> u64 {
    5000
}

#[derive(Clone, Debug, Deserialize)]
pub struct ReportConfig {
    #[serde(default = "default_report_min_total_shadow_pnl")]
    pub min_total_shadow_pnl: f64,
    #[serde(default = "default_report_min_avg_set_ratio")]
    pub min_avg_set_ratio: f64,
}

impl Default for ReportConfig {
    fn default() -> Self {
        Self {
            min_total_shadow_pnl: default_report_min_total_shadow_pnl(),
            min_avg_set_ratio: default_report_min_avg_set_ratio(),
        }
    }
}

fn default_report_min_total_shadow_pnl() -> f64 {
    0.0
}

fn default_report_min_avg_set_ratio() -> f64 {
    0.85
}
