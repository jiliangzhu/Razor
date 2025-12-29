use serde::Deserialize;
use std::path::PathBuf;

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
    #[allow(dead_code)]
    #[serde(default)]
    pub market_select: MarketSelectConfig,
    #[serde(default)]
    pub report: ReportConfig,
    #[allow(dead_code)]
    #[serde(default)]
    pub live: LiveConfig,
    #[allow(dead_code)]
    #[serde(default)]
    pub calibration: CalibrationConfig,
    #[allow(dead_code)]
    #[serde(default)]
    pub sim: SimConfig,
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
    #[allow(dead_code)]
    #[serde(default = "default_max_snapshot_staleness_ms")]
    pub max_snapshot_staleness_ms: u64,
}

impl Default for BrainConfig {
    fn default() -> Self {
        Self {
            risk_premium_bps: default_risk_premium_bps(),
            min_net_edge_bps: default_min_net_edge_bps(),
            q_req: default_q_req(),
            signal_cooldown_ms: default_signal_cooldown_ms(),
            max_snapshot_staleness_ms: default_max_snapshot_staleness_ms(),
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

fn default_max_snapshot_staleness_ms() -> u64 {
    500
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
    #[serde(default = "default_shadow_max_trades")]
    pub max_trades: usize,
    #[allow(dead_code)]
    #[serde(default = "default_shadow_max_trade_gap_ms")]
    pub max_trade_gap_ms: u64,
}

impl Default for ShadowConfig {
    fn default() -> Self {
        Self {
            window_start_ms: default_window_start_ms(),
            window_end_ms: default_window_end_ms(),
            trade_poll_interval_ms: default_trade_poll_interval_ms(),
            trade_poll_limit: default_trade_poll_limit(),
            trade_retention_ms: default_trade_retention_ms(),
            max_trades: default_shadow_max_trades(),
            max_trade_gap_ms: default_shadow_max_trade_gap_ms(),
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

fn default_shadow_max_trades() -> usize {
    200_000
}

fn default_shadow_max_trade_gap_ms() -> u64 {
    700
}

#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize)]
pub struct MarketSelectConfig {
    #[serde(default = "default_market_select_probe_seconds")]
    pub probe_seconds: u64,
    #[serde(default = "default_market_select_pool_limit")]
    pub pool_limit: usize,
    #[serde(default = "default_market_select_prefer_strategy")]
    pub prefer_strategy: String,
    #[serde(default = "default_market_select_max_concurrency")]
    pub max_concurrency: usize,
}

impl Default for MarketSelectConfig {
    fn default() -> Self {
        Self {
            probe_seconds: default_market_select_probe_seconds(),
            pool_limit: default_market_select_pool_limit(),
            prefer_strategy: default_market_select_prefer_strategy(),
            max_concurrency: default_market_select_max_concurrency(),
        }
    }
}

fn default_market_select_probe_seconds() -> u64 {
    3600
}

fn default_market_select_pool_limit() -> usize {
    200
}

fn default_market_select_prefer_strategy() -> String {
    "any".to_string()
}

fn default_market_select_max_concurrency() -> usize {
    5
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

#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize)]
pub struct LiveConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_live_chase_cap_bps")]
    pub chase_cap_bps: i32,
    #[serde(default = "default_live_ladder_step1_bps")]
    pub ladder_step1_bps: i32,
    #[serde(default = "default_live_flatten_lvl1_bps")]
    pub flatten_lvl1_bps: i32,
    #[serde(default = "default_live_flatten_lvl2_bps")]
    pub flatten_lvl2_bps: i32,
    #[serde(default = "default_live_flatten_lvl3_bps")]
    pub flatten_lvl3_bps: i32,
    #[serde(default = "default_live_flatten_max_attempts")]
    pub flatten_max_attempts: u8,
    #[serde(default = "default_live_cooldown_ms")]
    pub cooldown_ms: u64,
}

impl Default for LiveConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            chase_cap_bps: default_live_chase_cap_bps(),
            ladder_step1_bps: default_live_ladder_step1_bps(),
            flatten_lvl1_bps: default_live_flatten_lvl1_bps(),
            flatten_lvl2_bps: default_live_flatten_lvl2_bps(),
            flatten_lvl3_bps: default_live_flatten_lvl3_bps(),
            flatten_max_attempts: default_live_flatten_max_attempts(),
            cooldown_ms: default_live_cooldown_ms(),
        }
    }
}

fn default_live_chase_cap_bps() -> i32 {
    200
}

fn default_live_ladder_step1_bps() -> i32 {
    10
}

fn default_live_flatten_lvl1_bps() -> i32 {
    100
}

fn default_live_flatten_lvl2_bps() -> i32 {
    500
}

fn default_live_flatten_lvl3_bps() -> i32 {
    1000
}

fn default_live_flatten_max_attempts() -> u8 {
    3
}

fn default_live_cooldown_ms() -> u64 {
    1000
}

#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize)]
pub struct CalibrationConfig {
    #[serde(default = "default_calibration_min_samples_per_bucket")]
    pub min_samples_per_bucket: usize,
    #[serde(default = "default_calibration_suggest_filename")]
    pub suggest_filename: String,
    #[serde(default = "default_calibration_quantile")]
    pub quantile: f64,
}

impl Default for CalibrationConfig {
    fn default() -> Self {
        Self {
            min_samples_per_bucket: default_calibration_min_samples_per_bucket(),
            suggest_filename: default_calibration_suggest_filename(),
            quantile: default_calibration_quantile(),
        }
    }
}

fn default_calibration_min_samples_per_bucket() -> usize {
    30
}

fn default_calibration_suggest_filename() -> String {
    crate::schema::FILE_CALIBRATION_SUGGEST.to_string()
}

fn default_calibration_quantile() -> f64 {
    0.25
}

#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize)]
pub struct SimConfig {
    #[serde(default = "default_sim_fill_share_liquid")]
    pub sim_fill_share_liquid: f64,
    #[serde(default = "default_sim_fill_share_thin")]
    pub sim_fill_share_thin: f64,
    #[serde(default = "default_sim_network_latency_ms")]
    pub sim_network_latency_ms: u64,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            sim_fill_share_liquid: default_sim_fill_share_liquid(),
            sim_fill_share_thin: default_sim_fill_share_thin(),
            sim_network_latency_ms: default_sim_network_latency_ms(),
        }
    }
}

fn default_sim_fill_share_liquid() -> f64 {
    0.30
}

fn default_sim_fill_share_thin() -> f64 {
    0.10
}

fn default_sim_network_latency_ms() -> u64 {
    120
}
