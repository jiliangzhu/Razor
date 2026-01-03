use std::path::Path;

use anyhow::Context as _;
use serde::Serialize;

use crate::market_select::metrics::{
    MarketScoreRow, MarketScoreRowComputed, BUCKET_AFTER_DEGRADE, SNAPSHOT_SAMPLE_INTERVAL_MS,
};
use crate::market_select::select::SelectedTwoMarkets;

pub const FILE_MARKET_SCORES: &str = "market_scores.csv";
pub const FILE_RECOMMENDATION_JSON: &str = "recommendation.json";
pub const FILE_SUGGEST_TOML: &str = "suggest.toml";

pub const MARKET_SCORES_HEADER: [&str; 31] = [
    "run_id",
    "probe_start_unix_ms",
    "probe_end_unix_ms",
    "probe_seconds",
    "gamma_id",
    "condition_id",
    "legs_n",
    "strategy",
    "token0_id",
    "token1_id",
    "token2_id",
    "gamma_volume24hr",
    "gamma_liquidity",
    "snapshots_total",
    "one_sided_book_rate",
    "bucket_nan_rate",
    "depth3_degraded_rate",
    "liquid_bucket_rate",
    "thin_bucket_rate",
    "worst_spread_bps_p50",
    "worst_depth3_usdc_p50",
    "trades_total",
    "trades_per_min",
    "trade_poll_hit_limit_count",
    "trades_duplicated_count",
    "snapshots_eval_total",
    "passes_min_net_edge_count",
    "passes_min_net_edge_per_hour",
    "expected_net_bps_p50",
    "expected_net_bps_p90",
    "expected_net_bps_max",
];

pub fn write_market_scores_csv(
    out_dir: &Path,
    rows: &[MarketScoreRowComputed],
) -> anyhow::Result<()> {
    let path = out_dir.join(FILE_MARKET_SCORES);
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(false)
        .from_path(&path)
        .with_context(|| format!("open {}", path.display()))?;

    wtr.write_record(MARKET_SCORES_HEADER)
        .context("write market_scores.csv header")?;

    for r in rows {
        let row = &r.row;
        wtr.write_record(row_to_record(row)).context("write row")?;
    }
    wtr.flush().context("flush market_scores.csv")?;
    Ok(())
}

pub fn write_suggest_toml(
    out_dir: &Path,
    selected: Option<&SelectedTwoMarkets>,
    selection_error: Option<&str>,
) -> anyhow::Result<()> {
    let path = out_dir.join(FILE_SUGGEST_TOML);
    let content = match selected {
        Some(selected) => format!(
            "[run]\nmarket_ids = [\"{}\", \"{}\"]\n",
            selected.liquid.row.gamma_id, selected.thin.row.gamma_id
        ),
        None => {
            let mut s =
                "[run]\nmarket_ids = []\n\n[market_select]\ninsufficient_data = true\n".to_string();
            if let Some(err) = selection_error {
                // Keep this as a single-line TOML string for copy/paste into PR notes.
                let escaped = err.replace('\\', "\\\\").replace('"', "\\\"");
                s.push_str(&format!("reason = \"{escaped}\"\n"));
            }
            s
        }
    };
    std::fs::write(&path, content.as_bytes())
        .with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn write_recommendation_json(
    out_dir: &Path,
    run_id: &str,
    probe_seconds: u64,
    candidates_total: usize,
    probes_completed_ok: usize,
    probes_completed_failed: usize,
    aborted: bool,
    started_at_unix_ms: u64,
    updated_at_unix_ms: u64,
    last_ok_gamma_id: Option<&str>,
    selected: Option<&SelectedTwoMarkets>,
    selection_error: Option<String>,
) -> anyhow::Result<()> {
    let path = out_dir.join(FILE_RECOMMENDATION_JSON);

    let (
        selected_out,
        probe_hour_of_day_utc,
        probe_market_phase,
        poll_gap_max_ms,
        trade_gap_max_ms,
        trade_time_coverage_ok,
        estimated_trades_lost,
        passes_gap_p50_ms,
        passes_gap_p90_ms,
        passes_gap_max_ms,
        probe_warnings,
    ) = match selected {
        Some(selected) => {
            let liquid = SelectedMarketOut::from(&selected.liquid);
            let thin = SelectedMarketOut::from(&selected.thin);

            (
                Some(SelectedOut { liquid, thin }),
                Some(Map2u32 {
                    liquid: selected.liquid.probe_hour_of_day_utc,
                    thin: selected.thin.probe_hour_of_day_utc,
                }),
                Some(Map2 {
                    liquid: selected.liquid.probe_market_phase.as_str().to_string(),
                    thin: selected.thin.probe_market_phase.as_str().to_string(),
                }),
                Some(Map2u64 {
                    liquid: selected.liquid.poll_gap_max_ms,
                    thin: selected.thin.poll_gap_max_ms,
                }),
                Some(Map2u64 {
                    liquid: selected.liquid.trade_gap_max_ms,
                    thin: selected.thin.trade_gap_max_ms,
                }),
                Some(Map2bool {
                    liquid: selected.liquid.trade_time_coverage_ok,
                    thin: selected.thin.trade_time_coverage_ok,
                }),
                Some(Map2u64 {
                    liquid: selected.liquid.estimated_trades_lost,
                    thin: selected.thin.estimated_trades_lost,
                }),
                Some(Map2u64 {
                    liquid: selected.liquid.passes_gap_p50_ms,
                    thin: selected.thin.passes_gap_p50_ms,
                }),
                Some(Map2u64 {
                    liquid: selected.liquid.passes_gap_p90_ms,
                    thin: selected.thin.passes_gap_p90_ms,
                }),
                Some(Map2u64 {
                    liquid: selected.liquid.passes_gap_max_ms,
                    thin: selected.thin.passes_gap_max_ms,
                }),
                Some(Map2vec {
                    liquid: selected
                        .liquid
                        .probe_warnings
                        .iter()
                        .map(|w| w.as_str().to_string())
                        .collect(),
                    thin: selected
                        .thin
                        .probe_warnings
                        .iter()
                        .map(|w| w.as_str().to_string())
                        .collect(),
                }),
            )
        }
        None => (
            None, None, None, None, None, None, None, None, None, None, None,
        ),
    };

    let out = RecommendationOut {
        run_id: run_id.to_string(),
        probe_seconds,
        snapshot_sample_interval_ms: SNAPSHOT_SAMPLE_INTERVAL_MS,
        aborted,
        candidates_total,
        probes_completed_ok,
        probes_completed_failed,
        selection_error,
        progress: ProgressOut {
            started_at_unix_ms,
            updated_at_unix_ms,
            elapsed_seconds: updated_at_unix_ms.saturating_sub(started_at_unix_ms) / 1_000,
            markets_total: candidates_total,
            markets_done: probes_completed_ok.saturating_add(probes_completed_failed),
            markets_failed: probes_completed_failed,
            last_ok_gamma_id: last_ok_gamma_id.map(|s| s.to_string()),
        },
        bucket_after_degrade: BUCKET_AFTER_DEGRADE.to_string(),

        // Include the required fields as top-level maps for quick eyeballing.
        probe_hour_of_day_utc,
        probe_market_phase,
        poll_gap_max_ms,
        trade_gap_max_ms,
        trade_time_coverage_ok,
        estimated_trades_lost,
        passes_gap_p50_ms,
        passes_gap_p90_ms,
        passes_gap_max_ms,
        probe_warnings,

        selected: selected_out,
    };

    let json = serde_json::to_vec_pretty(&out).context("serialize recommendation.json")?;
    std::fs::write(&path, json).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

pub(super) fn row_to_record(row: &MarketScoreRow) -> [String; 31] {
    [
        row.run_id.clone(),
        row.probe_start_unix_ms.to_string(),
        row.probe_end_unix_ms.to_string(),
        row.probe_seconds.to_string(),
        row.gamma_id.clone(),
        row.condition_id.clone(),
        row.legs_n.to_string(),
        row.strategy.clone(),
        row.token0_id.clone(),
        row.token1_id.clone(),
        row.token2_id.clone(),
        fmt_f64(row.gamma_volume24hr),
        fmt_f64(row.gamma_liquidity),
        row.snapshots_total.to_string(),
        fmt_f64(row.one_sided_book_rate),
        fmt_f64(row.bucket_nan_rate),
        fmt_f64(row.depth3_degraded_rate),
        fmt_f64(row.liquid_bucket_rate),
        fmt_f64(row.thin_bucket_rate),
        row.worst_spread_bps_p50.to_string(),
        fmt_f64(row.worst_depth3_usdc_p50),
        row.trades_total.to_string(),
        fmt_f64(row.trades_per_min),
        row.trade_poll_hit_limit_count.to_string(),
        row.trades_duplicated_count.to_string(),
        row.snapshots_eval_total.to_string(),
        row.passes_min_net_edge_count.to_string(),
        fmt_f64(row.passes_min_net_edge_per_hour),
        row.expected_net_bps_p50.to_string(),
        row.expected_net_bps_p90.to_string(),
        row.expected_net_bps_max.to_string(),
    ]
}

fn fmt_f64(v: f64) -> String {
    if !v.is_finite() {
        return "NaN".to_string();
    }
    format!("{v:.6}")
}

#[derive(Debug, Serialize)]
struct RecommendationOut {
    pub run_id: String,
    pub probe_seconds: u64,
    pub snapshot_sample_interval_ms: u64,
    pub aborted: bool,
    pub candidates_total: usize,
    pub probes_completed_ok: usize,
    pub probes_completed_failed: usize,
    pub selection_error: Option<String>,
    pub progress: ProgressOut,
    pub probe_hour_of_day_utc: Option<Map2u32>,
    pub probe_market_phase: Option<Map2>,

    pub poll_gap_max_ms: Option<Map2u64>,
    pub trade_gap_max_ms: Option<Map2u64>,
    pub trade_time_coverage_ok: Option<Map2bool>,
    pub estimated_trades_lost: Option<Map2u64>,
    pub passes_gap_p50_ms: Option<Map2u64>,
    pub passes_gap_p90_ms: Option<Map2u64>,
    pub passes_gap_max_ms: Option<Map2u64>,

    pub bucket_after_degrade: String,
    pub probe_warnings: Option<Map2vec>,

    pub selected: Option<SelectedOut>,
}

#[derive(Debug, Serialize)]
struct ProgressOut {
    pub started_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
    pub elapsed_seconds: u64,
    pub markets_total: usize,
    pub markets_done: usize,
    pub markets_failed: usize,
    pub last_ok_gamma_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct SelectedOut {
    pub liquid: SelectedMarketOut,
    pub thin: SelectedMarketOut,
}

#[derive(Debug, Serialize)]
struct SelectedMarketOut {
    pub gamma_id: String,
    pub condition_id: String,
    pub legs_n: usize,
    pub strategy: String,
    pub token_ids: Vec<String>,

    pub market_scores: MarketScoreRowOut,

    pub probe_market_phase: String,
    pub poll_gap_max_ms: u64,
    pub trade_gap_max_ms: u64,
    pub trade_time_coverage_ok: bool,
    pub estimated_trades_lost: u64,
    pub passes_gap_p50_ms: u64,
    pub passes_gap_p90_ms: u64,
    pub passes_gap_max_ms: u64,
    pub probe_warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
struct MarketScoreRowOut {
    pub gamma_volume24hr: f64,
    pub gamma_liquidity: f64,
    pub snapshots_total: u64,
    pub one_sided_book_rate: f64,
    pub bucket_nan_rate: f64,
    pub depth3_degraded_rate: f64,
    pub liquid_bucket_rate: f64,
    pub thin_bucket_rate: f64,
    pub worst_spread_bps_p50: i32,
    pub worst_depth3_usdc_p50: f64,
    pub trades_total: u64,
    pub trades_per_min: f64,
    pub trade_poll_hit_limit_count: u64,
    pub trades_duplicated_count: u64,
    pub snapshots_eval_total: u64,
    pub passes_min_net_edge_count: u64,
    pub passes_min_net_edge_per_hour: f64,
    pub expected_net_bps_p50: i32,
    pub expected_net_bps_p90: i32,
    pub expected_net_bps_max: i32,
}

impl From<&MarketScoreRowComputed> for SelectedMarketOut {
    fn from(v: &MarketScoreRowComputed) -> Self {
        let row = &v.row;
        let mut token_ids = vec![row.token0_id.clone(), row.token1_id.clone()];
        if row.legs_n == 3 && !row.token2_id.is_empty() {
            token_ids.push(row.token2_id.clone());
        }

        SelectedMarketOut {
            gamma_id: row.gamma_id.clone(),
            condition_id: row.condition_id.clone(),
            legs_n: row.legs_n,
            strategy: row.strategy.clone(),
            token_ids,
            market_scores: MarketScoreRowOut {
                gamma_volume24hr: row.gamma_volume24hr,
                gamma_liquidity: row.gamma_liquidity,
                snapshots_total: row.snapshots_total,
                one_sided_book_rate: row.one_sided_book_rate,
                bucket_nan_rate: row.bucket_nan_rate,
                depth3_degraded_rate: row.depth3_degraded_rate,
                liquid_bucket_rate: row.liquid_bucket_rate,
                thin_bucket_rate: row.thin_bucket_rate,
                worst_spread_bps_p50: row.worst_spread_bps_p50,
                worst_depth3_usdc_p50: row.worst_depth3_usdc_p50,
                trades_total: row.trades_total,
                trades_per_min: row.trades_per_min,
                trade_poll_hit_limit_count: row.trade_poll_hit_limit_count,
                trades_duplicated_count: row.trades_duplicated_count,
                snapshots_eval_total: row.snapshots_eval_total,
                passes_min_net_edge_count: row.passes_min_net_edge_count,
                passes_min_net_edge_per_hour: row.passes_min_net_edge_per_hour,
                expected_net_bps_p50: row.expected_net_bps_p50,
                expected_net_bps_p90: row.expected_net_bps_p90,
                expected_net_bps_max: row.expected_net_bps_max,
            },
            probe_market_phase: v.probe_market_phase.as_str().to_string(),
            poll_gap_max_ms: v.poll_gap_max_ms,
            trade_gap_max_ms: v.trade_gap_max_ms,
            trade_time_coverage_ok: v.trade_time_coverage_ok,
            estimated_trades_lost: v.estimated_trades_lost,
            passes_gap_p50_ms: v.passes_gap_p50_ms,
            passes_gap_p90_ms: v.passes_gap_p90_ms,
            passes_gap_max_ms: v.passes_gap_max_ms,
            probe_warnings: v
                .probe_warnings
                .iter()
                .map(|w| w.as_str().to_string())
                .collect(),
        }
    }
}

#[derive(Debug, Serialize)]
struct Map2 {
    pub liquid: String,
    pub thin: String,
}

#[derive(Debug, Serialize)]
struct Map2u64 {
    pub liquid: u64,
    pub thin: u64,
}

#[derive(Debug, Serialize)]
struct Map2u32 {
    pub liquid: u32,
    pub thin: u32,
}

#[derive(Debug, Serialize)]
struct Map2bool {
    pub liquid: bool,
    pub thin: bool,
}

#[derive(Debug, Serialize)]
struct Map2vec {
    pub liquid: Vec<String>,
    pub thin: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_scores_header_is_frozen() {
        let header = MARKET_SCORES_HEADER.join(",");
        assert_eq!(header, "run_id,probe_start_unix_ms,probe_end_unix_ms,probe_seconds,gamma_id,condition_id,legs_n,strategy,token0_id,token1_id,token2_id,gamma_volume24hr,gamma_liquidity,snapshots_total,one_sided_book_rate,bucket_nan_rate,depth3_degraded_rate,liquid_bucket_rate,thin_bucket_rate,worst_spread_bps_p50,worst_depth3_usdc_p50,trades_total,trades_per_min,trade_poll_hit_limit_count,trades_duplicated_count,snapshots_eval_total,passes_min_net_edge_count,passes_min_net_edge_per_hour,expected_net_bps_p50,expected_net_bps_p90,expected_net_bps_max");
    }

    #[test]
    fn suggest_toml_marks_insufficient_data() {
        let dir = std::env::temp_dir().join(format!(
            "razor_market_select_test_{}_{}",
            std::process::id(),
            crate::types::now_ms()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");

        write_suggest_toml(&dir, None, Some("no markets pass hard gates"))
            .expect("write suggest.toml");

        let content = std::fs::read_to_string(dir.join(FILE_SUGGEST_TOML)).expect("read suggest");
        assert!(content.contains("insufficient_data = true"));
        assert!(content.contains("no markets pass hard gates"));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
