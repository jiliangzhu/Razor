use std::cmp::Ordering;

use serde::Serialize;

use crate::buckets::BucketDecision;
use crate::reasons::ShadowNoteReason;
use crate::types::{Bps, Bucket};

pub const BUCKET_AFTER_DEGRADE: &str = "thin";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProbePhase {
    Gt7d,
    D1ToD7,
    Lt24h,
    Unknown,
}

impl ProbePhase {
    pub fn as_str(self) -> &'static str {
        match self {
            ProbePhase::Gt7d => "GT_7D",
            ProbePhase::D1ToD7 => "D1_TO_D7",
            ProbePhase::Lt24h => "LT_24H",
            ProbePhase::Unknown => "UNKNOWN",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProbeWarning {
    DepthUnitSuspect,
    BurstyPasses,
}

impl ProbeWarning {
    pub fn as_str(self) -> &'static str {
        match self {
            ProbeWarning::DepthUnitSuspect => "DEPTH_UNIT_SUSPECT",
            ProbeWarning::BurstyPasses => "BURSTY_PASSES",
        }
    }
}

#[derive(Clone, Debug)]
pub struct MarketScoreRow {
    pub run_id: String,
    pub probe_start_unix_ms: u64,
    pub probe_end_unix_ms: u64,
    pub probe_seconds: u64,
    pub gamma_id: String,
    pub condition_id: String,
    pub legs_n: usize,
    pub strategy: String,
    pub token0_id: String,
    pub token1_id: String,
    pub token2_id: String,
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

#[derive(Clone, Debug)]
pub struct MarketScoreRowComputed {
    pub row: MarketScoreRow,

    // Extra fields for recommendation.json (not in market_scores.csv).
    pub probe_hour_of_day_utc: u32,
    pub probe_market_phase: ProbePhase,
    pub poll_gap_max_ms: u64,
    pub trade_gap_max_ms: u64,
    pub trade_time_coverage_ok: bool,
    pub estimated_trades_lost: u64,
    pub passes_gap_p50_ms: u64,
    pub passes_gap_p90_ms: u64,
    pub passes_gap_max_ms: u64,
    pub bucket_after_degrade: &'static str,
    pub probe_warnings: Vec<ProbeWarning>,
}

#[derive(Default)]
pub struct SnapshotAccum {
    pub snapshots_total: u64,
    pub one_sided_count: u64,
    pub bucket_nan_count: u64,
    pub depth3_degraded_count: u64,
    pub liquid_count: u64,
    pub thin_count: u64,
    pub worst_spread_bps_samples: Vec<i32>,
    pub worst_depth3_usdc_samples: Vec<f64>,
    pub expected_net_bps_samples: Vec<i32>,
    pub passes_ts_ms: Vec<u64>,
    pub snapshots_eval_total: u64,
    pub passes_min_net_edge_count: u64,
}

impl SnapshotAccum {
    #[allow(clippy::too_many_arguments)]
    pub fn push_snapshot(
        &mut self,
        ts_ms: u64,
        best_bids: &[f64],
        best_asks: &[f64],
        depth3_usdc: &[f64],
        bucket: Bucket,
        bucket_decision: &BucketDecision,
        depth3_degraded: bool,
        expected_net_bps: Option<i32>,
        passes: bool,
    ) {
        self.snapshots_total += 1;

        // one_sided_book_rate: if any leg violates rules.
        let one_sided = best_bids
            .iter()
            .zip(best_asks.iter())
            .any(|(bid, ask)| *bid <= 0.0 || *ask <= 0.0 || *ask >= 1.0);
        if one_sided {
            self.one_sided_count += 1;
        }

        if depth3_degraded || depth3_usdc.iter().any(|d| !d.is_finite()) {
            self.depth3_degraded_count += 1;
        }

        if bucket_decision.reasons.iter().any(|r| {
            matches!(
                r,
                ShadowNoteReason::BucketThinNan | ShadowNoteReason::BucketLiquidNan
            )
        }) {
            self.bucket_nan_count += 1;
        }

        match bucket {
            Bucket::Liquid => self.liquid_count += 1,
            Bucket::Thin => self.thin_count += 1,
        }

        self.worst_spread_bps_samples
            .push(bucket_decision.metrics.worst_spread_bps);
        self.worst_depth3_usdc_samples
            .push(bucket_decision.metrics.worst_depth3_usdc);

        if let Some(net) = expected_net_bps {
            self.snapshots_eval_total += 1;
            self.expected_net_bps_samples.push(net);
            if passes {
                self.passes_min_net_edge_count += 1;
                self.passes_ts_ms.push(ts_ms);
            }
        }
    }
}

#[derive(Default)]
pub struct TradesAccum {
    pub trades_total: u64,
    pub trades_duplicated_count: u64,
    pub trade_poll_hit_limit_count: u64,
    pub poll_ok_ts_ms: Vec<u64>,
    pub trade_ts_ms: Vec<u64>,
}

#[allow(clippy::too_many_arguments)]
pub fn compute_row(
    run_id: &str,
    probe_start_unix_ms: u64,
    probe_end_unix_ms: u64,
    probe_seconds: u64,
    gamma_id: &str,
    condition_id: &str,
    legs_n: usize,
    strategy: &str,
    token_ids: &[String],
    gamma_volume24hr: f64,
    gamma_liquidity: f64,
    phase: ProbePhase,
    snap: SnapshotAccum,
    trades: TradesAccum,
    _min_net_edge_bps: i32,
    trade_poll_limit: usize,
) -> MarketScoreRowComputed {
    let denom = snap.snapshots_total as f64;
    let one_sided_book_rate = if snap.snapshots_total > 0 {
        (snap.one_sided_count as f64) / denom
    } else {
        f64::NAN
    };
    let bucket_nan_rate = if snap.snapshots_total > 0 {
        (snap.bucket_nan_count as f64) / denom
    } else {
        f64::NAN
    };
    let depth3_degraded_rate = if snap.snapshots_total > 0 {
        (snap.depth3_degraded_count as f64) / denom
    } else {
        f64::NAN
    };
    let liquid_bucket_rate = if snap.snapshots_total > 0 {
        (snap.liquid_count as f64) / denom
    } else {
        f64::NAN
    };
    let thin_bucket_rate = if snap.snapshots_total > 0 {
        (snap.thin_count as f64) / denom
    } else {
        f64::NAN
    };

    let worst_spread_bps_p50 =
        quantile_i32(&snap.worst_spread_bps_samples, 0.50).unwrap_or(i32::MAX);
    let worst_depth3_usdc_p50 =
        quantile_f64(&snap.worst_depth3_usdc_samples, 0.50).unwrap_or(f64::NAN);

    let trades_per_min = if probe_seconds > 0 {
        (trades.trades_total as f64) / (probe_seconds as f64) * 60.0
    } else {
        0.0
    };

    let passes_per_hour = if probe_seconds > 0 {
        (snap.passes_min_net_edge_count as f64) / (probe_seconds as f64) * 3600.0
    } else {
        0.0
    };

    let expected_net_bps_p50 =
        quantile_i32(&snap.expected_net_bps_samples, 0.50).unwrap_or(i32::MIN);
    let expected_net_bps_p90 =
        quantile_i32(&snap.expected_net_bps_samples, 0.90).unwrap_or(i32::MIN);
    let expected_net_bps_max = snap
        .expected_net_bps_samples
        .iter()
        .copied()
        .max()
        .unwrap_or(i32::MIN);

    let poll_gap_max_ms = max_gap_ms(&trades.poll_ok_ts_ms);
    let trade_gap_max_ms = max_gap_ms(&trades.trade_ts_ms);
    let trade_time_coverage_ok = trade_gap_max_ms <= 300_000 && trades.trades_total > 0;
    let estimated_trades_lost = trades.trade_poll_hit_limit_count * (trade_poll_limit as u64);

    let passes_gap_max_ms = max_gap_ms(&snap.passes_ts_ms);
    let passes_gap_p50_ms = gap_quantile_ms(&snap.passes_ts_ms, 0.50);
    let passes_gap_p90_ms = gap_quantile_ms(&snap.passes_ts_ms, 0.90);

    let mut warnings: Vec<ProbeWarning> = Vec::new();
    if snap.depth3_degraded_count > 0 {
        warnings.push(ProbeWarning::DepthUnitSuspect);
    }
    if passes_gap_max_ms > 30 * 60 * 1000 {
        warnings.push(ProbeWarning::BurstyPasses);
    }

    let token0 = token_ids.first().cloned().unwrap_or_default();
    let token1 = token_ids.get(1).cloned().unwrap_or_default();
    let token2 = if legs_n >= 3 {
        token_ids.get(2).cloned().unwrap_or_default()
    } else {
        String::new()
    };

    let probe_hour_of_day_utc = ((probe_start_unix_ms / 1000) % 86_400 / 3600) as u32;

    MarketScoreRowComputed {
        row: MarketScoreRow {
            run_id: run_id.to_string(),
            probe_start_unix_ms,
            probe_end_unix_ms,
            probe_seconds,
            gamma_id: gamma_id.to_string(),
            condition_id: condition_id.to_string(),
            legs_n,
            strategy: strategy.to_string(),
            token0_id: token0,
            token1_id: token1,
            token2_id: token2,
            gamma_volume24hr,
            gamma_liquidity,
            snapshots_total: snap.snapshots_total,
            one_sided_book_rate,
            bucket_nan_rate,
            depth3_degraded_rate,
            liquid_bucket_rate,
            thin_bucket_rate,
            worst_spread_bps_p50,
            worst_depth3_usdc_p50,
            trades_total: trades.trades_total,
            trades_per_min,
            trade_poll_hit_limit_count: trades.trade_poll_hit_limit_count,
            trades_duplicated_count: trades.trades_duplicated_count,
            snapshots_eval_total: snap.snapshots_eval_total,
            passes_min_net_edge_count: snap.passes_min_net_edge_count,
            passes_min_net_edge_per_hour: passes_per_hour,
            expected_net_bps_p50,
            expected_net_bps_p90,
            expected_net_bps_max,
        },
        probe_hour_of_day_utc,
        probe_market_phase: phase,
        poll_gap_max_ms,
        trade_gap_max_ms,
        trade_time_coverage_ok,
        estimated_trades_lost,
        passes_gap_p50_ms,
        passes_gap_p90_ms,
        passes_gap_max_ms,
        bucket_after_degrade: BUCKET_AFTER_DEGRADE,
        probe_warnings: warnings,
    }
}

pub fn compute_expected_net_bps(sum_ask: f64, risk_premium_bps: i32) -> Option<i32> {
    if !sum_ask.is_finite() || sum_ask < 0.0 {
        return None;
    }
    let raw_cost_bps = Bps::from_price_cost(sum_ask);
    let raw_edge_bps = Bps::ONE_HUNDRED_PERCENT - raw_cost_bps;
    let hard_fees_bps = Bps::FEE_POLY + Bps::FEE_MERGE;
    let risk = Bps::new(risk_premium_bps);
    let net = raw_edge_bps - hard_fees_bps - risk;
    Some(net.raw())
}

pub fn depth3_is_degraded(depth3_usdc: f64) -> bool {
    !depth3_usdc.is_finite() || depth3_usdc <= 0.0 || depth3_usdc > 10_000_000.0
}

pub fn quantile_i32(values: &[i32], q: f64) -> Option<i32> {
    if values.is_empty() {
        return None;
    }
    let mut v: Vec<i32> = values.to_vec();
    v.sort_unstable();
    let idx = quantile_index(v.len(), q);
    Some(v[idx])
}

pub fn quantile_f64(values: &[f64], q: f64) -> Option<f64> {
    let mut v: Vec<f64> = values.iter().copied().filter(|x| x.is_finite()).collect();
    if v.is_empty() {
        return None;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let idx = quantile_index(v.len(), q);
    Some(v[idx])
}

fn quantile_index(n: usize, q: f64) -> usize {
    if n <= 1 {
        return 0;
    }
    let q = q.clamp(0.0, 1.0);
    let idx = ((n - 1) as f64) * q;
    idx.floor() as usize
}

fn max_gap_ms(ts: &[u64]) -> u64 {
    if ts.len() < 2 {
        return 0;
    }
    let mut max = 0u64;
    for w in ts.windows(2) {
        let d = w[1].saturating_sub(w[0]);
        max = max.max(d);
    }
    max
}

fn gap_quantile_ms(pass_ts_ms: &[u64], q: f64) -> u64 {
    if pass_ts_ms.len() < 2 {
        return 0;
    }
    let mut gaps: Vec<u64> = pass_ts_ms
        .windows(2)
        .map(|w| w[1].saturating_sub(w[0]))
        .collect();
    gaps.sort_unstable();
    let idx = quantile_index(gaps.len(), q);
    gaps[idx]
}

pub fn cmp_f64_desc(a: f64, b: f64) -> Ordering {
    match (a.is_finite(), b.is_finite()) {
        (true, true) => b.partial_cmp(&a).unwrap_or(Ordering::Equal),
        (true, false) => Ordering::Less, // finite wins (desc)
        (false, true) => Ordering::Greater,
        (false, false) => Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantile_index_matches_frozen_spec() {
        let v = vec![10, 20, 30, 40];
        assert_eq!(quantile_i32(&v, 0.50).unwrap(), 20); // idx=floor((4-1)*0.5)=1
        assert_eq!(quantile_i32(&v, 0.90).unwrap(), 30); // idx=floor(2.7)=2
        assert_eq!(quantile_i32(&v, 0.00).unwrap(), 10);
        assert_eq!(quantile_i32(&v, 1.00).unwrap(), 40);
    }

    #[test]
    fn depth3_degraded_threshold_is_locked() {
        assert!(depth3_is_degraded(f64::NAN));
        assert!(depth3_is_degraded(0.0));
        assert!(depth3_is_degraded(10_000_000.01));
        assert!(!depth3_is_degraded(500.0));
    }
}
