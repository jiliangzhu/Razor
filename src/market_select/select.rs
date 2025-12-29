use std::cmp::Ordering;

use anyhow::Context as _;

use crate::market_select::metrics::{cmp_f64_desc, MarketScoreRowComputed};
use crate::market_select::PreferStrategy;

#[derive(Clone, Debug)]
pub struct SelectedTwoMarkets {
    pub liquid: MarketScoreRowComputed,
    pub thin: MarketScoreRowComputed,
}

pub fn select_two_markets(
    rows: &[MarketScoreRowComputed],
    prefer_strategy: PreferStrategy,
) -> anyhow::Result<SelectedTwoMarkets> {
    let eligible: Vec<&MarketScoreRowComputed> =
        rows.iter().filter(|r| passes_hard_gates(r)).collect();
    if eligible.is_empty() {
        anyhow::bail!("no markets pass hard gates");
    }

    let liquid = pick_liquid(&eligible).context("pick liquid")?;
    let thin = pick_thin(&eligible, &liquid.row.gamma_id).context("pick thin")?;

    if prefer_strategy != PreferStrategy::Any && liquid.row.strategy != thin.row.strategy {
        anyhow::bail!(
            "prefer_strategy={} requires same strategy; liquid={} thin={}",
            prefer_strategy.as_str(),
            liquid.row.strategy,
            thin.row.strategy
        );
    }

    Ok(SelectedTwoMarkets {
        liquid: liquid.clone(),
        thin: thin.clone(),
    })
}

fn passes_hard_gates(r: &MarketScoreRowComputed) -> bool {
    let row = &r.row;
    if row.legs_n != 2 && row.legs_n != 3 {
        return false;
    }
    if row.snapshots_total < 300 {
        return false;
    }
    if row.trades_total < 10 {
        return false;
    }
    if !row.bucket_nan_rate.is_finite() || row.bucket_nan_rate > 0.20 {
        return false;
    }
    if row.passes_min_net_edge_count < 1 {
        return false;
    }
    true
}

fn pick_liquid<'a>(
    eligible: &'a [&'a MarketScoreRowComputed],
) -> anyhow::Result<&'a MarketScoreRowComputed> {
    let mut filtered: Vec<&MarketScoreRowComputed> = eligible
        .iter()
        .copied()
        .filter(|r| r.row.liquid_bucket_rate.is_finite() && r.row.liquid_bucket_rate >= 0.50)
        .filter(|r| r.row.one_sided_book_rate.is_finite() && r.row.one_sided_book_rate <= 0.30)
        .collect();

    if filtered.is_empty() {
        anyhow::bail!("no liquid candidates after filters");
    }

    filtered.sort_by(|a, b| liquid_sort_key(a, b));
    Ok(filtered[0])
}

fn pick_thin<'a>(
    eligible: &'a [&'a MarketScoreRowComputed],
    exclude_gamma_id: &str,
) -> anyhow::Result<&'a MarketScoreRowComputed> {
    let mut filtered: Vec<&MarketScoreRowComputed> = eligible
        .iter()
        .copied()
        .filter(|r| r.row.gamma_id != exclude_gamma_id)
        .filter(|r| r.row.thin_bucket_rate.is_finite() && r.row.thin_bucket_rate >= 0.70)
        .filter(|r| r.row.trades_per_min.is_finite() && r.row.trades_per_min >= 0.2)
        .collect();

    if filtered.is_empty() {
        anyhow::bail!("no thin candidates after filters");
    }

    filtered.sort_by(|a, b| thin_sort_key(a, b));
    Ok(filtered[0])
}

fn liquid_sort_key(a: &MarketScoreRowComputed, b: &MarketScoreRowComputed) -> Ordering {
    cmp_f64_desc(
        a.row.passes_min_net_edge_per_hour,
        b.row.passes_min_net_edge_per_hour,
    )
    .then_with(|| cmp_f64_desc(a.row.liquid_bucket_rate, b.row.liquid_bucket_rate))
    .then_with(|| cmp_f64_desc(a.row.trades_per_min, b.row.trades_per_min))
    .then_with(|| cmp_f64_desc(a.row.gamma_volume24hr, b.row.gamma_volume24hr))
    .then_with(|| a.row.gamma_id.cmp(&b.row.gamma_id))
}

fn thin_sort_key(a: &MarketScoreRowComputed, b: &MarketScoreRowComputed) -> Ordering {
    cmp_f64_desc(
        a.row.passes_min_net_edge_per_hour,
        b.row.passes_min_net_edge_per_hour,
    )
    .then_with(|| cmp_f64_desc(a.row.thin_bucket_rate, b.row.thin_bucket_rate))
    .then_with(|| cmp_f64_desc(a.row.trades_per_min, b.row.trades_per_min))
    .then_with(|| cmp_f64_desc(a.row.gamma_volume24hr, b.row.gamma_volume24hr))
    .then_with(|| a.row.gamma_id.cmp(&b.row.gamma_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_select::metrics::{MarketScoreRow, ProbePhase};

    fn mk(
        gamma_id: &str,
        strategy: &str,
        liquid_rate: f64,
        thin_rate: f64,
        passes_per_hour: f64,
    ) -> MarketScoreRowComputed {
        MarketScoreRowComputed {
            row: MarketScoreRow {
                run_id: "r".into(),
                probe_start_unix_ms: 0,
                probe_end_unix_ms: 0,
                probe_seconds: 3600,
                gamma_id: gamma_id.into(),
                condition_id: format!("c{gamma_id}"),
                legs_n: 2,
                strategy: strategy.into(),
                token0_id: "t0".into(),
                token1_id: "t1".into(),
                token2_id: "".into(),
                gamma_volume24hr: 100.0,
                gamma_liquidity: 100.0,
                snapshots_total: 300,
                one_sided_book_rate: 0.0,
                bucket_nan_rate: 0.0,
                depth3_degraded_rate: 0.0,
                liquid_bucket_rate: liquid_rate,
                thin_bucket_rate: thin_rate,
                worst_spread_bps_p50: 10,
                worst_depth3_usdc_p50: 1000.0,
                trades_total: 10,
                trades_per_min: 1.0,
                trade_poll_hit_limit_count: 0,
                trades_duplicated_count: 0,
                snapshots_eval_total: 300,
                passes_min_net_edge_count: 1,
                passes_min_net_edge_per_hour: passes_per_hour,
                expected_net_bps_p50: 10,
                expected_net_bps_p90: 20,
                expected_net_bps_max: 30,
            },
            probe_hour_of_day_utc: 0,
            probe_market_phase: ProbePhase::Unknown,
            poll_gap_max_ms: 0,
            trade_gap_max_ms: 0,
            trade_time_coverage_ok: true,
            estimated_trades_lost: 0,
            passes_gap_p50_ms: 0,
            passes_gap_p90_ms: 0,
            passes_gap_max_ms: 0,
            bucket_after_degrade: "thin",
            probe_warnings: vec![],
        }
    }

    #[test]
    fn selects_one_liquid_and_one_thin() {
        let liquid = mk("1", "binary", 0.9, 0.1, 10.0);
        let thin = mk("2", "binary", 0.1, 0.9, 9.0);
        let rows = vec![liquid.clone(), thin.clone()];
        let sel = select_two_markets(&rows, PreferStrategy::Any).unwrap();
        assert_eq!(sel.liquid.row.gamma_id, "1");
        assert_eq!(sel.thin.row.gamma_id, "2");
    }
}
