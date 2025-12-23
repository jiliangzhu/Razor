use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context as _;
use tokio::sync::mpsc;
use tracing::{debug, info};

use crate::buckets::fill_share_p25;
use crate::config::Config;
use crate::recorder::{CsvAppender, SHADOW_HEADER};
use crate::schema::DUMP_SLIPPAGE_ASSUMED;
use crate::trade_store::TradeStore;
use crate::types::{now_ms, Bps, MarketDef, Signal, TradeTick};

const LEFTOVER_DUMP_MULT: f64 = 1.0 - DUMP_SLIPPAGE_ASSUMED;

pub async fn run(
    cfg: Config,
    _markets: Vec<MarketDef>,
    mut trade_rx: mpsc::Receiver<TradeTick>,
    mut signal_rx: mpsc::Receiver<Signal>,
    shadow_path: PathBuf,
    run_id: String,
) -> anyhow::Result<()> {
    let mut out = CsvAppender::open(shadow_path, &SHADOW_HEADER).context("open shadow_log.csv")?;

    let window_start_ms = cfg.shadow.window_start_ms;
    let window_end_ms = cfg.shadow.window_end_ms;

    let mut store = TradeStore::new(cfg.shadow.trade_retention_ms);
    let mut pending: Vec<Signal> = Vec::new();

    let mut tick = tokio::time::interval(Duration::from_millis(50));

    loop {
        tokio::select! {
            maybe = trade_rx.recv() => {
                let Some(t) = maybe else {
                    return Err(anyhow::anyhow!("trade channel closed"));
                };
                store.push(t);
            }
            maybe = signal_rx.recv() => {
                let Some(s) = maybe else {
                    return Err(anyhow::anyhow!("signal channel closed"));
                };
                pending.push(s);
            }
            _ = tick.tick() => {
                let now = now_ms();
                if pending.is_empty() {
                    continue;
                }

                let mut still_pending = Vec::with_capacity(pending.len());
                for s in pending.drain(..) {
                    if now < s.ts_ms + window_end_ms {
                        still_pending.push(s);
                        continue;
                    }
                    settle_one(
                        &cfg,
                        &mut out,
                        &store,
                        &s,
                        &run_id,
                        window_start_ms,
                        window_end_ms,
                    )?;
                }
                pending = still_pending;
            }
        }
    }
}

fn settle_one(
    cfg: &Config,
    out: &mut CsvAppender,
    store: &TradeStore,
    s: &Signal,
    run_id: &str,
    window_start_ms: u64,
    window_end_ms: u64,
) -> anyhow::Result<()> {
    let start_ms = s.ts_ms + window_start_ms;
    let end_ms = s.ts_ms + window_end_ms;

    let fill_share_used = fill_share_p25(s.bucket, &cfg.buckets);

    let legs_n = s.legs.len();
    let mut v_mkt: Vec<f64> = Vec::with_capacity(legs_n);
    let mut q_fill: Vec<f64> = Vec::with_capacity(legs_n);

    for leg in &s.legs {
        let v = store.volume_at_or_better_price(
            &s.market_id,
            &leg.token_id,
            start_ms,
            end_ms,
            leg.p_limit,
        );
        v_mkt.push(v);

        let v_my = v * fill_share_used;
        q_fill.push(s.q_req.min(v_my));
    }

    let q_set = q_fill
        .iter()
        .copied()
        .fold(f64::INFINITY, |a, b| a.min(b))
        .min(s.q_req);

    let q_fill_sum: f64 = q_fill.iter().copied().sum();
    let q_fill_avg = if legs_n > 0 {
        q_fill_sum / (legs_n as f64)
    } else {
        0.0
    };

    let mut q_left: Vec<f64> = q_fill.iter().map(|q| q - q_set).collect();
    while q_left.len() < 3 {
        q_left.push(0.0);
    }

    let mut legs = s.legs.clone();
    while legs.len() < 3 {
        legs.push(crate::types::SignalLeg {
            token_id: "".to_string(),
            p_limit: 0.0,
            best_bid_at_t0: 0.0,
        });
        v_mkt.push(0.0);
        q_fill.push(0.0);
    }

    let cost_per_set: f64 = legs
        .iter()
        .take(3)
        .map(|l| Bps::FEE_POLY.apply_cost(l.p_limit))
        .sum();
    let proceeds_per_set = Bps::FEE_MERGE.apply_proceeds(1.0);

    let cost_set = q_set * cost_per_set;
    let proceeds_set = q_set * proceeds_per_set;
    let pnl_set = proceeds_set - cost_set;

    let mut pnl_left_total = 0.0f64;
    for (i, l) in legs.iter().take(3).enumerate() {
        let exit = l.best_bid_at_t0 * LEFTOVER_DUMP_MULT;
        let cost = q_left[i] * Bps::FEE_POLY.apply_cost(l.p_limit);
        let proceeds = q_left[i] * Bps::FEE_POLY.apply_proceeds(exit);
        pnl_left_total += proceeds - cost;
    }

    let pnl_total = pnl_set + pnl_left_total;
    let set_ratio = if q_fill_avg > 0.0 {
        q_set / q_fill_avg
    } else {
        0.0
    };
    let notes = if q_fill_avg <= 0.0 {
        "no_volume"
    } else if set_ratio < 0.85 {
        "partial_legging"
    } else {
        ""
    };

    out.write_record([
        run_id.to_string(),
        cfg.schema_version.clone(),
        s.signal_id.to_string(),
        s.ts_ms.to_string(),
        window_start_ms.to_string(),
        window_end_ms.to_string(),
        s.market_id.clone(),
        s.strategy.as_str().to_string(),
        match s.bucket {
            crate::types::Bucket::Liquid => "liquid".to_string(),
            crate::types::Bucket::Thin => "thin".to_string(),
        },
        s.worst_leg_token_id.clone(),
        s.q_req.to_string(),
        (legs_n as u8).to_string(),
        q_set.to_string(),
        legs[0].token_id.clone(),
        legs[0].p_limit.to_string(),
        legs[0].best_bid_at_t0.to_string(),
        v_mkt[0].to_string(),
        q_fill[0].to_string(),
        legs[1].token_id.clone(),
        legs[1].p_limit.to_string(),
        legs[1].best_bid_at_t0.to_string(),
        v_mkt[1].to_string(),
        q_fill[1].to_string(),
        legs[2].token_id.clone(),
        legs[2].p_limit.to_string(),
        legs[2].best_bid_at_t0.to_string(),
        v_mkt[2].to_string(),
        q_fill[2].to_string(),
        cost_set.to_string(),
        proceeds_set.to_string(),
        pnl_set.to_string(),
        pnl_left_total.to_string(),
        pnl_total.to_string(),
        q_fill_avg.to_string(),
        set_ratio.to_string(),
        fill_share_used.to_string(),
        DUMP_SLIPPAGE_ASSUMED.to_string(),
        notes.to_string(),
    ])?;

    debug!(
        signal_id = s.signal_id,
        ts_signal_us = s.ts_signal_us,
        q_set,
        pnl_total,
        "shadow settle"
    );

    if s.signal_id % 100 == 0 {
        info!(signal_id = s.signal_id, "shadow checkpoint");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        BrainConfig, BucketConfig, Config, PolymarketConfig, ReportConfig, RunConfig, ShadowConfig,
    };
    use crate::recorder::CsvAppender;
    use crate::types::{Bps, Bucket, BucketMode, SignalLeg, Strategy};
    use assert_approx_eq::assert_approx_eq;

    #[test]
    fn settles_binary_signal_with_leftover_penalty() {
        let base_ms = now_ms();
        let cfg = Config {
            polymarket: PolymarketConfig::default(),
            run: RunConfig {
                data_dir: "data".into(),
                market_ids: vec![],
            },
            schema_version: crate::schema::SCHEMA_VERSION.to_string(),
            brain: BrainConfig {
                risk_premium_bps: 80,
                min_net_edge_bps: 10,
                q_req: 10.0,
                signal_cooldown_ms: 0,
            },
            buckets: BucketConfig {
                fill_share_liquid_p25: 0.5,
                fill_share_thin_p25: 0.1,
            },
            shadow: ShadowConfig::default(),
            report: ReportConfig::default(),
        };

        let tmp =
            std::env::temp_dir().join(format!("razor_shadow_test_{}.csv", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let mut out = CsvAppender::open(&tmp, &SHADOW_HEADER).expect("open csv");

        let s = Signal {
            signal_id: 1,
            ts_signal_us: 1_000_000,
            ts_ms: base_ms,
            ts_snapshot_us: 0,
            market_id: "mkt".to_string(),
            strategy: Strategy::Binary,
            bucket: Bucket::Liquid,
            bucket_mode: BucketMode::FullL2,
            worst_leg_token_id: "A".to_string(),
            q_req: 10.0,
            raw_cost_bps: Bps::from_price_cost(0.97),
            raw_edge_bps: Bps::new(300),
            hard_fees_bps: Bps::FEE_POLY + Bps::FEE_MERGE,
            risk_premium_bps: Bps::new(80),
            expected_net_bps: Bps::new(10),
            legs: vec![
                SignalLeg {
                    token_id: "A".to_string(),
                    p_limit: 0.49,
                    best_bid_at_t0: 0.48,
                },
                SignalLeg {
                    token_id: "B".to_string(),
                    p_limit: 0.48,
                    best_bid_at_t0: 0.47,
                },
            ],
        };

        let mut store = TradeStore::new(60_000);
        store.push(TradeTick {
            ts_ms: base_ms + 200,
            market_id: "mkt".to_string(),
            token_id: "A".to_string(),
            price: 0.48,
            size: 30.0,
        });
        store.push(TradeTick {
            ts_ms: base_ms + 200,
            market_id: "mkt".to_string(),
            token_id: "B".to_string(),
            price: 0.48,
            size: 12.0,
        });

        settle_one(&cfg, &mut out, &store, &s, "run_test", 100, 1_100).expect("settle");

        let text = std::fs::read_to_string(&tmp).expect("read csv");
        let mut lines = text.lines();
        let header = lines.next().expect("header");
        let row = lines.next().expect("row");
        let names: Vec<&str> = header.split(',').collect();
        let cols: Vec<&str> = row.split(',').collect();
        assert_eq!(cols.len(), SHADOW_HEADER.len());

        let idx = |name: &str| -> usize {
            names
                .iter()
                .position(|n| n.eq_ignore_ascii_case(name))
                .unwrap_or_else(|| panic!("missing column {name}"))
        };

        assert_eq!(cols[idx("run_id")], "run_test");
        assert_eq!(cols[idx("schema_version")], crate::schema::SCHEMA_VERSION);

        let q_set: f64 = cols[idx("q_set")].parse().expect("q_set");
        let q_fill_avg: f64 = cols[idx("q_fill_avg")].parse().expect("q_fill_avg");
        let set_ratio: f64 = cols[idx("set_ratio")].parse().expect("set_ratio");
        let pnl_set: f64 = cols[idx("pnl_set")].parse().expect("pnl_set");
        let pnl_left: f64 = cols[idx("pnl_left_total")].parse().expect("pnl_left_total");
        let pnl_total: f64 = cols[idx("total_pnl")].parse().expect("total_pnl");

        // q_fill: A=10, B=6 => q_set=6, q_fill_avg=8, set_ratio=0.75
        assert_approx_eq!(q_set, 6.0, 1e-9);
        assert_approx_eq!(q_fill_avg, 8.0, 1e-9);
        assert_approx_eq!(set_ratio, 0.75, 1e-9);

        let cost_per_set = Bps::FEE_POLY.apply_cost(0.49) + Bps::FEE_POLY.apply_cost(0.48);
        let proceeds_per_set = Bps::FEE_MERGE.apply_proceeds(1.0);
        let expected_pnl_set = (6.0 * proceeds_per_set) - (6.0 * cost_per_set);

        let exit_a = 0.48 * LEFTOVER_DUMP_MULT;
        let expected_left_cost_a = 4.0 * Bps::FEE_POLY.apply_cost(0.49);
        let expected_left_proceeds_a = 4.0 * Bps::FEE_POLY.apply_proceeds(exit_a);
        let expected_pnl_left = expected_left_proceeds_a - expected_left_cost_a;

        let expected_total = expected_pnl_set + expected_pnl_left;

        assert_approx_eq!(pnl_set, expected_pnl_set, 1e-9);
        assert_approx_eq!(pnl_left, expected_pnl_left, 1e-9);
        assert_approx_eq!(pnl_total, expected_total, 1e-9);
    }
}
