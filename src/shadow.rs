use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use tokio::sync::{mpsc, watch};
use tracing::{debug, info};

use crate::buckets::fill_share_p25;
use crate::config::Config;
use crate::health::HealthCounters;
use crate::reasons::{format_notes, ShadowReason};
use crate::recorder::{CsvAppender, SHADOW_HEADER};
use crate::schema::DUMP_SLIPPAGE_ASSUMED;
use crate::trade_store::TradeStore;
use crate::types::{now_ms, Bps, Leg, MarketDef, Side, Signal, TradeTick};

const LEFTOVER_DUMP_MULT: f64 = 1.0 - DUMP_SLIPPAGE_ASSUMED;

pub async fn run(
    cfg: Config,
    _markets: Vec<MarketDef>,
    mut trade_rx: mpsc::Receiver<TradeTick>,
    mut signal_rx: mpsc::Receiver<Signal>,
    shadow_path: PathBuf,
    health: Arc<HealthCounters>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut out = CsvAppender::open(shadow_path, &SHADOW_HEADER).context("open shadow_log.csv")?;

    let window_start_ms = cfg.shadow.window_start_ms;
    let window_end_ms = cfg.shadow.window_end_ms;

    let mut store = TradeStore::new_with_cap(cfg.shadow.trade_retention_ms, cfg.shadow.max_trades);
    let mut pending: Vec<Signal> = Vec::new();

    let mut tick = tokio::time::interval(Duration::from_millis(50));

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    let now = now_ms();
                    settle_ready(
                        &cfg,
                        &mut out,
                        &store,
                        &mut pending,
                        now,
                        window_start_ms,
                        window_end_ms,
                        health.as_ref(),
                    )?;
                    break;
                }
            }
            maybe = trade_rx.recv() => {
                let Some(t) = maybe else {
                    return Err(anyhow::anyhow!("trade channel closed"));
                };
                let push = store.push(t);
                if push.evicted > 0 {
                    health.inc_trade_store_evicted(push.evicted as u64);
                }
                health.set_trade_store_size(store.len());
            }
            maybe = signal_rx.recv() => {
                let Some(s) = maybe else {
                    return Err(anyhow::anyhow!("signal channel closed"));
                };
                pending.push(s);
            }
            _ = tick.tick() => {
                let now = now_ms();
                settle_ready(
                    &cfg,
                    &mut out,
                    &store,
                    &mut pending,
                    now,
                    window_start_ms,
                    window_end_ms,
                    health.as_ref(),
                )?;
            }
        }
    }

    out.flush_and_sync().context("flush shadow_log.csv")?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn settle_ready(
    cfg: &Config,
    out: &mut CsvAppender,
    store: &TradeStore,
    pending: &mut Vec<Signal>,
    now_ms: u64,
    window_start_ms: u64,
    window_end_ms: u64,
    health: &HealthCounters,
) -> anyhow::Result<()> {
    if pending.is_empty() {
        return Ok(());
    }

    let mut still_pending = Vec::with_capacity(pending.len());
    for s in pending.drain(..) {
        if now_ms < s.signal_ts_ms + window_end_ms {
            still_pending.push(s);
            continue;
        }
        settle_one(cfg, out, store, &s, window_start_ms, window_end_ms)?;
        health.set_last_shadow_write_ms(now_ms);
        health.inc_shadow_processed(1);
    }
    *pending = still_pending;
    Ok(())
}

fn settle_one(
    cfg: &Config,
    out: &mut CsvAppender,
    store: &TradeStore,
    s: &Signal,
    window_start_ms: u64,
    window_end_ms: u64,
) -> anyhow::Result<()> {
    let start_ms = s.signal_ts_ms + window_start_ms;
    let end_ms = s.signal_ts_ms + window_end_ms;

    let fill_share_used = fill_share_p25(s.bucket, &cfg.buckets);
    let window_stats = store.window_stats(&s.market_id, start_ms, end_ms);

    let legs_n = s.legs.len();

    // Make the CSV stable: always log legs in `leg_index` order (0..).
    let mut legs_sorted = s.legs.clone();
    legs_sorted.sort_by_key(|l| l.leg_index);

    let mut reasons: Vec<ShadowReason> = s.reasons.clone();

    let mut v_mkt: Vec<f64> = vec![0.0; legs_n.min(3)];
    let mut q_fill: Vec<f64> = vec![0.0; legs_n.min(3)];
    let mut invalid_limit = false;

    for (i, leg) in legs_sorted.iter().take(3).enumerate() {
        if !leg.limit_price.is_finite() || leg.limit_price <= 0.0 {
            invalid_limit = true;
            continue;
        }
        let v = store.volume_at_or_better_price(
            &s.market_id,
            &leg.token_id,
            start_ms,
            end_ms,
            leg.limit_price,
        );
        v_mkt[i] = v;
        let v_my = v * fill_share_used;
        q_fill[i] = s.q_req.min(v_my);
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

    let mut legs: Vec<Leg> = legs_sorted;
    while legs.len() < 3 {
        legs.push(Leg {
            leg_index: legs.len(),
            token_id: String::new(),
            side: Side::Buy,
            limit_price: 0.0,
            qty: 0.0,
            best_bid_at_signal: 0.0,
            best_ask_at_signal: 0.0,
        });
    }
    while v_mkt.len() < 3 {
        v_mkt.push(0.0);
    }
    while q_fill.len() < 3 {
        q_fill.push(0.0);
    }

    let mut q_left: Vec<f64> = vec![0.0; 3];
    for i in 0..legs_n.min(3) {
        q_left[i] = q_fill[i] - q_set;
    }

    let cost_per_set: f64 = legs
        .iter()
        .take(legs_n.min(3))
        .map(|l| Bps::FEE_POLY.apply_cost(l.limit_price))
        .sum();
    let proceeds_per_set = Bps::FEE_MERGE.apply_proceeds(1.0);

    let cost_set = q_set * cost_per_set;
    let proceeds_set = q_set * proceeds_per_set;
    let pnl_set = proceeds_set - cost_set;

    let mut pnl_left_total = 0.0f64;
    let mut bid_missing_legs: Vec<usize> = Vec::new();
    for (i, l) in legs.iter().take(legs_n.min(3)).enumerate() {
        let bid_missing = !l.best_bid_at_signal.is_finite() || l.best_bid_at_signal <= 0.0;
        if bid_missing {
            bid_missing_legs.push(i);
        }
        let exit_price = if bid_missing {
            0.0
        } else {
            l.best_bid_at_signal * LEFTOVER_DUMP_MULT
        };
        let cost = q_left[i] * Bps::FEE_POLY.apply_cost(l.limit_price);
        let proceeds = q_left[i] * Bps::FEE_POLY.apply_proceeds(exit_price);
        let pnl = proceeds - cost;
        pnl_left_total += pnl;
    }

    let total_pnl = pnl_set + pnl_left_total;
    let set_ratio = if q_fill_avg > 0.0 {
        q_set / q_fill_avg
    } else {
        0.0
    };

    if legs_n < 3 {
        reasons.push(ShadowReason::LegsPadded);
    }

    if !s.q_req.is_finite() || s.q_req <= 0.0 {
        reasons.push(ShadowReason::InvalidQty);
    }

    if legs
        .iter()
        .take(legs_n.min(3))
        .any(|l| !l.qty.is_finite() || l.qty <= 0.0)
    {
        reasons.push(ShadowReason::InvalidQty);
    }

    if !bid_missing_legs.is_empty() {
        reasons.push(ShadowReason::MissingBid);
    }

    if invalid_limit {
        reasons.push(ShadowReason::InvalidPrice);
    }

    let v_mkt_sum: f64 = v_mkt.iter().copied().sum();
    if window_stats.trades_in_window == 0 {
        reasons.push(ShadowReason::WindowEmpty);
    }

    if v_mkt_sum <= 0.0 {
        reasons.push(ShadowReason::NoTrades);
    }

    if store.dedup_hits_in_window(&s.market_id, start_ms, end_ms) > 0 {
        reasons.push(ShadowReason::DedupHit);
    }

    let mut worst_leg_token_id = legs
        .iter()
        .find(|l| l.leg_index == s.bucket_metrics.worst_leg_index)
        .map(|l| l.token_id.clone())
        .unwrap_or_default();

    if reasons.iter().any(|r| matches!(r, ShadowReason::BucketNan)) {
        worst_leg_token_id.clear();
    }

    if worst_leg_token_id.is_empty() {
        reasons.push(ShadowReason::BucketNan);
    }

    let mut kv: Vec<String> = Vec::new();
    kv.push("TS_SRC=local".to_string());
    kv.push(format!("LAT_MS={window_start_ms}"));
    kv.push(format!("WIN_END={window_end_ms}"));
    kv.push(format!("BUCKET={}", s.bucket.as_str().to_ascii_uppercase()));

    let notes = format_notes(&reasons, &kv);

    let mut record: Vec<String> = Vec::with_capacity(SHADOW_HEADER.len());
    record.push(s.run_id.clone());
    record.push(cfg.schema_version.clone());
    record.push(s.signal_id.to_string());
    record.push(s.signal_ts_ms.to_string());
    record.push(window_start_ms.to_string());
    record.push(window_end_ms.to_string());
    record.push(s.market_id.clone());
    record.push(s.strategy.as_str().to_string());
    record.push(s.bucket.as_str().to_ascii_lowercase());
    record.push(worst_leg_token_id);
    record.push(s.q_req.to_string());
    record.push((legs_n as u8).to_string());
    record.push(q_set.to_string());

    for i in 0..3 {
        record.push(legs[i].token_id.clone());
        record.push(legs[i].limit_price.to_string());
        record.push(legs[i].best_bid_at_signal.to_string());
        record.push(v_mkt[i].to_string());
        record.push(q_fill[i].to_string());
    }

    record.push(cost_set.to_string());
    record.push(proceeds_set.to_string());
    record.push(pnl_set.to_string());
    record.push(pnl_left_total.to_string());
    record.push(total_pnl.to_string());
    record.push(q_fill_avg.to_string());
    record.push(set_ratio.to_string());
    record.push(fill_share_used.to_string());
    record.push(DUMP_SLIPPAGE_ASSUMED.to_string());
    record.push(notes);
    debug_assert_eq!(record.len(), SHADOW_HEADER.len());

    out.write_record(record)?;

    debug!(signal_id = s.signal_id, q_set, total_pnl, "shadow settle");

    if s.signal_id % 100 == 0 {
        info!(signal_id = s.signal_id, "shadow checkpoint");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        BrainConfig, BucketConfig, CalibrationConfig, Config, LiveConfig, PolymarketConfig,
        ReportConfig, RunConfig, ShadowConfig, SimConfig,
    };
    use crate::recorder::CsvAppender;
    use crate::types::{Bps, Bucket, BucketMetrics, Leg, Side, Strategy};
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
                max_snapshot_staleness_ms: 500,
            },
            buckets: BucketConfig {
                fill_share_liquid_p25: 0.5,
                fill_share_thin_p25: 0.1,
            },
            shadow: ShadowConfig::default(),
            report: ReportConfig::default(),
            live: LiveConfig::default(),
            calibration: CalibrationConfig::default(),
            sim: SimConfig::default(),
        };

        let tmp =
            std::env::temp_dir().join(format!("razor_shadow_test_{}.csv", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let mut out = CsvAppender::open(&tmp, &SHADOW_HEADER).expect("open csv");

        let s = Signal {
            run_id: "run_test".to_string(),
            signal_id: 1,
            signal_ts_ms: base_ms,
            market_id: "mkt".to_string(),
            strategy: Strategy::Binary,
            bucket: Bucket::Liquid,
            reasons: Vec::new(),
            q_req: 10.0,
            raw_cost_bps: Bps::from_price_cost(0.97),
            raw_edge_bps: Bps::new(300),
            hard_fees_bps: Bps::FEE_POLY + Bps::FEE_MERGE,
            risk_premium_bps: Bps::new(80),
            expected_net_bps: Bps::new(10),
            bucket_metrics: BucketMetrics {
                worst_leg_index: 0,
                worst_spread_bps: 0,
                worst_depth3_usdc: 1000.0,
                is_depth3_degraded: false,
            },
            legs: vec![
                Leg {
                    leg_index: 0,
                    token_id: "A".to_string(),
                    side: Side::Buy,
                    limit_price: 0.49,
                    qty: 10.0,
                    best_bid_at_signal: 0.48,
                    best_ask_at_signal: 0.49,
                },
                Leg {
                    leg_index: 1,
                    token_id: "B".to_string(),
                    side: Side::Buy,
                    limit_price: 0.48,
                    qty: 10.0,
                    best_bid_at_signal: 0.47,
                    best_ask_at_signal: 0.48,
                },
            ],
        };

        let mut store = TradeStore::new_with_cap(60_000, usize::MAX);
        let _ = store.push(TradeTick {
            ts_ms: base_ms + 200,
            ingest_ts_ms: base_ms + 200,
            exchange_ts_ms: Some(base_ms + 200),
            market_id: "mkt".to_string(),
            token_id: "A".to_string(),
            price: 0.48,
            size: 30.0,
            trade_id: "t1".to_string(),
        });
        let _ = store.push(TradeTick {
            ts_ms: base_ms + 200,
            ingest_ts_ms: base_ms + 200,
            exchange_ts_ms: Some(base_ms + 200),
            market_id: "mkt".to_string(),
            token_id: "B".to_string(),
            price: 0.48,
            size: 12.0,
            trade_id: "t2".to_string(),
        });

        settle_one(&cfg, &mut out, &store, &s, 100, 1_100).expect("settle");

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
        let notes = cols[idx("notes")];
        assert!(notes.contains("LEGS_PADDED"));

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

    #[test]
    fn bid_missing_hard_penalty_is_visible_in_notes() {
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
                max_snapshot_staleness_ms: 500,
            },
            buckets: BucketConfig {
                fill_share_liquid_p25: 0.5,
                fill_share_thin_p25: 0.1,
            },
            shadow: ShadowConfig::default(),
            report: ReportConfig::default(),
            live: LiveConfig::default(),
            calibration: CalibrationConfig::default(),
            sim: SimConfig::default(),
        };

        let tmp = std::env::temp_dir().join(format!(
            "razor_shadow_test_bid_missing_{}.csv",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&tmp);
        let mut out = CsvAppender::open(&tmp, &SHADOW_HEADER).expect("open csv");

        let s = Signal {
            run_id: "run_test".to_string(),
            signal_id: 1,
            signal_ts_ms: base_ms,
            market_id: "mkt".to_string(),
            strategy: Strategy::Binary,
            bucket: Bucket::Liquid,
            reasons: Vec::new(),
            q_req: 10.0,
            raw_cost_bps: Bps::from_price_cost(0.97),
            raw_edge_bps: Bps::new(300),
            hard_fees_bps: Bps::FEE_POLY + Bps::FEE_MERGE,
            risk_premium_bps: Bps::new(80),
            expected_net_bps: Bps::new(10),
            bucket_metrics: BucketMetrics {
                worst_leg_index: 0,
                worst_spread_bps: 0,
                worst_depth3_usdc: 1000.0,
                is_depth3_degraded: false,
            },
            legs: vec![
                Leg {
                    leg_index: 0,
                    token_id: "A".to_string(),
                    side: Side::Buy,
                    limit_price: 0.49,
                    qty: 10.0,
                    best_bid_at_signal: 0.0, // missing
                    best_ask_at_signal: 0.49,
                },
                Leg {
                    leg_index: 1,
                    token_id: "B".to_string(),
                    side: Side::Buy,
                    limit_price: 0.48,
                    qty: 10.0,
                    best_bid_at_signal: 0.47,
                    best_ask_at_signal: 0.48,
                },
            ],
        };

        let mut store = TradeStore::new_with_cap(60_000, usize::MAX);
        let _ = store.push(TradeTick {
            ts_ms: base_ms + 200,
            ingest_ts_ms: base_ms + 200,
            exchange_ts_ms: Some(base_ms + 200),
            market_id: "mkt".to_string(),
            token_id: "A".to_string(),
            price: 0.48,
            size: 30.0,
            trade_id: "t1".to_string(),
        });
        let _ = store.push(TradeTick {
            ts_ms: base_ms + 200,
            ingest_ts_ms: base_ms + 200,
            exchange_ts_ms: Some(base_ms + 200),
            market_id: "mkt".to_string(),
            token_id: "B".to_string(),
            price: 0.48,
            size: 12.0,
            trade_id: "t2".to_string(),
        });

        settle_one(&cfg, &mut out, &store, &s, 100, 1_100).expect("settle");

        let text = std::fs::read_to_string(&tmp).expect("read csv");
        let mut lines = text.lines();
        let header = lines.next().expect("header");
        let row = lines.next().expect("row");
        let names: Vec<&str> = header.split(',').collect();
        let cols: Vec<&str> = row.split(',').collect();

        let idx = |name: &str| -> usize {
            names
                .iter()
                .position(|n| n.eq_ignore_ascii_case(name))
                .unwrap_or_else(|| panic!("missing column {name}"))
        };

        let notes = cols[idx("notes")];
        assert!(notes.contains("MISSING_BID"));
        assert!(notes.contains("LEGS_PADDED"));

        let q_set: f64 = cols[idx("q_set")].parse().expect("q_set");
        let q_fill0: f64 = cols[idx("leg0_q_fill")].parse().expect("leg0_q_fill");
        let q_left0 = q_fill0 - q_set;
        assert_approx_eq!(q_left0, 4.0, 1e-9);

        let pnl_left_total: f64 = cols[idx("pnl_left_total")].parse().expect("pnl_left_total");
        let expected_cost0 = q_left0 * Bps::FEE_POLY.apply_cost(0.49);
        // exit_price=0 => proceeds=0 => pnl_left_total = -cost_left0
        assert_approx_eq!(pnl_left_total, -expected_cost0, 1e-9);
    }
}
