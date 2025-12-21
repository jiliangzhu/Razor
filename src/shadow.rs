use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context as _;
use tokio::sync::mpsc;
use tracing::{debug, info};

use crate::config::Config;
use crate::recorder::{CsvAppender, SHADOW_HEADER};
use crate::types::{now_us, Bps, Bucket, MarketDef, Signal, TradeTick};

const LEFTOVER_DUMP_MULT: f64 = 0.95;

pub async fn run(
    cfg: Config,
    _markets: Vec<MarketDef>,
    mut trade_rx: mpsc::Receiver<TradeTick>,
    mut signal_rx: mpsc::Receiver<Signal>,
    shadow_path: PathBuf,
) -> anyhow::Result<()> {
    let mut out = CsvAppender::open(shadow_path, &SHADOW_HEADER).context("open shadow_log.csv")?;

    let retention_us = cfg.shadow.trade_retention_ms * 1_000;
    let window_start_us = cfg.shadow.window_start_ms * 1_000;
    let window_end_us = cfg.shadow.window_end_ms * 1_000;

    let mut trades_by_token: HashMap<String, VecDeque<TradeTick>> = HashMap::new();
    let mut pending: Vec<Signal> = Vec::new();

    let mut tick = tokio::time::interval(Duration::from_millis(50));

    loop {
        tokio::select! {
            maybe = trade_rx.recv() => {
                let Some(t) = maybe else {
                    return Err(anyhow::anyhow!("trade channel closed"));
                };

                let q = trades_by_token.entry(t.token_id.clone()).or_default();
                q.push_back(t);

                prune_trades(&mut trades_by_token, retention_us);
            }
            maybe = signal_rx.recv() => {
                let Some(s) = maybe else {
                    return Err(anyhow::anyhow!("signal channel closed"));
                };
                pending.push(s);
            }
            _ = tick.tick() => {
                let now = now_us();
                if pending.is_empty() {
                    continue;
                }

                let mut still_pending = Vec::with_capacity(pending.len());
                for s in pending.drain(..) {
                    if now < s.ts_signal_us + window_end_us {
                        still_pending.push(s);
                        continue;
                    }
                    settle_one(&cfg, &mut out, &trades_by_token, &s, window_start_us, window_end_us)?;
                }
                pending = still_pending;
            }
        }
    }
}

fn prune_trades(trades_by_token: &mut HashMap<String, VecDeque<TradeTick>>, retention_us: u64) {
    let cutoff = now_us().saturating_sub(retention_us);
    for q in trades_by_token.values_mut() {
        while q.front().is_some_and(|t| t.ts_recv_us < cutoff) {
            q.pop_front();
        }
    }
}

fn settle_one(
    cfg: &Config,
    out: &mut CsvAppender,
    trades_by_token: &HashMap<String, VecDeque<TradeTick>>,
    s: &Signal,
    window_start_us: u64,
    window_end_us: u64,
) -> anyhow::Result<()> {
    let start_us = s.ts_signal_us + window_start_us;
    let end_us = s.ts_signal_us + window_end_us;

    let fill_share_used = match s.bucket {
        Bucket::Liquid => cfg.buckets.fill_share_liquid_p25,
        Bucket::Thin => cfg.buckets.fill_share_thin_p25,
    };

    let mut v_mkt: Vec<f64> = Vec::with_capacity(s.legs.len());
    let mut q_fill: Vec<f64> = Vec::with_capacity(s.legs.len());

    for leg in &s.legs {
        let mut v = 0.0f64;
        if let Some(q) = trades_by_token.get(&leg.token_id) {
            for t in q {
                if t.market_id != s.market_id {
                    continue;
                }
                if t.ts_recv_us < start_us || t.ts_recv_us > end_us {
                    continue;
                }
                if t.price <= leg.p_limit {
                    v += t.size;
                }
            }
        }
        v_mkt.push(v);

        let v_my = v * fill_share_used;
        q_fill.push(s.q_req.min(v_my));
    }

    let q_set = q_fill
        .iter()
        .copied()
        .fold(f64::INFINITY, |a, b| a.min(b))
        .min(s.q_req);

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

    let pnl_set = (q_set * proceeds_per_set) - (q_set * cost_per_set);

    let mut pnl_left_total = 0.0f64;
    let mut exits: [f64; 3] = [0.0; 3];
    for (i, l) in legs.iter().take(3).enumerate() {
        let exit = l.best_bid_at_t0 * LEFTOVER_DUMP_MULT;
        exits[i] = exit;
        let cost = q_left[i] * Bps::FEE_POLY.apply_cost(l.p_limit);
        let proceeds = q_left[i] * Bps::FEE_POLY.apply_proceeds(exit);
        pnl_left_total += proceeds - cost;
    }

    let pnl_total = pnl_set + pnl_left_total;
    let set_ratio = if s.q_req > 0.0 { q_set / s.q_req } else { 0.0 };

    out.write_record([
        s.ts_signal_us.to_string(),
        s.signal_id.to_string(),
        s.market_id.clone(),
        s.strategy.as_str().to_string(),
        s.bucket.as_str().to_string(),
        s.q_req.to_string(),
        legs[0].token_id.clone(),
        legs[0].p_limit.to_string(),
        v_mkt[0].to_string(),
        q_fill[0].to_string(),
        legs[0].best_bid_at_t0.to_string(),
        exits[0].to_string(),
        legs[1].token_id.clone(),
        legs[1].p_limit.to_string(),
        v_mkt[1].to_string(),
        q_fill[1].to_string(),
        legs[1].best_bid_at_t0.to_string(),
        exits[1].to_string(),
        legs[2].token_id.clone(),
        legs[2].p_limit.to_string(),
        v_mkt[2].to_string(),
        q_fill[2].to_string(),
        legs[2].best_bid_at_t0.to_string(),
        exits[2].to_string(),
        q_set.to_string(),
        q_left[0].to_string(),
        q_left[1].to_string(),
        q_left[2].to_string(),
        set_ratio.to_string(),
        pnl_set.to_string(),
        pnl_left_total.to_string(),
        pnl_total.to_string(),
        fill_share_used.to_string(),
        cfg.brain.risk_premium_bps.to_string(),
        s.expected_net_bps.raw().to_string(),
    ])?;

    debug!(signal_id = s.signal_id, q_set, pnl_total, "shadow settle");

    if s.signal_id % 100 == 0 {
        info!(signal_id = s.signal_id, "shadow checkpoint");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        BrainConfig, BucketConfig, Config, PolymarketConfig, RunConfig, ShadowConfig,
    };
    use crate::recorder::CsvAppender;
    use crate::types::{Bps, SignalLeg, Strategy};

    #[test]
    fn settles_binary_signal_with_leftover_penalty() {
        let cfg = Config {
            polymarket: PolymarketConfig::default(),
            run: RunConfig {
                data_dir: "data".into(),
                market_ids: vec![],
            },
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
        };

        let tmp =
            std::env::temp_dir().join(format!("razor_shadow_test_{}.csv", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        let mut out = CsvAppender::open(&tmp, &SHADOW_HEADER).expect("open csv");

        let s = Signal {
            signal_id: 1,
            ts_signal_us: 1_000_000,
            market_id: "mkt".to_string(),
            strategy: Strategy::Binary,
            bucket: Bucket::Liquid,
            q_req: 10.0,
            expected_net_bps: Bps::new(20),
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

        let mut trades_by_token: HashMap<String, VecDeque<TradeTick>> = HashMap::new();
        trades_by_token.insert(
            "A".to_string(),
            VecDeque::from([TradeTick {
                ts_recv_us: 1_200_000,
                market_id: "mkt".to_string(),
                token_id: "A".to_string(),
                price: 0.48,
                size: 30.0,
            }]),
        );
        trades_by_token.insert(
            "B".to_string(),
            VecDeque::from([TradeTick {
                ts_recv_us: 1_200_000,
                market_id: "mkt".to_string(),
                token_id: "B".to_string(),
                price: 0.48,
                size: 12.0,
            }]),
        );

        settle_one(&cfg, &mut out, &trades_by_token, &s, 100_000, 1_100_000).expect("settle");

        let text = std::fs::read_to_string(&tmp).expect("read csv");
        let mut lines = text.lines();
        let _header = lines.next().expect("header");
        let row = lines.next().expect("row");
        let cols: Vec<&str> = row.split(',').collect();
        assert_eq!(cols.len(), SHADOW_HEADER.len());

        let q_set: f64 = cols[24].parse().expect("q_set");
        let set_ratio: f64 = cols[28].parse().expect("set_ratio");
        let pnl_total: f64 = cols[31].parse().expect("pnl_total");

        // q_fill: A=10, B=6 => q_set=6, set_ratio=0.6
        assert!((q_set - 6.0).abs() < 1e-9);
        assert!((set_ratio - 0.6).abs() < 1e-9);

        // pnl_total should be negative with leftover dump penalty.
        assert!(pnl_total < 0.0);
    }
}
