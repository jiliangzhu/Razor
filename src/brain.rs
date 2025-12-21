use std::collections::HashMap;

use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};

use crate::buckets::bucket_for_snapshot;
use crate::config::Config;
use crate::types::{now_us, Bps, Bucket, MarketDef, MarketSnapshot, Signal, SignalLeg};

pub async fn run(
    cfg: Config,
    markets: Vec<MarketDef>,
    mut snap_rx: watch::Receiver<Option<MarketSnapshot>>,
    signal_tx: mpsc::Sender<Signal>,
) -> anyhow::Result<()> {
    let mut next_signal_id: u64 = 1;
    let mut last_emit_us: HashMap<String, u64> = HashMap::new();
    let cooldown_us = cfg.brain.signal_cooldown_ms * 1_000;
    let min_net_edge = Bps::new(cfg.brain.min_net_edge_bps);

    let mut supported: HashMap<String, usize> = HashMap::new();
    for m in markets {
        supported.insert(m.market_id, m.token_ids.len());
    }

    loop {
        snap_rx.changed().await?;
        let Some(snap) = snap_rx.borrow().clone() else {
            continue;
        };

        let Some(&leg_count) = supported.get(&snap.market_id) else {
            continue;
        };
        if snap.legs.len() != leg_count {
            continue;
        }

        let ts_signal_us = now_us();

        if let Some(prev) = last_emit_us.get(&snap.market_id) {
            if ts_signal_us.saturating_sub(*prev) < cooldown_us {
                continue;
            }
        }

        let (strategy, bucket, net_edge) = match eval_snapshot(&cfg, &snap) {
            Ok(v) => v,
            Err(e) => {
                warn!(market_id = %snap.market_id, error = %e, "skip snapshot");
                continue;
            }
        };

        if net_edge <= min_net_edge {
            debug!(
                market_id = %snap.market_id,
                net_edge_bps = net_edge.raw(),
                "filtered by net edge"
            );
            continue;
        }

        let q_req = cfg.brain.q_req;
        let legs: Vec<SignalLeg> = snap
            .legs
            .iter()
            .map(|l| SignalLeg {
                token_id: l.token_id.clone(),
                p_limit: l.best_ask,
                best_bid_at_t0: l.best_bid,
            })
            .collect();

        let signal = Signal {
            signal_id: next_signal_id,
            ts_signal_us,
            market_id: snap.market_id.clone(),
            strategy,
            bucket,
            q_req,
            expected_net_bps: net_edge,
            legs,
        };

        next_signal_id += 1;
        last_emit_us.insert(snap.market_id.clone(), ts_signal_us);

        info!(
            signal_id = signal.signal_id,
            market_id = %signal.market_id,
            bucket = %signal.bucket.as_str(),
            strategy = %signal.strategy.as_str(),
            expected_net_bps = signal.expected_net_bps.raw(),
            "signal"
        );

        if signal_tx.send(signal).await.is_err() {
            return Err(anyhow::anyhow!("signal receiver dropped"));
        }
    }
}

fn eval_snapshot(
    cfg: &Config,
    snap: &MarketSnapshot,
) -> anyhow::Result<(crate::types::Strategy, Bucket, Bps)> {
    let strategy = match snap.legs.len() {
        2 => crate::types::Strategy::Binary,
        3 => crate::types::Strategy::Triangle,
        n => anyhow::bail!("unsupported legs: {n}"),
    };

    let bucket = bucket_for_snapshot(snap);

    let sum_ask: f64 = snap.legs.iter().map(|l| l.best_ask).sum();
    // Common case: `sum(best_ask_i) >= 1.0` just means "no arb" — never panic / crash the brain.
    // Treat it as a non-signal path (net edge forced negative) so the caller filters it out.
    if sum_ask >= 1.0 {
        return Ok((strategy, bucket, Bps::new(-10_000)));
    }

    let raw_cost_bps = Bps::from_price_cost(sum_ask);
    let raw_edge = Bps::ONE_HUNDRED_PERCENT - raw_cost_bps;

    let hard_fees = Bps::FEE_POLY + Bps::FEE_MERGE;
    let risk_premium = Bps::new(cfg.brain.risk_premium_bps);

    let net_edge = raw_edge - hard_fees - risk_premium;

    Ok((strategy, bucket, net_edge))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        BrainConfig, BucketConfig, Config, PolymarketConfig, RunConfig, ShadowConfig,
    };
    use crate::types::LegSnapshot;

    #[test]
    fn net_edge_is_bps_domain() {
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
            buckets: BucketConfig::default(),
            shadow: ShadowConfig::default(),
        };

        let snap = MarketSnapshot {
            market_id: "0xdeadbeef".to_string(),
            legs: vec![
                LegSnapshot {
                    token_id: "a".to_string(),
                    best_ask: 0.45,
                    best_bid: 0.4492,
                    ask_depth3_usdc: 1000.0,
                    ts_recv_us: 0,
                },
                LegSnapshot {
                    token_id: "b".to_string(),
                    best_ask: 0.45,
                    best_bid: 0.4492,
                    ask_depth3_usdc: 1000.0,
                    ts_recv_us: 0,
                },
            ],
        };

        let (_, bucket, net_edge) = eval_snapshot(&cfg, &snap).expect("eval");
        assert_eq!(bucket, Bucket::Liquid);
        // sum ask = 0.90 => raw_edge = 1000 bps; net = 1000 - 210 - 80 = 710
        assert_eq!(net_edge.raw(), 710);
    }

    #[test]
    fn sum_asks_ge_one_is_non_signal_path() {
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
            buckets: BucketConfig::default(),
            shadow: ShadowConfig::default(),
        };

        let snap = MarketSnapshot {
            market_id: "m".to_string(),
            legs: vec![
                LegSnapshot {
                    token_id: "a".to_string(),
                    best_ask: 0.6,
                    best_bid: 0.5992,
                    ask_depth3_usdc: 1_000.0,
                    ts_recv_us: 0,
                },
                LegSnapshot {
                    token_id: "b".to_string(),
                    best_ask: 0.6,
                    best_bid: 0.5992,
                    ask_depth3_usdc: 1_000.0,
                    ts_recv_us: 0,
                },
            ],
        };

        let (_, bucket, net_edge) = eval_snapshot(&cfg, &snap).expect("eval");
        assert_eq!(bucket, Bucket::Liquid);
        assert!(net_edge <= Bps::ZERO);
    }
}
