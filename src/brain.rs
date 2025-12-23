use std::collections::HashMap;

use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};

use crate::buckets::bucket_for_snapshot;
use crate::config::Config;
use crate::types::{
    now_us, Bps, Bucket, BucketMode, MarketDef, MarketSnapshot, Signal, SignalLeg, Strategy,
};

const COOLDOWN_IMPROVE_OVERRIDE_BPS: Bps = Bps::new(20);

#[derive(Clone, Copy, Debug)]
struct LastSignalState {
    ts_ms: u64,
    expected_net_bps: Bps,
}

#[derive(Debug)]
enum SkipReason {
    BelowMinEdge,
    CooldownNotImproved {
        remaining_ms: u64,
        improvement_bps: Bps,
    },
}

#[derive(Clone, Copy, Debug)]
struct EvalMetrics {
    strategy: Strategy,
    bucket: Bucket,
    bucket_mode: BucketMode,
    raw_cost_bps: Bps,
    raw_edge_bps: Bps,
    hard_fees_bps: Bps,
    risk_premium_bps: Bps,
    expected_net_bps: Bps,
    ts_snapshot_us: u64,
}

pub async fn run(
    cfg: Config,
    markets: Vec<MarketDef>,
    mut snap_rx: watch::Receiver<Option<MarketSnapshot>>,
    signal_tx: mpsc::Sender<Signal>,
) -> anyhow::Result<()> {
    let mut next_signal_id: u64 = 1;
    let mut last_by_market: HashMap<String, LastSignalState> = HashMap::new();
    let cooldown_ms = cfg.brain.signal_cooldown_ms;
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
        let ts_ms = ts_signal_us / 1_000;

        let metrics = match eval_snapshot(&cfg, &snap) {
            Ok(v) => v,
            Err(e) => {
                warn!(market_id = %snap.market_id, error = %e, "skip snapshot");
                continue;
            }
        };

        if let Err(reason) = should_emit(
            ts_ms,
            metrics.expected_net_bps,
            min_net_edge,
            cooldown_ms,
            last_by_market.get(&snap.market_id),
        ) {
            match reason {
                SkipReason::BelowMinEdge => {
                    debug!(
                        market_id = %snap.market_id,
                        expected_net_bps = metrics.expected_net_bps.raw(),
                        min_net_edge_bps = min_net_edge.raw(),
                        "skip: below min net edge"
                    );
                }
                SkipReason::CooldownNotImproved {
                    remaining_ms,
                    improvement_bps,
                } => {
                    debug!(
                        market_id = %snap.market_id,
                        remaining_ms,
                        expected_net_bps = metrics.expected_net_bps.raw(),
                        improvement_bps = improvement_bps.raw(),
                        "skip: cooldown"
                    );
                }
            }
            continue;
        };

        let q_req = cfg.brain.q_req;
        let worst_leg_token_id = crate::buckets::worst_leg(&snap)
            .map(|(leg, _)| leg.token_id.clone())
            .unwrap_or_default();
        let legs: Vec<SignalLeg> = snap
            .legs
            .iter()
            .map(|l| SignalLeg {
                token_id: l.token_id.clone(),
                p_limit: l.best_ask,
                best_bid_at_t0: l.best_bid,
            })
            .collect();

        let signal_id = next_signal_id;
        next_signal_id += 1;

        let signal = Signal {
            signal_id,
            ts_signal_us,
            ts_ms,
            ts_snapshot_us: metrics.ts_snapshot_us,
            market_id: snap.market_id.clone(),
            strategy: metrics.strategy,
            bucket: metrics.bucket,
            bucket_mode: metrics.bucket_mode,
            worst_leg_token_id,
            q_req,
            raw_cost_bps: metrics.raw_cost_bps,
            raw_edge_bps: metrics.raw_edge_bps,
            hard_fees_bps: metrics.hard_fees_bps,
            risk_premium_bps: metrics.risk_premium_bps,
            expected_net_bps: metrics.expected_net_bps,
            legs,
        };

        last_by_market.insert(
            snap.market_id.clone(),
            LastSignalState {
                ts_ms,
                expected_net_bps: metrics.expected_net_bps,
            },
        );

        match signal_tx.try_send(signal) {
            Ok(()) => {
                info!(
                    signal_id,
                    market_id = %snap.market_id,
                    bucket = %metrics.bucket.as_str(),
                    bucket_mode = %metrics.bucket_mode.as_str(),
                    strategy = %metrics.strategy.as_str(),
                    raw_cost_bps = metrics.raw_cost_bps.raw(),
                    expected_net_bps = metrics.expected_net_bps.raw(),
                    "signal"
                );
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(s)) => {
                warn!(
                    signal_id = s.signal_id,
                    market_id = %s.market_id,
                    bucket = %s.bucket.as_str(),
                    bucket_mode = %s.bucket_mode.as_str(),
                    ts_snapshot_us = s.ts_snapshot_us,
                    raw_cost_bps = s.raw_cost_bps.raw(),
                    raw_edge_bps = s.raw_edge_bps.raw(),
                    hard_fees_bps = s.hard_fees_bps.raw(),
                    risk_premium_bps = s.risk_premium_bps.raw(),
                    expected_net_bps = s.expected_net_bps.raw(),
                    "signal channel full; dropped"
                );
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                return Err(anyhow::anyhow!("signal receiver dropped"));
            }
        }
    }
}

fn eval_snapshot(cfg: &Config, snap: &MarketSnapshot) -> anyhow::Result<EvalMetrics> {
    let strategy = match snap.legs.len() {
        2 => Strategy::Binary,
        3 => Strategy::Triangle,
        n => anyhow::bail!("unsupported legs: {n}"),
    };

    let bucket = bucket_for_snapshot(snap);

    let bucket_mode = if snap
        .legs
        .iter()
        .all(|l| l.ask_depth3_usdc.is_finite() && l.ask_depth3_usdc > 0.0)
    {
        BucketMode::FullL2
    } else {
        BucketMode::ApproxDepth1
    };

    let ts_snapshot_us = snap.legs.iter().map(|l| l.ts_recv_us).max().unwrap_or(0);

    let sum_ask: f64 = snap.legs.iter().map(|l| l.best_ask).sum();
    if !sum_ask.is_finite() || sum_ask < 0.0 {
        anyhow::bail!("invalid sum_ask={sum_ask}");
    }

    // Cost/gating conversion uses ceil to avoid overstating edge near thresholds.
    let raw_cost_bps = Bps::from_price_cost(sum_ask);
    let raw_edge_bps = Bps::ONE_HUNDRED_PERCENT - raw_cost_bps;

    let hard_fees_bps = Bps::FEE_POLY + Bps::FEE_MERGE;
    let risk_premium_bps = Bps::new(cfg.brain.risk_premium_bps);

    let expected_net_bps = raw_edge_bps - hard_fees_bps - risk_premium_bps;

    Ok(EvalMetrics {
        strategy,
        bucket,
        bucket_mode,
        raw_cost_bps,
        raw_edge_bps,
        hard_fees_bps,
        risk_premium_bps,
        expected_net_bps,
        ts_snapshot_us,
    })
}

fn should_emit(
    now_ms: u64,
    expected_net_bps: Bps,
    min_net_edge_bps: Bps,
    cooldown_ms: u64,
    prev: Option<&LastSignalState>,
) -> Result<(), SkipReason> {
    if expected_net_bps < min_net_edge_bps {
        return Err(SkipReason::BelowMinEdge);
    }

    let Some(prev) = prev else {
        return Ok(());
    };

    let elapsed_ms = now_ms.saturating_sub(prev.ts_ms);
    if elapsed_ms >= cooldown_ms {
        return Ok(());
    }

    let improvement = expected_net_bps - prev.expected_net_bps;
    if improvement >= COOLDOWN_IMPROVE_OVERRIDE_BPS {
        return Ok(());
    }

    Err(SkipReason::CooldownNotImproved {
        remaining_ms: cooldown_ms.saturating_sub(elapsed_ms),
        improvement_bps: improvement,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        BrainConfig, BucketConfig, Config, PolymarketConfig, ReportConfig, RunConfig, ShadowConfig,
    };
    use crate::types::LegSnapshot;

    #[test]
    fn test_bps_from_price_rounding() {
        assert_eq!(Bps::from_price(0.985).raw(), 9850);
    }

    #[test]
    fn test_net_edge_computation() {
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
            buckets: BucketConfig::default(),
            shadow: ShadowConfig::default(),
            report: ReportConfig::default(),
        };

        let snap = MarketSnapshot {
            market_id: "0xdeadbeef".to_string(),
            legs: vec![
                LegSnapshot {
                    token_id: "a".to_string(),
                    best_ask: 0.48,
                    best_bid: 0.4796,
                    ask_depth3_usdc: 1000.0,
                    ts_recv_us: 1,
                },
                LegSnapshot {
                    token_id: "b".to_string(),
                    best_ask: 0.49,
                    best_bid: 0.4896,
                    ask_depth3_usdc: 1000.0,
                    ts_recv_us: 2,
                },
            ],
        };

        let metrics = eval_snapshot(&cfg, &snap).expect("eval");
        assert_eq!(metrics.strategy, Strategy::Binary);
        assert_eq!(metrics.bucket, Bucket::Liquid);
        assert_eq!(metrics.raw_cost_bps.raw(), 9700);
        assert_eq!(metrics.raw_edge_bps.raw(), 300);
        assert_eq!(metrics.hard_fees_bps.raw(), 210);
        assert_eq!(metrics.risk_premium_bps.raw(), 80);
        // net = 300 - 210 - 80 = 10
        assert_eq!(metrics.expected_net_bps.raw(), 10);
        assert_eq!(metrics.ts_snapshot_us, 2);
    }

    #[test]
    fn test_filter_min_net_edge() {
        let now_ms = 1_000;
        let min_edge = Bps::new(11);
        let expected = Bps::new(10);
        assert!(should_emit(now_ms, expected, min_edge, 1_000, None).is_err());
    }

    #[test]
    fn test_cooldown_override_on_improve() {
        let prev = LastSignalState {
            ts_ms: 1_000,
            expected_net_bps: Bps::new(50),
        };
        let now_ms = 1_500; // within cooldown=1_000 (elapsed 500)
        let min_edge = Bps::new(-10_000);
        let cooldown_ms = 1_000;

        // Improve by 19 bps => still blocked.
        let expected = Bps::new(69);
        let err = should_emit(now_ms, expected, min_edge, cooldown_ms, Some(&prev)).unwrap_err();
        assert!(matches!(err, SkipReason::CooldownNotImproved { .. }));

        // Improve by 20 bps => override.
        let expected = Bps::new(70);
        assert!(should_emit(now_ms, expected, min_edge, cooldown_ms, Some(&prev)).is_ok());
    }

    #[test]
    fn sum_asks_ge_one_is_non_signal_path() {
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
            buckets: BucketConfig::default(),
            shadow: ShadowConfig::default(),
            report: ReportConfig::default(),
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

        let metrics = eval_snapshot(&cfg, &snap).expect("eval");
        assert_eq!(metrics.bucket, Bucket::Liquid);
        assert!(metrics.expected_net_bps <= Bps::ZERO);
    }
}
