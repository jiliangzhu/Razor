use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};

use crate::buckets::classify_bucket;
use crate::config::Config;
use crate::health::HealthCounters;
use crate::reasons::ShadowNoteReason;
use crate::types::{
    now_ms, now_us, Bps, Bucket, BucketMetrics, Leg, MarketDef, MarketSnapshot, Side, Signal,
    Strategy,
};

#[derive(Clone, Copy, Debug)]
struct LastSignalState {
    ts_ms: u64,
    _expected_net_bps: Bps,
}

#[derive(Debug)]
enum SkipReason {
    BelowMinEdge,
    SuppressedDuplicate {
        remaining_ms: u64,
        key_cost_bps: i32,
    },
}

#[derive(Clone, Debug)]
struct EvalMetrics {
    strategy: Strategy,
    bucket: Bucket,
    raw_cost_bps: Bps,
    raw_edge_bps: Bps,
    hard_fees_bps: Bps,
    risk_premium_bps: Bps,
    expected_net_bps: Bps,
    bucket_metrics: BucketMetrics,
    worst_leg_token_id: String,
    reasons: Vec<ShadowNoteReason>,
}

#[derive(Clone, Debug)]
struct NegRiskMarket {
    market_id: String,
    yes_token_id: String,
    no_token_id: String,
}

#[derive(Clone, Debug)]
struct NegRiskGroup {
    #[allow(dead_code)]
    group_id: String,
    markets: Vec<NegRiskMarket>,
}

pub async fn run(
    cfg: Config,
    run_id: String,
    markets: Vec<MarketDef>,
    mut snap_rx: watch::Receiver<Option<MarketSnapshot>>,
    signal_tx: mpsc::Sender<Signal>,
    health: Arc<HealthCounters>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut next_signal_id: u64 = 1;
    let mut last_by_key: HashMap<(String, Strategy, i32), LastSignalState> = HashMap::new();
    let cooldown_ms = cfg.brain.signal_cooldown_ms;
    let min_net_edge = Bps::new(cfg.brain.min_net_edge_bps);
    let mut last_prune_ms: u64 = 0;
    const DEDUP_PRUNE_EVERY_MS: u64 = 60_000;
    const DEDUP_TTL_MS: u64 = 60 * 60_000;

    let mut supported: HashMap<String, usize> = HashMap::new();
    let mut neg_risk_groups: HashMap<String, NegRiskGroup> = HashMap::new();
    let mut groups_by_market: HashMap<String, Vec<String>> = HashMap::new();

    for m in &markets {
        supported.insert(m.market_id.clone(), m.token_ids.len());
        if let (Some(group_id), Some(yes), Some(no)) = (
            m.neg_risk_market_id.clone(),
            m.yes_token_id.clone(),
            m.no_token_id.clone(),
        ) {
            let entry = neg_risk_groups
                .entry(group_id.clone())
                .or_insert_with(|| NegRiskGroup {
                    group_id: group_id.clone(),
                    markets: Vec::new(),
                });
            entry.markets.push(NegRiskMarket {
                market_id: m.market_id.clone(),
                yes_token_id: yes,
                no_token_id: no,
            });
            groups_by_market
                .entry(m.market_id.clone())
                .or_default()
                .push(group_id);
        }
    }

    for group in neg_risk_groups.values_mut() {
        group
            .markets
            .sort_by(|a, b| a.market_id.cmp(&b.market_id));
    }

    let mut latest_by_market: HashMap<String, MarketSnapshot> = HashMap::new();

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() { break; }
            }
            res = snap_rx.changed() => {
                res?;
            }
        }
        if *shutdown.borrow() {
            break;
        }
        let Some(snap) = snap_rx.borrow().clone() else {
            continue;
        };

        let Some(&leg_count) = supported.get(&snap.market_id) else {
            continue;
        };
        if snap.legs.len() != leg_count {
            continue;
        }

        let max_recv_us = snap.legs.iter().map(|l| l.ts_recv_us).max().unwrap_or(0);
        if max_recv_us > 0 {
            let lag_ms = now_us().saturating_sub(max_recv_us) / 1000;
            if lag_ms > cfg.brain.max_snapshot_staleness_ms {
                health.inc_snapshots_stale_skipped(1);
                debug!(
                    market_id = %snap.market_id,
                    lag_ms,
                    threshold_ms = cfg.brain.max_snapshot_staleness_ms,
                    "skip: snapshot stale"
                );
                continue;
            }
        }

        let signal_ts_ms = now_ms();
        if signal_ts_ms.saturating_sub(last_prune_ms) >= DEDUP_PRUNE_EVERY_MS {
            last_prune_ms = signal_ts_ms;
            let cutoff = signal_ts_ms.saturating_sub(DEDUP_TTL_MS);
            let before = last_by_key.len();
            last_by_key.retain(|_, v| v.ts_ms >= cutoff);
            let after = last_by_key.len();
            if after < before {
                debug!(
                    pruned = before - after,
                    remaining = after,
                    ttl_ms = DEDUP_TTL_MS,
                    "brain dedup state pruned"
                );
            }
        }

        let metrics = match eval_snapshot(&cfg, &snap) {
            Ok(v) => v,
            Err(e) => {
                warn!(market_id = %snap.market_id, error = %e, "skip snapshot");
                continue;
            }
        };

        let q_req = cfg.brain.q_req;
        let legs: Vec<Leg> = snap
            .legs
            .iter()
            .enumerate()
            .map(|(idx, l)| Leg {
                leg_index: idx,
                market_id: l.market_id.clone(),
                token_id: l.token_id.clone(),
                side: Side::Buy,
                limit_price: l.best_ask,
                qty: q_req,
                best_bid_at_signal: l.best_bid,
                best_ask_at_signal: l.best_ask,
            })
            .collect();

        emit_signal(
            &run_id,
            signal_ts_ms,
            snap.market_id.clone(),
            metrics,
            q_req,
            legs,
            min_net_edge,
            cooldown_ms,
            &mut last_by_key,
            &mut next_signal_id,
            &signal_tx,
            health.as_ref(),
        )?;

        latest_by_market.insert(snap.market_id.clone(), snap.clone());

        if let Some(groups) = groups_by_market.get(&snap.market_id) {
            for group_id in groups {
                if let Some(group) = neg_risk_groups.get(group_id) {
                    if let Some(neg_snap) = build_neg_risk_snapshot(
                        group_id,
                        group,
                        &latest_by_market,
                        now_us(),
                        cfg.brain.max_snapshot_staleness_ms,
                    )
                    {
                        let metrics = match eval_snapshot_with_strategy(
                            &cfg,
                            &neg_snap,
                            Strategy::NegRisk,
                        ) {
                            Ok(v) => v,
                            Err(e) => {
                                warn!(group_id = %group_id, error = %e, "skip neg_risk snapshot");
                                continue;
                            }
                        };

                        let legs: Vec<Leg> = neg_snap
                            .legs
                            .iter()
                            .enumerate()
                            .map(|(idx, l)| Leg {
                                leg_index: idx,
                                market_id: l.market_id.clone(),
                                token_id: l.token_id.clone(),
                                side: Side::Buy,
                                limit_price: l.best_ask,
                                qty: q_req,
                                best_bid_at_signal: l.best_bid,
                                best_ask_at_signal: l.best_ask,
                            })
                            .collect();

                        emit_signal(
                            &run_id,
                            signal_ts_ms,
                            neg_snap.market_id.clone(),
                            metrics,
                            q_req,
                            legs,
                            min_net_edge,
                            cooldown_ms,
                            &mut last_by_key,
                            &mut next_signal_id,
                            &signal_tx,
                            health.as_ref(),
                        )?;
                    }
                }
            }
        }
    }

    Ok(())
}

fn eval_snapshot(cfg: &Config, snap: &MarketSnapshot) -> anyhow::Result<EvalMetrics> {
    let strategy = match snap.legs.len() {
        2 => Strategy::Binary,
        3 => Strategy::Triangle,
        n if n >= 4 => Strategy::Multi,
        n => anyhow::bail!("unsupported legs: {n}"),
    };
    eval_snapshot_with_strategy(cfg, snap, strategy)
}

fn eval_snapshot_with_strategy(
    cfg: &Config,
    snap: &MarketSnapshot,
    strategy: Strategy,
) -> anyhow::Result<EvalMetrics> {
    let crate::buckets::BucketDecision {
        bucket,
        worst_leg_token_id,
        metrics: bucket_metrics,
        reasons,
    } = classify_bucket(snap);

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
        raw_cost_bps,
        raw_edge_bps,
        hard_fees_bps,
        risk_premium_bps,
        expected_net_bps,
        bucket_metrics,
        worst_leg_token_id,
        reasons,
    })
}

fn build_neg_risk_snapshot(
    group_id: &str,
    group: &NegRiskGroup,
    latest_by_market: &HashMap<String, MarketSnapshot>,
    now_us: u64,
    max_staleness_ms: u64,
) -> Option<MarketSnapshot> {
    if group.markets.len() < 2 {
        return None;
    }

    // Load latest legs for each market.
    let mut quotes: Vec<(String, crate::types::LegSnapshot, crate::types::LegSnapshot)> =
        Vec::with_capacity(group.markets.len());
    for m in &group.markets {
        let snap = latest_by_market.get(&m.market_id)?;
        let max_recv_us = snap.legs.iter().map(|l| l.ts_recv_us).max().unwrap_or(0);
        if max_recv_us > 0 {
            let lag_ms = now_us.saturating_sub(max_recv_us) / 1000;
            if lag_ms > max_staleness_ms {
                return None;
            }
        }
        let yes = find_leg(snap, &m.yes_token_id)?;
        let no = find_leg(snap, &m.no_token_id)?;
        if !yes.best_ask.is_finite() || yes.best_ask <= 0.0 {
            return None;
        }
        if !no.best_ask.is_finite() || no.best_ask <= 0.0 {
            return None;
        }
        quotes.push((m.market_id.clone(), yes, no));
    }

    // Pick the NO anchor with minimal cost = NO_i + sum(YES_j).
    let mut best_anchor: Option<(usize, f64)> = None;
    for (i, (_, _yes_i, no_i)) in quotes.iter().enumerate() {
        let mut cost = no_i.best_ask;
        if !cost.is_finite() || cost <= 0.0 {
            continue;
        }
        for (j, (_, yes_j, _no_j)) in quotes.iter().enumerate() {
            if i == j {
                continue;
            }
            let ask = yes_j.best_ask;
            if !ask.is_finite() || ask <= 0.0 {
                cost = f64::NAN;
                break;
            }
            cost += ask;
        }
        if !cost.is_finite() {
            continue;
        }
        if best_anchor.map_or(true, |(_, best)| cost < best) {
            best_anchor = Some((i, cost));
        }
    }

    let (anchor_idx, _) = best_anchor?;
    let anchor = &quotes[anchor_idx];

    let mut legs: Vec<crate::types::LegSnapshot> = Vec::with_capacity(quotes.len());
    legs.push(anchor.2.clone()); // NO_i

    let mut others: Vec<&(String, crate::types::LegSnapshot, crate::types::LegSnapshot)> = quotes
        .iter()
        .enumerate()
        .filter_map(|(idx, q)| if idx == anchor_idx { None } else { Some(q) })
        .collect();
    others.sort_by(|a, b| a.0.cmp(&b.0));
    for q in others {
        legs.push(q.1.clone()); // YES_j
    }

    Some(MarketSnapshot {
        market_id: group_id.to_string(),
        legs,
    })
}

fn find_leg(snap: &MarketSnapshot, token_id: &str) -> Option<crate::types::LegSnapshot> {
    snap.legs
        .iter()
        .find(|l| l.token_id == token_id)
        .cloned()
}

fn should_emit(
    now_ms: u64,
    expected_net_bps: Bps,
    min_net_edge_bps: Bps,
    cooldown_ms: u64,
    prev: Option<&LastSignalState>,
    key_cost_bps: i32,
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

    Err(SkipReason::SuppressedDuplicate {
        remaining_ms: cooldown_ms.saturating_sub(elapsed_ms),
        key_cost_bps,
    })
}

fn emit_signal(
    run_id: &str,
    signal_ts_ms: u64,
    market_id: String,
    metrics: EvalMetrics,
    q_req: f64,
    legs: Vec<Leg>,
    min_net_edge: Bps,
    cooldown_ms: u64,
    last_by_key: &mut HashMap<(String, Strategy, i32), LastSignalState>,
    next_signal_id: &mut u64,
    signal_tx: &mpsc::Sender<Signal>,
    health: &HealthCounters,
) -> anyhow::Result<()> {
    let rounded_cost_bps = (metrics.raw_cost_bps.raw() / 2) * 2;
    let key = (market_id.clone(), metrics.strategy, rounded_cost_bps);

    if let Err(reason) = should_emit(
        signal_ts_ms,
        metrics.expected_net_bps,
        min_net_edge,
        cooldown_ms,
        last_by_key.get(&key),
        rounded_cost_bps,
    ) {
        match reason {
            SkipReason::BelowMinEdge => {
                debug!(
                    market_id = %market_id,
                    expected_net_bps = metrics.expected_net_bps.raw(),
                    min_net_edge_bps = min_net_edge.raw(),
                    "skip: below min net edge"
                );
            }
            SkipReason::SuppressedDuplicate {
                remaining_ms,
                key_cost_bps,
            } => {
                health.inc_signals_suppressed(1);
                debug!(
                    market_id = %market_id,
                    remaining_ms,
                    expected_net_bps = metrics.expected_net_bps.raw(),
                    key_cost_bps,
                    "skip: suppressed duplicate"
                );
            }
        }
        return Ok(());
    }

    let signal_id = *next_signal_id;
    *next_signal_id = next_signal_id.saturating_add(1);

    let signal = Signal {
        run_id: run_id.to_string(),
        signal_id,
        signal_ts_ms,
        market_id: market_id.clone(),
        strategy: metrics.strategy,
        bucket: metrics.bucket,
        reasons: metrics.reasons.clone(),
        q_req,
        raw_cost_bps: metrics.raw_cost_bps,
        raw_edge_bps: metrics.raw_edge_bps,
        hard_fees_bps: metrics.hard_fees_bps,
        risk_premium_bps: metrics.risk_premium_bps,
        expected_net_bps: metrics.expected_net_bps,
        bucket_metrics: metrics.bucket_metrics.clone(),
        legs,
    };

    last_by_key.insert(
        key,
        LastSignalState {
            ts_ms: signal_ts_ms,
            _expected_net_bps: metrics.expected_net_bps,
        },
    );

    match signal_tx.try_send(signal) {
        Ok(()) => {
            health.inc_signals_emitted(1);
            info!(
                signal_id,
                market_id = %market_id,
                bucket = %metrics.bucket.as_str(),
                strategy = %metrics.strategy.as_str(),
                worst_leg_token_id = %metrics.worst_leg_token_id,
                raw_cost_bps = metrics.raw_cost_bps.raw(),
                expected_net_bps = metrics.expected_net_bps.raw(),
                q_req,
                "signal"
            );
        }
        Err(tokio::sync::mpsc::error::TrySendError::Full(s)) => {
            health.inc_signals_dropped(1);
            warn!(
                signal_id = s.signal_id,
                market_id = %s.market_id,
                bucket = %s.bucket.as_str(),
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

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        BrainConfig, BucketConfig, CalibrationConfig, Config, LiveConfig, MarketSelectConfig,
        PolymarketConfig, ReportConfig, RunConfig, ShadowConfig, SimConfig,
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
                snapshot_log_interval_ms: 1_000,
                raw_ws_rotate_keep: 0,
            },
            schema_version: crate::schema::SCHEMA_VERSION.to_string(),
            brain: BrainConfig {
                risk_premium_bps: 80,
                min_net_edge_bps: 10,
                q_req: 10.0,
                signal_cooldown_ms: 0,
                max_snapshot_staleness_ms: 500,
            },
            buckets: BucketConfig::default(),
            shadow: ShadowConfig::default(),
            market_select: MarketSelectConfig::default(),
            report: ReportConfig::default(),
            live: LiveConfig::default(),
            calibration: CalibrationConfig::default(),
            sim: SimConfig::default(),
        };

        let snap = MarketSnapshot {
            market_id: "0xdeadbeef".to_string(),
            legs: vec![
                LegSnapshot {
                    market_id: "0xdeadbeef".to_string(),
                    token_id: "a".to_string(),
                    best_ask: 0.48,
                    best_bid: 0.4796,
                    best_ask_size_best: 0.0,
                    best_bid_size_best: 0.0,
                    ask_depth3_usdc: 1000.0,
                    ts_recv_us: 1,
                },
                LegSnapshot {
                    market_id: "0xdeadbeef".to_string(),
                    token_id: "b".to_string(),
                    best_ask: 0.49,
                    best_bid: 0.4896,
                    best_ask_size_best: 0.0,
                    best_bid_size_best: 0.0,
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
        assert_eq!(metrics.bucket_metrics.worst_leg_index, 0);
    }

    #[test]
    fn test_filter_min_net_edge() {
        let now_ms = 1_000;
        let min_edge = Bps::new(11);
        let expected = Bps::new(10);
        assert!(should_emit(now_ms, expected, min_edge, 1_000, None, 9_700).is_err());
    }

    #[test]
    fn test_duplicate_suppressed_within_cooldown() {
        let prev = LastSignalState {
            ts_ms: 1_000,
            _expected_net_bps: Bps::new(50),
        };
        let now_ms = 1_500; // within cooldown=1_000 (elapsed 500)
        let min_edge = Bps::new(-10_000);
        let cooldown_ms = 1_000;

        let expected = Bps::new(10);
        let err =
            should_emit(now_ms, expected, min_edge, cooldown_ms, Some(&prev), 9_700).unwrap_err();
        assert!(matches!(err, SkipReason::SuppressedDuplicate { .. }));
    }

    #[test]
    fn test_emit_after_cooldown() {
        let prev = LastSignalState {
            ts_ms: 1_000,
            _expected_net_bps: Bps::new(50),
        };
        let now_ms = 2_100; // elapsed 1100 >= cooldown 1000
        let min_edge = Bps::new(-10_000);
        let cooldown_ms = 1_000;

        let expected = Bps::new(10);
        assert!(should_emit(now_ms, expected, min_edge, cooldown_ms, Some(&prev), 9_700).is_ok());
    }

    #[test]
    fn sum_asks_ge_one_is_non_signal_path() {
        let cfg = Config {
            polymarket: PolymarketConfig::default(),
            run: RunConfig {
                data_dir: "data".into(),
                market_ids: vec![],
                snapshot_log_interval_ms: 1_000,
                raw_ws_rotate_keep: 0,
            },
            schema_version: crate::schema::SCHEMA_VERSION.to_string(),
            brain: BrainConfig {
                risk_premium_bps: 80,
                min_net_edge_bps: 10,
                q_req: 10.0,
                signal_cooldown_ms: 0,
                max_snapshot_staleness_ms: 500,
            },
            buckets: BucketConfig::default(),
            shadow: ShadowConfig::default(),
            market_select: MarketSelectConfig::default(),
            report: ReportConfig::default(),
            live: LiveConfig::default(),
            calibration: CalibrationConfig::default(),
            sim: SimConfig::default(),
        };

        let snap = MarketSnapshot {
            market_id: "m".to_string(),
            legs: vec![
                LegSnapshot {
                    market_id: "m".to_string(),
                    token_id: "a".to_string(),
                    best_ask: 0.6,
                    best_bid: 0.5992,
                    best_ask_size_best: 0.0,
                    best_bid_size_best: 0.0,
                    ask_depth3_usdc: 1_000.0,
                    ts_recv_us: 0,
                },
                LegSnapshot {
                    market_id: "m".to_string(),
                    token_id: "b".to_string(),
                    best_ask: 0.6,
                    best_bid: 0.5992,
                    best_ask_size_best: 0.0,
                    best_bid_size_best: 0.0,
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
