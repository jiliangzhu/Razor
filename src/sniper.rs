use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch, Mutex};
use tracing::{debug, error, info, warn};

use crate::calibration::CalibrationEvent;
use crate::config::Config;
use crate::execution::{top_of_book, ExecKind, ExecutionGateway, PlaceIocRequest, TopOfBook};
use crate::recorder::CsvAppender;
use crate::schema::TRADE_LOG_HEADER;
use crate::types::{now_ms, Bps, FillReport, FillStatus, MarketSnapshot, Side, Signal};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OmsAction {
    FireLeg1,
    Chase,
    Flatten,
    HardStop,
    Cooldown,
    DedupHit,
}

impl OmsAction {
    fn as_str(self) -> &'static str {
        match self {
            OmsAction::FireLeg1 => "FIRE_LEG1",
            OmsAction::Chase => "CHASE",
            OmsAction::Flatten => "FLATTEN",
            OmsAction::HardStop => "HARDSTOP",
            OmsAction::Cooldown => "COOLDOWN",
            OmsAction::DedupHit => "DEDUP_HIT",
        }
    }

    fn exec_kind(self) -> Option<ExecKind> {
        match self {
            OmsAction::FireLeg1 => Some(ExecKind::FireLeg1),
            OmsAction::Chase => Some(ExecKind::Chase),
            OmsAction::Flatten => Some(ExecKind::Flatten),
            OmsAction::HardStop | OmsAction::Cooldown | OmsAction::DedupHit => None,
        }
    }
}

#[derive(Debug)]
enum OmsState {
    Idle,
    HardStop { reason: String },
}

#[derive(Debug, Clone)]
struct PositionChunk {
    token_id: String,
    qty: f64,
}

pub async fn run(
    cfg: Config,
    mut snap_rx: watch::Receiver<Option<MarketSnapshot>>,
    mut signal_rx: mpsc::Receiver<Signal>,
    trade_log_path: PathBuf,
    calibration_tx: mpsc::Sender<CalibrationEvent>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut trade_log = CsvAppender::open(trade_log_path, &TRADE_LOG_HEADER)?;

    let snapshots: Arc<Mutex<HashMap<String, MarketSnapshot>>> =
        Arc::new(Mutex::new(HashMap::new()));
    spawn_snapshot_ingest(&mut snap_rx, Arc::clone(&snapshots));

    let force_chase_fail = env_flag("RAZOR_SIM_FORCE_CHASE_FAIL");
    if force_chase_fail {
        warn!("RAZOR_SIM_FORCE_CHASE_FAIL=1 enabled: all CHASE orders will fill NONE");
    }
    let exec = if cfg.live.enabled {
        info!("LIVE mode enabled: deriving API creds (orders not implemented yet)");
        ExecutionGateway::new_live(&cfg).await?
    } else {
        ExecutionGateway::new_sim(&cfg, force_chase_fail)
    };

    info!(
        enabled = cfg.live.enabled,
        cooldown_ms = cfg.live.cooldown_ms,
        chase_cap_bps = cfg.live.chase_cap_bps,
        ladder_step1_bps = cfg.live.ladder_step1_bps,
        "sniper start (SIM)"
    );

    let mut state = OmsState::Idle;
    let mut hardstop_heartbeat = tokio::time::interval(Duration::from_secs(5));
    hardstop_heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut cooldown_by_market: HashMap<String, u64> = HashMap::new();
    let mut seen_signal_ids: HashMap<u64, u64> = HashMap::new();
    let mut last_prune_ms: u64 = 0;
    const PRUNE_EVERY_MS: u64 = 60_000;
    const TTL_MS: u64 = 60 * 60_000;

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() { break; }
            }
            _ = hardstop_heartbeat.tick() => {
                if let OmsState::HardStop{ reason } = &state {
                    warn!(%reason, "sniper HARDSTOP (heartbeat)");
                }
            }
            maybe = signal_rx.recv() => {
                let Some(signal) = maybe else { break; };

                if *shutdown.borrow() {
                    break;
                }

                let now = now_ms();

                match &state {
                    OmsState::HardStop{ reason } => {
                        warn!(signal_id = signal.signal_id, %reason, "hardstop; ignoring signal");
                        continue;
                    }
                    OmsState::Idle => {}
                }

                if let Some(until_ms) = cooldown_by_market.get(&signal.market_id).copied() {
                    if now < until_ms {
                        write_trade_row(
                            &mut trade_log,
                            &signal,
                            OmsAction::Cooldown,
                            -1,
                            "",
                            Side::Buy,
                            0.0,
                            0.0,
                            0.0,
                            FillStatus::None,
                            &format!("cooldown_until_ms={until_ms}"),
                        )?;
                        continue;
                    }
                    cooldown_by_market.remove(&signal.market_id);
                }

                if let Some(prev_ts_ms) = seen_signal_ids.get(&signal.signal_id).copied() {
                    write_trade_row(
                        &mut trade_log,
                        &signal,
                        OmsAction::DedupHit,
                        -1,
                        "",
                        Side::Buy,
                        0.0,
                        0.0,
                        0.0,
                        FillStatus::None,
                        &format!("dedup_prev_ts_ms={prev_ts_ms}"),
                    )?;
                    continue;
                }

                let outcome = process_signal_sim(
                    &cfg,
                    &signal,
                    &snapshots,
                    &mut trade_log,
                    &calibration_tx,
                    &exec,
                ).await;

                seen_signal_ids.insert(signal.signal_id, now);
                if now.saturating_sub(last_prune_ms) >= PRUNE_EVERY_MS {
                    last_prune_ms = now;
                    let cutoff = now.saturating_sub(TTL_MS);
                    seen_signal_ids.retain(|_, ts| *ts >= cutoff);
                }

                match outcome {
                    SignalOutcome::Completed => {
                        let until_ms = now_ms().saturating_add(cfg.live.cooldown_ms);
                        write_trade_row(
                            &mut trade_log,
                            &signal,
                            OmsAction::Cooldown,
                            -1,
                            "",
                            Side::Buy,
                            0.0,
                            0.0,
                            0.0,
                            FillStatus::None,
                            &format!("until_ms={until_ms}"),
                        )?;
                        cooldown_by_market.insert(signal.market_id.clone(), until_ms);
                    }
                    SignalOutcome::HardStop { reason } => {
                        write_trade_row(
                            &mut trade_log,
                            &signal,
                            OmsAction::HardStop,
                            -1,
                            "",
                            Side::Sell,
                            0.0,
                            0.0,
                            0.0,
                            FillStatus::None,
                            &reason,
                        )?;
                        error!(signal_id = signal.signal_id, %reason, "sniper entered HARDSTOP");
                        state = OmsState::HardStop{ reason };
                    }
                }
            }
        }
    }

    trade_log.flush_and_sync()?;
    Ok(())
}

enum SignalOutcome {
    Completed,
    HardStop { reason: String },
}

async fn process_signal_sim(
    cfg: &Config,
    signal: &Signal,
    snapshots: &Arc<Mutex<HashMap<String, MarketSnapshot>>>,
    trade_log: &mut CsvAppender,
    calibration_tx: &mpsc::Sender<CalibrationEvent>,
    exec: &ExecutionGateway,
) -> SignalOutcome {
    info!(
        signal_id = signal.signal_id,
        market_id = %signal.market_id,
        strategy = %signal.strategy.as_str(),
        bucket = %signal.bucket.as_str(),
        expected_net_bps = signal.expected_net_bps.raw(),
        "sniper signal (SIM)"
    );

    let Some(snap) = latest_market_snapshot(snapshots, &signal.market_id).await else {
        warn!(signal_id = signal.signal_id, market_id = %signal.market_id, "no snapshot; skip");
        let _ = write_trade_row(
            trade_log,
            signal,
            OmsAction::FireLeg1,
            -1,
            "",
            Side::Buy,
            0.0,
            signal.q_req,
            0.0,
            FillStatus::None,
            "no_snapshot",
        );
        return SignalOutcome::Completed;
    };

    if signal.legs.is_empty() {
        return SignalOutcome::Completed;
    }

    let mut leg_idxs: Vec<usize> = (0..signal.legs.len()).collect();

    // Prefer the worst-leg anchor computed by Brain (auditable and deterministic).
    // Fallback to live snapshot depth3 ordering if the index is out of range.
    let leg1_idx = if signal.bucket_metrics.worst_leg_index < signal.legs.len() {
        let worst = signal.bucket_metrics.worst_leg_index;
        leg_idxs.retain(|&i| i != worst);
        leg_idxs.sort_unstable();
        leg_idxs.insert(0, worst);
        worst
    } else {
        leg_idxs.sort_by(|&a, &b| {
            let da = depth3_for_token(&snap, &signal.legs[a].token_id);
            let db = depth3_for_token(&snap, &signal.legs[b].token_id);
            da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
        });
        *leg_idxs.first().unwrap_or(&0)
    };

    let Some(top1) = top_of_book(&snap, &signal.legs[leg1_idx].token_id) else {
        warn!(
            signal_id = signal.signal_id,
            "leg1 token missing in snapshot; skip"
        );
        let _ = write_trade_row(
            trade_log,
            signal,
            OmsAction::FireLeg1,
            leg1_idx as i32,
            &signal.legs[leg1_idx].token_id,
            Side::Buy,
            0.0,
            signal.q_req,
            0.0,
            FillStatus::None,
            "no_leg_snapshot",
        );
        return SignalOutcome::Completed;
    };

    // Leg1 IOC buy at current best_ask.
    let limit_price = top1.best_ask;
    let leg1_req = signal
        .legs
        .get(leg1_idx)
        .and_then(|l| l.qty.is_finite().then_some(l.qty))
        .filter(|q| *q > 0.0)
        .unwrap_or(signal.q_req);
    let leg1_side = signal
        .legs
        .get(leg1_idx)
        .map(|l| l.side)
        .unwrap_or(Side::Buy);
    let leg1_fill = match simulate_ioc_and_log(
        cfg,
        signal,
        trade_log,
        calibration_tx,
        exec,
        OmsAction::FireLeg1,
        leg1_idx as i32,
        &signal.legs[leg1_idx].token_id,
        leg1_side,
        limit_price,
        leg1_req,
        "attempt=1|leg1",
        top1,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => return SignalOutcome::HardStop { reason: e },
    };

    if leg1_fill.status == FillStatus::None || leg1_fill.filled_qty <= 0.0 {
        return SignalOutcome::Completed;
    }

    debug!(
        signal_id = signal.signal_id,
        leg_index = leg1_idx,
        token_id = %signal.legs[leg1_idx].token_id,
        requested_qty = leg1_fill.requested_qty,
        filled_qty = leg1_fill.filled_qty,
        avg_price = leg1_fill.avg_price,
        "leg1 fill"
    );

    let target_qty = leg1_fill.filled_qty.min(leg1_fill.requested_qty);
    let mut positions: Vec<PositionChunk> = vec![PositionChunk {
        token_id: signal.legs[leg1_idx].token_id.clone(),
        qty: target_qty,
    }];

    let max_chase_bps = max_chase_bps(cfg, signal.expected_net_bps);
    if signal.expected_net_bps.raw() < 0 || max_chase_bps.raw() <= 0 {
        return flatten_positions(
            cfg,
            signal,
            snapshots,
            trade_log,
            calibration_tx,
            exec,
            positions,
        )
        .await;
    }

    for &idx in &leg_idxs[1..] {
        let token_id = &signal.legs[idx].token_id;
        let Some(top) = top_of_book(&snap, token_id) else {
            warn!(signal_id = signal.signal_id, %token_id, "token missing in snapshot; flatten");
            return flatten_positions(
                cfg,
                signal,
                snapshots,
                trade_log,
                calibration_tx,
                exec,
                positions,
            )
            .await;
        };

        let step1_bps = Bps::new(cfg.live.ladder_step1_bps);
        let p1 = top.best_ask * (1.0 + step1_bps.to_f64());
        let p2 = top.best_ask * (1.0 + max_chase_bps.to_f64());

        let mut filled = 0.0f64;
        for (attempt, px) in [(1, p1), (2, p2)] {
            if filled + 1e-12 >= target_qty {
                break;
            }
            let need = (target_qty - filled).max(0.0);
            let notes = if attempt == 1 {
                format!("ladder_step1_bps={}", step1_bps.raw())
            } else {
                format!("max_chase_bps={}", max_chase_bps.raw())
            };
            let notes = format!("attempt={attempt}|{notes}");

            let r = match simulate_ioc_and_log(
                cfg,
                signal,
                trade_log,
                calibration_tx,
                exec,
                OmsAction::Chase,
                idx as i32,
                token_id,
                signal.legs[idx].side,
                px,
                need,
                &notes,
                top,
            )
            .await
            {
                Ok(r) => r,
                Err(e) => return SignalOutcome::HardStop { reason: e },
            };

            filled += r.filled_qty;
        }

        if filled + 1e-9 < target_qty {
            warn!(
                signal_id = signal.signal_id,
                leg_index = idx,
                %token_id,
                filled,
                target_qty,
                "legging failed; flatten"
            );
            if filled > 0.0 {
                positions.push(PositionChunk {
                    token_id: token_id.clone(),
                    qty: filled,
                });
            }
            return flatten_positions(
                cfg,
                signal,
                snapshots,
                trade_log,
                calibration_tx,
                exec,
                positions,
            )
            .await;
        }

        positions.push(PositionChunk {
            token_id: token_id.clone(),
            qty: target_qty,
        });
    }

    SignalOutcome::Completed
}

async fn flatten_positions(
    cfg: &Config,
    signal: &Signal,
    snapshots: &Arc<Mutex<HashMap<String, MarketSnapshot>>>,
    trade_log: &mut CsvAppender,
    calibration_tx: &mpsc::Sender<CalibrationEvent>,
    exec: &ExecutionGateway,
    mut positions: Vec<PositionChunk>,
) -> SignalOutcome {
    positions.retain(|p| p.qty.is_finite() && p.qty > 0.0 && !p.token_id.is_empty());
    if positions.is_empty() {
        return SignalOutcome::Completed;
    }

    let lvls: [Bps; 3] = [
        Bps::new(cfg.live.flatten_lvl1_bps),
        Bps::new(cfg.live.flatten_lvl2_bps),
        Bps::new(cfg.live.flatten_lvl3_bps),
    ];

    let max_attempts = cfg.live.flatten_max_attempts.max(1) as usize;
    let mut attempts_done = 0usize;

    while attempts_done < max_attempts {
        let lvl = lvls.get(attempts_done).copied().unwrap_or_else(|| lvls[2]);
        attempts_done += 1;

        let Some(snap) = latest_market_snapshot(snapshots, &signal.market_id).await else {
            return SignalOutcome::HardStop {
                reason: "flatten_failed:no_snapshot".to_string(),
            };
        };

        let mut still: Vec<PositionChunk> = Vec::new();
        for p in positions {
            let Some(top) = top_of_book(&snap, &p.token_id) else {
                still.push(p);
                continue;
            };
            let limit_price = top.best_bid * (1.0 - lvl.to_f64());
            let notes = format!("flatten_lvl_bps={}", lvl.raw());
            let notes = format!("attempt={attempts_done}|{notes}");

            let r = match simulate_ioc_and_log(
                cfg,
                signal,
                trade_log,
                calibration_tx,
                exec,
                OmsAction::Flatten,
                -1,
                &p.token_id,
                Side::Sell,
                limit_price,
                p.qty,
                &notes,
                top,
            )
            .await
            {
                Ok(r) => r,
                Err(e) => return SignalOutcome::HardStop { reason: e },
            };

            let remaining = (p.qty - r.filled_qty).max(0.0);
            if remaining > 1e-12 {
                still.push(PositionChunk {
                    token_id: p.token_id,
                    qty: remaining,
                });
            }
        }

        positions = still;
        if positions.is_empty() {
            return SignalOutcome::Completed;
        }
    }

    SignalOutcome::HardStop {
        reason: "flatten_failed".to_string(),
    }
}

#[allow(clippy::too_many_arguments)]
async fn simulate_ioc_and_log(
    _cfg: &Config,
    signal: &Signal,
    trade_log: &mut CsvAppender,
    calibration_tx: &mpsc::Sender<CalibrationEvent>,
    exec: &ExecutionGateway,
    action: OmsAction,
    leg_index: i32,
    token_id: &str,
    side: Side,
    limit_price: f64,
    req_qty: f64,
    notes: &str,
    top: TopOfBook,
) -> Result<FillReport, String> {
    let kind = action
        .exec_kind()
        .ok_or_else(|| "not an executable action".to_string())?;

    let exec_res = exec
        .place_ioc(PlaceIocRequest {
            kind,
            bucket: signal.bucket,
            token_id,
            side,
            limit_price,
            req_qty,
            top,
        })
        .await
        .map_err(|e| format!("exec error: {e:#}"))?;

    let report = exec_res.fill;
    let full_notes = format!(
        "{notes}|order_id={}|latency_ms={}|spike_ms={}|book_dropped={}|sim_fill_share_used={}",
        &report.order_id,
        report.latency_ms,
        exec_res.latency_spike_ms_applied,
        exec_res.book_dropped,
        exec_res.sim_fill_share_used
    );

    write_trade_row(
        trade_log,
        signal,
        action,
        leg_index,
        token_id,
        side,
        limit_price,
        req_qty,
        report.filled_qty,
        report.status,
        &full_notes,
    )
    .map_err(|e| format!("trade_log write failed: {e:#}"))?;

    let ev = CalibrationEvent {
        ts_ms: now_ms(),
        bucket: signal.bucket,
        market_id: signal.market_id.clone(),
        token_id: token_id.to_string(),
        side,
        req_qty,
        filled_qty: report.filled_qty,
        market_ask_size_best: exec_res.top.best_ask_size_best,
        market_bid_size_best: exec_res.top.best_bid_size_best,
        sim_fill_share_used: exec_res.sim_fill_share_used,
        mode: "SIM".to_string(),
    };
    if calibration_tx.try_send(ev).is_err() {
        warn!(
            signal_id = signal.signal_id,
            "calibration channel full/closed; dropped event"
        );
    }

    Ok(report)
}

fn max_chase_bps(cfg: &Config, expected_net_bps: Bps) -> Bps {
    let half = expected_net_bps.raw() / 2;
    let capped = half.clamp(0, cfg.live.chase_cap_bps);
    Bps::new(capped)
}

#[allow(clippy::too_many_arguments)]
fn write_trade_row(
    out: &mut CsvAppender,
    signal: &Signal,
    action: OmsAction,
    leg_index: i32,
    token_id: &str,
    side: Side,
    limit_price: f64,
    req_qty: f64,
    fill_qty: f64,
    fill_status: FillStatus,
    notes: &str,
) -> anyhow::Result<()> {
    out.write_record([
        now_ms().to_string(),
        signal.signal_id.to_string(),
        signal.market_id.clone(),
        signal.strategy.as_str().to_string(),
        signal.bucket.as_str().to_string(),
        "SIM".to_string(),
        action.as_str().to_string(),
        leg_index.to_string(),
        token_id.to_string(),
        side.as_str().to_string(),
        limit_price.to_string(),
        req_qty.to_string(),
        fill_qty.to_string(),
        fill_status.as_str().to_string(),
        signal.expected_net_bps.raw().to_string(),
        notes.to_string(),
    ])
}

async fn latest_market_snapshot(
    snapshots: &Arc<Mutex<HashMap<String, MarketSnapshot>>>,
    market_id: &str,
) -> Option<MarketSnapshot> {
    let map = snapshots.lock().await;
    map.get(market_id).cloned()
}

fn depth3_for_token(snap: &MarketSnapshot, token_id: &str) -> f64 {
    snap.legs
        .iter()
        .find(|l| l.token_id == token_id)
        .map(|l| l.ask_depth3_usdc)
        .filter(|d| d.is_finite() && *d >= 0.0)
        .unwrap_or(f64::INFINITY)
}

fn spawn_snapshot_ingest(
    snap_rx: &mut watch::Receiver<Option<MarketSnapshot>>,
    snapshots: Arc<Mutex<HashMap<String, MarketSnapshot>>>,
) {
    let mut snap_rx = snap_rx.clone();
    tokio::spawn(async move {
        loop {
            if snap_rx.changed().await.is_err() {
                break;
            }
            let snap = snap_rx.borrow().clone();
            if let Some(s) = snap {
                let mut map = snapshots.lock().await;
                map.insert(s.market_id.clone(), s);
            }
        }
    });
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| {
            let v = v.trim().to_ascii_lowercase();
            v == "1" || v == "true" || v == "yes" || v == "y"
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_chase_is_half_capped_by_config() {
        let cfg = Config {
            polymarket: crate::config::PolymarketConfig::default(),
            run: crate::config::RunConfig {
                data_dir: "data".into(),
                market_ids: vec![],
                snapshot_log_interval_ms: 1_000,
                raw_ws_rotate_keep: 0,
            },
            schema_version: crate::schema::SCHEMA_VERSION.to_string(),
            brain: crate::config::BrainConfig::default(),
            buckets: crate::config::BucketConfig::default(),
            shadow: crate::config::ShadowConfig::default(),
            market_select: crate::config::MarketSelectConfig::default(),
            report: crate::config::ReportConfig::default(),
            live: crate::config::LiveConfig {
                enabled: false,
                chain_id: 137,
                private_key_env: "POLYGON_PRIVATE_KEY".to_string(),
                api_key_nonce: 0,
                chase_cap_bps: 200,
                ladder_step1_bps: 10,
                flatten_lvl1_bps: 100,
                flatten_lvl2_bps: 500,
                flatten_lvl3_bps: 1000,
                flatten_max_attempts: 3,
                cooldown_ms: 1000,
            },
            calibration: crate::config::CalibrationConfig::default(),
            sim: crate::config::SimConfig::default(),
        };

        assert_eq!(max_chase_bps(&cfg, Bps::new(10)).raw(), 5);
        assert_eq!(max_chase_bps(&cfg, Bps::new(401)).raw(), 200);
        assert_eq!(max_chase_bps(&cfg, Bps::new(-10)).raw(), 0);
    }
}
