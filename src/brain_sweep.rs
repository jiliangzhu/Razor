use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Context as _;

use crate::buckets::{classify_bucket, fill_share_p25};
use crate::config::Config;
use crate::schema::{
    FILE_RUN_CONFIG, FILE_SNAPSHOTS, FILE_TRADES, SNAPSHOTS_HEADER, TRADES_HEADER,
};
use crate::types::{Bps, LegSnapshot, MarketSnapshot, Signal, SignalLeg, Strategy, TradeTick};

pub const FILE_BRAIN_SWEEP_SCORES: &str = "brain_sweep_scores.csv";
pub const FILE_BEST_BRAIN_PATCH: &str = "best_brain_patch.toml";

pub const BRAIN_SWEEP_SCORES_HEADER: [&str; 12] = [
    "base_run_id",
    "signals_total",
    "signals_ok",
    "signals_bad",
    "min_net_edge_bps",
    "risk_premium_bps",
    "signal_cooldown_ms",
    "total_pnl_sum",
    "total_pnl_avg",
    "avg_set_ratio",
    "legging_rate",
    "worst_20_pnl_sum",
];

const GRID_MIN_NET_EDGE_BPS: [i32; 4] = [10, 20, 30, 40];
const GRID_RISK_PREMIUM_BPS: [i32; 3] = [60, 80, 100];
const GRID_SIGNAL_COOLDOWN_MS: [u64; 3] = [500, 1000, 2000];

#[derive(Debug, Clone)]
pub struct BrainSweepResult {
    pub run_dir: PathBuf,
    pub out_dir: PathBuf,
    pub base_run_id: String,
    pub rows: Vec<BrainSweepScoreRow>,
    pub best: Option<BrainSweepScoreRow>,
}

#[derive(Debug, Clone)]
pub struct BrainSweepScoreRow {
    pub base_run_id: String,
    pub signals_total: u64,
    pub signals_ok: u64,
    pub signals_bad: u64,
    pub min_net_edge_bps: i32,
    pub risk_premium_bps: i32,
    pub signal_cooldown_ms: u64,
    pub total_pnl_sum: f64,
    pub total_pnl_avg: f64,
    pub avg_set_ratio: f64,
    pub legging_rate: f64,
    pub worst_20_pnl_sum: f64,
}

#[derive(Debug, Clone)]
struct TimedSnapshot {
    ts_ms: u64,
    snapshot: MarketSnapshot,
}

#[derive(Debug, Clone, Copy)]
struct TradeLite {
    ts_ms: u64,
    price: f64,
    size: f64,
}

pub fn run_brain_sweep(run_dir: &Path, out_dir: &Path) -> anyhow::Result<BrainSweepResult> {
    std::fs::create_dir_all(out_dir).with_context(|| format!("create {}", out_dir.display()))?;

    let cfg_raw = std::fs::read_to_string(run_dir.join(FILE_RUN_CONFIG))
        .context("read run config snapshot")?;
    let cfg_base: Config = toml::from_str(&cfg_raw).context("parse run config snapshot")?;

    let base_run_id = crate::run_meta::RunMeta::read_from_dir(run_dir)
        .map(|m| m.run_id)
        .unwrap_or_else(|_| "unknown".to_string());

    let snapshots = read_snapshots_csv(&run_dir.join(FILE_SNAPSHOTS)).context("read snapshots")?;
    let trades_by_key = read_trades_by_key(&run_dir.join(FILE_TRADES)).context("read trades")?;

    let mut rows: Vec<BrainSweepScoreRow> = Vec::new();

    for min_net_edge_bps in GRID_MIN_NET_EDGE_BPS {
        for risk_premium_bps in GRID_RISK_PREMIUM_BPS {
            for signal_cooldown_ms in GRID_SIGNAL_COOLDOWN_MS {
                let mut cfg = cfg_base.clone();
                cfg.brain.min_net_edge_bps = min_net_edge_bps;
                cfg.brain.risk_premium_bps = risk_premium_bps;
                cfg.brain.signal_cooldown_ms = signal_cooldown_ms;

                let signals = generate_signals(&cfg, "brain_sweep", &snapshots);
                let score = score_signals(
                    &cfg,
                    &base_run_id,
                    min_net_edge_bps,
                    risk_premium_bps,
                    signal_cooldown_ms,
                    &signals,
                    &trades_by_key,
                );
                rows.push(score);
            }
        }
    }

    // Deterministic ordering in CSV: keep the frozen grid order.
    let scores_path = out_dir.join(FILE_BRAIN_SWEEP_SCORES);
    write_scores_csv(&scores_path, &rows).context("write brain_sweep_scores.csv")?;

    let best = pick_best(&rows);
    let patch_path = out_dir.join(FILE_BEST_BRAIN_PATCH);
    write_best_patch(&patch_path, best.as_ref()).context("write best_brain_patch.toml")?;

    Ok(BrainSweepResult {
        run_dir: run_dir.to_path_buf(),
        out_dir: out_dir.to_path_buf(),
        base_run_id,
        rows,
        best,
    })
}

fn write_scores_csv(path: &Path, rows: &[BrainSweepScoreRow]) -> anyhow::Result<()> {
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(false)
        .from_path(path)
        .with_context(|| format!("open {}", path.display()))?;

    wtr.write_record(BRAIN_SWEEP_SCORES_HEADER)
        .context("write header")?;

    for r in rows {
        wtr.write_record([
            r.base_run_id.clone(),
            r.signals_total.to_string(),
            r.signals_ok.to_string(),
            r.signals_bad.to_string(),
            r.min_net_edge_bps.to_string(),
            r.risk_premium_bps.to_string(),
            r.signal_cooldown_ms.to_string(),
            r.total_pnl_sum.to_string(),
            r.total_pnl_avg.to_string(),
            r.avg_set_ratio.to_string(),
            r.legging_rate.to_string(),
            r.worst_20_pnl_sum.to_string(),
        ])
        .context("write row")?;
    }

    wtr.flush().context("flush brain_sweep_scores.csv")?;
    Ok(())
}

fn write_best_patch(path: &Path, best: Option<&BrainSweepScoreRow>) -> anyhow::Result<()> {
    if let Some(b) = best {
        let toml = format!(
            "[brain]\nmin_net_edge_bps = {}\nrisk_premium_bps = {}\nsignal_cooldown_ms = {}\n",
            b.min_net_edge_bps, b.risk_premium_bps, b.signal_cooldown_ms
        );
        std::fs::write(path, toml).with_context(|| format!("write {}", path.display()))?;
        return Ok(());
    }

    let toml = "[brain]\n# insufficient_data=true\n".to_string();
    std::fs::write(path, toml).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

fn pick_best(rows: &[BrainSweepScoreRow]) -> Option<BrainSweepScoreRow> {
    rows.iter()
        .filter(|r| r.signals_ok > 0)
        .cloned()
        .max_by(compare_rows)
}

fn compare_rows(a: &BrainSweepScoreRow, b: &BrainSweepScoreRow) -> std::cmp::Ordering {
    // Best rule (frozen):
    // 1) total_pnl_sum (desc)
    // 2) legging_rate (asc)
    // 3) signals_ok (desc)
    // Tie-break (deterministic, conservative): higher thresholds first.

    cmp_f64_high_is_better(a.total_pnl_sum, b.total_pnl_sum)
        .then_with(|| cmp_f64_low_is_better(a.legging_rate, b.legging_rate))
        .then_with(|| a.signals_ok.cmp(&b.signals_ok))
        .then_with(|| a.min_net_edge_bps.cmp(&b.min_net_edge_bps))
        .then_with(|| a.risk_premium_bps.cmp(&b.risk_premium_bps))
        .then_with(|| a.signal_cooldown_ms.cmp(&b.signal_cooldown_ms))
}

fn cmp_f64_high_is_better(a: f64, b: f64) -> std::cmp::Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        (false, false) => a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal),
    }
}

fn cmp_f64_low_is_better(a: f64, b: f64) -> std::cmp::Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        (false, false) => b.partial_cmp(&a).unwrap_or(std::cmp::Ordering::Equal),
    }
}

fn score_signals(
    cfg: &Config,
    base_run_id: &str,
    min_net_edge_bps: i32,
    risk_premium_bps: i32,
    signal_cooldown_ms: u64,
    signals: &[Signal],
    trades_by_key: &HashMap<(String, String), Vec<TradeLite>>,
) -> BrainSweepScoreRow {
    let mut total_pnl_sum: f64 = 0.0;
    let mut set_ratio_sum: f64 = 0.0;
    let mut legging_fail: u64 = 0;
    let mut total_pnls: Vec<f64> = Vec::with_capacity(signals.len());

    let mut ok: u64 = 0;
    let mut bad: u64 = 0;

    for s in signals {
        match settle_one(cfg, s, trades_by_key) {
            Some((total_pnl, set_ratio)) => {
                ok += 1;
                total_pnl_sum += total_pnl;
                set_ratio_sum += set_ratio;
                if set_ratio < cfg.report.min_avg_set_ratio {
                    legging_fail += 1;
                }
                total_pnls.push(total_pnl);
            }
            None => {
                bad += 1;
            }
        }
    }

    total_pnls.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let worst_20_pnl_sum: f64 = total_pnls.iter().take(20).copied().sum();

    let total_pnl_avg = if ok > 0 {
        total_pnl_sum / (ok as f64)
    } else {
        0.0
    };
    let avg_set_ratio = if ok > 0 {
        set_ratio_sum / (ok as f64)
    } else {
        0.0
    };
    let legging_rate = if ok > 0 {
        (legging_fail as f64) / (ok as f64)
    } else {
        1.0
    };

    BrainSweepScoreRow {
        base_run_id: base_run_id.to_string(),
        signals_total: signals.len() as u64,
        signals_ok: ok,
        signals_bad: bad,
        min_net_edge_bps,
        risk_premium_bps,
        signal_cooldown_ms,
        total_pnl_sum,
        total_pnl_avg,
        avg_set_ratio,
        legging_rate,
        worst_20_pnl_sum,
    }
}

fn settle_one(
    cfg: &Config,
    s: &Signal,
    trades_by_key: &HashMap<(String, String), Vec<TradeLite>>,
) -> Option<(f64, f64)> {
    let legs_n = s.legs.len();
    if !(2..=3).contains(&legs_n) {
        return None;
    }
    if !s.q_req.is_finite() || s.q_req <= 0.0 {
        return None;
    }

    let mut legs = s.legs.clone();
    legs.sort_by_key(|l| l.leg_index);

    let window_start_ms = s.signal_ts_ms + cfg.shadow.window_start_ms;
    let window_end_ms = s.signal_ts_ms + cfg.shadow.window_end_ms;
    if window_start_ms > window_end_ms {
        return None;
    }

    let fill_share_used = fill_share_p25(s.bucket, &cfg.buckets);
    if !fill_share_used.is_finite() || fill_share_used < 0.0 {
        return None;
    }

    let mut v_mkt: [f64; 3] = [0.0, 0.0, 0.0];
    let mut q_fill: [f64; 3] = [0.0, 0.0, 0.0];
    for (i, leg) in legs.iter().take(3).enumerate() {
        if !leg.limit_price.is_finite() || leg.limit_price <= 0.0 {
            return None;
        }
        let key = (s.market_id.clone(), leg.token_id.clone());
        if let Some(trades) = trades_by_key.get(&key) {
            v_mkt[i] =
                volume_at_or_better_price(trades, window_start_ms, window_end_ms, leg.limit_price);
        }
        q_fill[i] = (v_mkt[i] * fill_share_used).min(s.q_req);
    }

    let q_set = q_fill[..legs_n]
        .iter()
        .copied()
        .fold(f64::INFINITY, f64::min)
        .min(s.q_req);
    let q_set = if q_set.is_finite() { q_set } else { 0.0 };

    let mut cost_set_per_unit: f64 = 0.0;
    for leg in legs.iter().take(legs_n) {
        cost_set_per_unit += Bps::FEE_POLY.apply_cost(leg.limit_price);
    }
    let cost_set = q_set * cost_set_per_unit;
    let proceeds_set = q_set * Bps::FEE_MERGE.apply_proceeds(1.0);
    let pnl_set = proceeds_set - cost_set;

    let dump_slippage_assumed = crate::schema::DUMP_SLIPPAGE_ASSUMED;
    let mut pnl_left_total: f64 = 0.0;
    for (i, leg) in legs.iter().take(legs_n).enumerate() {
        let q_left = q_fill[i] - q_set;
        if q_left <= 0.0 {
            continue;
        }
        let exit_price = leg.best_bid_at_signal.max(0.0) * (1.0 - dump_slippage_assumed);
        let proceeds_left_per_unit = Bps::FEE_POLY.apply_proceeds(exit_price);
        let cost_left_per_unit = Bps::FEE_POLY.apply_cost(leg.limit_price);
        pnl_left_total += q_left * (proceeds_left_per_unit - cost_left_per_unit);
    }

    let total_pnl = pnl_set + pnl_left_total;
    let q_fill_avg = q_fill[..legs_n].iter().sum::<f64>() / (legs_n as f64);
    let set_ratio = if q_fill_avg > 0.0 {
        q_set / q_fill_avg
    } else {
        0.0
    };

    Some((total_pnl, set_ratio))
}

fn volume_at_or_better_price(
    trades: &[TradeLite],
    start_ms: u64,
    end_ms: u64,
    price_limit: f64,
) -> f64 {
    if start_ms > end_ms || !price_limit.is_finite() {
        return 0.0;
    }

    let start_idx = lower_bound(trades, start_ms);
    let mut vol: f64 = 0.0;
    for t in &trades[start_idx..] {
        if t.ts_ms > end_ms {
            break;
        }
        if t.price <= price_limit {
            vol += t.size;
        }
    }
    vol
}

fn lower_bound(trades: &[TradeLite], ts_ms: u64) -> usize {
    let mut lo = 0usize;
    let mut hi = trades.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if trades[mid].ts_ms < ts_ms {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

fn generate_signals(cfg: &Config, run_id: &str, snapshots: &[TimedSnapshot]) -> Vec<Signal> {
    let mut out: Vec<Signal> = Vec::new();
    let mut next_signal_id: u64 = 1;
    let mut last_by_key: HashMap<(String, Strategy, i32), u64> = HashMap::new();

    let cooldown_ms = cfg.brain.signal_cooldown_ms;
    let min_net_edge = Bps::new(cfg.brain.min_net_edge_bps);

    for s in snapshots {
        let snap = &s.snapshot;
        let strategy = match snap.legs.len() {
            2 => Strategy::Binary,
            3 => Strategy::Triangle,
            _ => continue,
        };

        let decision = classify_bucket(snap);

        let sum_ask: f64 = snap.legs.iter().map(|l| l.best_ask).sum();
        if !sum_ask.is_finite() || sum_ask < 0.0 {
            continue;
        }

        // Cost/gating conversion uses ceil to avoid overstating edge.
        let raw_cost_bps = Bps::from_price_cost(sum_ask);
        let raw_edge_bps = Bps::ONE_HUNDRED_PERCENT - raw_cost_bps;

        let hard_fees_bps = Bps::FEE_POLY + Bps::FEE_MERGE;
        let risk_premium_bps = Bps::new(cfg.brain.risk_premium_bps);
        let expected_net_bps = raw_edge_bps - hard_fees_bps - risk_premium_bps;

        if expected_net_bps < min_net_edge {
            continue;
        }

        let rounded_cost_bps = (raw_cost_bps.raw() / 2) * 2;
        let key = (snap.market_id.clone(), strategy, rounded_cost_bps);
        if let Some(prev_ts) = last_by_key.get(&key) {
            let elapsed = s.ts_ms.saturating_sub(*prev_ts);
            if elapsed < cooldown_ms {
                continue;
            }
        }

        let q_req = cfg.brain.q_req;
        let legs: Vec<SignalLeg> = snap
            .legs
            .iter()
            .enumerate()
            .map(|(idx, l)| SignalLeg {
                leg_index: idx,
                token_id: l.token_id.clone(),
                side: crate::types::Side::Buy,
                limit_price: l.best_ask,
                qty: q_req,
                best_bid_at_signal: l.best_bid,
                best_ask_at_signal: l.best_ask,
            })
            .collect();

        out.push(Signal {
            run_id: run_id.to_string(),
            signal_id: next_signal_id,
            signal_ts_ms: s.ts_ms,
            market_id: snap.market_id.clone(),
            strategy,
            bucket: decision.bucket,
            reasons: decision.reasons.clone(),
            q_req,
            raw_cost_bps,
            raw_edge_bps,
            hard_fees_bps,
            risk_premium_bps,
            expected_net_bps,
            bucket_metrics: decision.metrics,
            legs,
        });

        last_by_key.insert(key, s.ts_ms);
        next_signal_id += 1;
    }

    out
}

fn read_snapshots_csv(path: &Path) -> anyhow::Result<Vec<TimedSnapshot>> {
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .from_path(path)
        .with_context(|| format!("open {}", path.display()))?;
    let header = rdr
        .headers()
        .with_context(|| format!("read header {}", path.display()))?
        .clone();
    if header.iter().map(|s| s.trim()).collect::<Vec<_>>() != SNAPSHOTS_HEADER {
        anyhow::bail!("snapshots.csv header mismatch (expected frozen SNAPSHOTS_HEADER)");
    }

    let mut out: Vec<TimedSnapshot> = Vec::new();
    for record in rdr.records() {
        let record = record?;
        let ts_ms = record.get(0).and_then(parse_u64).context("ts_ms")?;
        let market_id = record.get(1).unwrap_or("").trim().to_string();
        let legs_n = record.get(2).and_then(parse_u64).context("legs_n")? as usize;
        if !(2..=3).contains(&legs_n) {
            continue;
        }

        let mut legs: Vec<LegSnapshot> = Vec::with_capacity(legs_n);
        for i in 0..legs_n {
            let base = 3 + i * 4;
            let token_id = record.get(base).unwrap_or("").trim().to_string();
            if token_id.is_empty() {
                continue;
            }
            let best_bid = record.get(base + 1).and_then(parse_f64).unwrap_or(0.0);
            let best_ask = record.get(base + 2).and_then(parse_f64).unwrap_or(1.0);
            let depth3 = record.get(base + 3).and_then(parse_f64).unwrap_or(f64::NAN);
            legs.push(LegSnapshot {
                token_id,
                best_bid,
                best_ask,
                best_ask_size_best: 0.0,
                best_bid_size_best: 0.0,
                ask_depth3_usdc: depth3,
                ts_recv_us: ts_ms * 1000,
            });
        }
        if legs.len() != legs_n {
            continue;
        }

        out.push(TimedSnapshot {
            ts_ms,
            snapshot: MarketSnapshot { market_id, legs },
        });
    }
    out.sort_by_key(|s| s.ts_ms);
    Ok(out)
}

fn read_trades_by_key(path: &Path) -> anyhow::Result<HashMap<(String, String), Vec<TradeLite>>> {
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .from_path(path)
        .with_context(|| format!("open {}", path.display()))?;
    let header = rdr
        .headers()
        .with_context(|| format!("read header {}", path.display()))?
        .clone();
    if header.iter().map(|s| s.trim()).collect::<Vec<_>>() != TRADES_HEADER {
        anyhow::bail!("trades.csv header mismatch (expected frozen TRADES_HEADER)");
    }

    let mut out: HashMap<(String, String), Vec<TradeLite>> = HashMap::new();
    for record in rdr.records() {
        let record = record?;
        let tick = parse_trade_tick(&record)?;
        let ts_ms = if tick.ingest_ts_ms > 0 {
            tick.ingest_ts_ms
        } else {
            tick.ts_ms
        };
        out.entry((tick.market_id, tick.token_id))
            .or_default()
            .push(TradeLite {
                ts_ms,
                price: tick.price,
                size: tick.size,
            });
    }
    for v in out.values_mut() {
        v.sort_by_key(|t| t.ts_ms);
    }
    Ok(out)
}

fn parse_trade_tick(record: &csv::StringRecord) -> anyhow::Result<TradeTick> {
    let ts_ms = record.get(0).and_then(parse_u64).context("ts_ms")?;
    let market_id = record.get(1).unwrap_or("").trim().to_string();
    let token_id = record.get(2).unwrap_or("").trim().to_string();
    let price = record.get(3).and_then(parse_f64).context("price")?;
    let size = record.get(4).and_then(parse_f64).context("size")?;
    let trade_id = record.get(5).unwrap_or("").trim().to_string();
    let ingest_ts_ms = record.get(6).and_then(parse_u64).unwrap_or(ts_ms);
    let exchange_ts_ms = record.get(7).and_then(parse_u64);

    Ok(TradeTick {
        ts_ms,
        ingest_ts_ms,
        exchange_ts_ms,
        market_id,
        token_id,
        price,
        size,
        trade_id,
    })
}

fn parse_u64(s: &str) -> Option<u64> {
    s.trim().parse::<u64>().ok()
}

fn parse_f64(s: &str) -> Option<f64> {
    let v = s.trim().parse::<f64>().ok()?;
    if v.is_finite() {
        Some(v)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn brain_sweep_scores_header_is_frozen() {
        assert_eq!(
            BRAIN_SWEEP_SCORES_HEADER.join(","),
            "base_run_id,signals_total,signals_ok,signals_bad,min_net_edge_bps,risk_premium_bps,signal_cooldown_ms,total_pnl_sum,total_pnl_avg,avg_set_ratio,legging_rate,worst_20_pnl_sum"
        );
    }

    #[test]
    fn picks_best_patch_deterministically_on_fixture() -> anyhow::Result<()> {
        let run_dir = PathBuf::from("tests/fixtures/brain_sweep_small");
        assert!(run_dir.exists());

        let out_dir = std::env::temp_dir().join(format!(
            "razor_brain_sweep_test_{}_{}",
            std::process::id(),
            crate::types::now_ms()
        ));
        let _ = std::fs::remove_dir_all(&out_dir);

        let res = run_brain_sweep(&run_dir, &out_dir)?;
        assert!(out_dir.join(FILE_BRAIN_SWEEP_SCORES).exists());
        assert!(out_dir.join(FILE_BEST_BRAIN_PATCH).exists());
        assert!(res.best.is_some());

        // This fixture is designed so that the best run is the most conservative
        // configuration that drops the first (lossy) signal while keeping the second.
        let patch = std::fs::read_to_string(out_dir.join(FILE_BEST_BRAIN_PATCH))?;
        assert!(patch.contains("min_net_edge_bps = 40"));
        assert!(patch.contains("risk_premium_bps = 100"));
        assert!(patch.contains("signal_cooldown_ms = 2000"));

        let _ = std::fs::remove_dir_all(&out_dir);
        Ok(())
    }
}
