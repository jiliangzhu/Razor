use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Context as _;

use crate::buckets::{classify_bucket, fill_share_p25};
use crate::config::Config;
use crate::reasons::{format_notes, ShadowNoteReason};
use crate::report::{generate_report_files, ReportThresholds};
use crate::schema::{
    FILE_REPORT_JSON, FILE_REPORT_MD, FILE_RUN_CONFIG, FILE_SHADOW_LOG, FILE_SNAPSHOTS,
    FILE_TRADES, SCHEMA_VERSION, SHADOW_HEADER, SNAPSHOTS_HEADER, TRADES_HEADER,
};
use crate::types::{Bps, LegSnapshot, MarketSnapshot, Signal, SignalLeg, Strategy, TradeTick};

pub const FILE_REPLAY_SHADOW_LOG: &str = "replay_shadow_log.csv";
pub const FILE_REPLAY_REPORT_JSON: &str = "replay_report.json";
pub const FILE_REPLAY_REPORT_MD: &str = "replay_report.md";

#[derive(Debug, Clone)]
pub struct ReplayOptions {
    pub out_dir: PathBuf,
    pub replay_run_id: String,
}

#[derive(Debug)]
pub struct ReplayResult {
    pub run_dir: PathBuf,
    pub out_dir: PathBuf,
    pub replay_run_id: String,
    pub signals: u64,
    pub shadow_rows: u64,
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

pub fn run_replay(run_dir: &Path, opts: ReplayOptions) -> anyhow::Result<ReplayResult> {
    std::fs::create_dir_all(&opts.out_dir)
        .with_context(|| format!("create {}", opts.out_dir.display()))?;

    let cfg_raw = std::fs::read_to_string(run_dir.join(FILE_RUN_CONFIG))
        .context("read run config snapshot")?;
    let cfg: Config = toml::from_str(&cfg_raw).context("parse run config snapshot")?;

    let snapshots_path = run_dir.join(FILE_SNAPSHOTS);
    let trades_path = run_dir.join(FILE_TRADES);

    let snapshots = read_snapshots_csv(&snapshots_path).context("read snapshots.csv")?;
    let trades_by_key = read_trades_by_key(&trades_path).context("read trades.csv")?;

    let signals = generate_signals(&cfg, &opts.replay_run_id, &snapshots);

    let out_shadow_path = opts.out_dir.join(FILE_REPLAY_SHADOW_LOG);
    write_replay_shadow_log(
        &cfg,
        &opts.replay_run_id,
        &out_shadow_path,
        &signals,
        &trades_by_key,
    )
    .context("write replay_shadow_log.csv")?;

    // Generate report.json/md using the existing report generator by symlinking/copying
    // the replay shadow log into the expected file name.
    let shadow_link = opts.out_dir.join(FILE_SHADOW_LOG);
    link_or_copy(&out_shadow_path, &shadow_link).context("link shadow_log.csv")?;

    let thresholds = ReportThresholds {
        min_total_shadow_pnl: cfg.report.min_total_shadow_pnl,
        min_avg_set_ratio: cfg.report.min_avg_set_ratio,
    };
    let _report = generate_report_files(&opts.out_dir, &opts.replay_run_id, thresholds)
        .context("generate report for replay")?;

    let report_json = opts.out_dir.join(FILE_REPORT_JSON);
    let report_md = opts.out_dir.join(FILE_REPORT_MD);
    std::fs::copy(&report_json, opts.out_dir.join(FILE_REPLAY_REPORT_JSON))
        .with_context(|| format!("copy {}", report_json.display()))?;
    std::fs::copy(&report_md, opts.out_dir.join(FILE_REPLAY_REPORT_MD))
        .with_context(|| format!("copy {}", report_md.display()))?;

    Ok(ReplayResult {
        run_dir: run_dir.to_path_buf(),
        out_dir: opts.out_dir,
        replay_run_id: opts.replay_run_id,
        signals: signals.len() as u64,
        shadow_rows: signals.len() as u64,
    })
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
        if !sum_ask.is_finite() || sum_ask <= 0.0 {
            continue;
        }

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

fn write_replay_shadow_log(
    cfg: &Config,
    run_id: &str,
    out_path: &Path,
    signals: &[Signal],
    trades_by_key: &HashMap<(String, String), Vec<TradeLite>>,
) -> anyhow::Result<()> {
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(false)
        .from_path(out_path)
        .with_context(|| format!("open {}", out_path.display()))?;
    wtr.write_record(SHADOW_HEADER)
        .context("write replay shadow header")?;

    for s in signals {
        let legs_n = s.legs.len();
        if !(2..=3).contains(&legs_n) {
            continue;
        }

        let window_start_ms = s.signal_ts_ms + cfg.shadow.window_start_ms;
        let window_end_ms = s.signal_ts_ms + cfg.shadow.window_end_ms;

        let fill_share_used = fill_share_p25(s.bucket, &cfg.buckets);

        let mut legs_sorted = s.legs.clone();
        legs_sorted.sort_by_key(|l| l.leg_index);

        let mut v_mkt: [f64; 3] = [0.0, 0.0, 0.0];
        let mut q_fill: [f64; 3] = [0.0, 0.0, 0.0];

        let mut invalid_limit = false;
        for (i, leg) in legs_sorted.iter().take(3).enumerate() {
            if !leg.limit_price.is_finite() || leg.limit_price <= 0.0 {
                invalid_limit = true;
                continue;
            }
            let key = (s.market_id.clone(), leg.token_id.clone());
            if let Some(trades) = trades_by_key.get(&key) {
                v_mkt[i] = volume_at_or_better_price(
                    trades,
                    window_start_ms,
                    window_end_ms,
                    leg.limit_price,
                );
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
        for leg in &legs_sorted {
            cost_set_per_unit += Bps::FEE_POLY.apply_cost(leg.limit_price);
        }
        let cost_set = q_set * cost_set_per_unit;
        let proceeds_set = q_set * Bps::FEE_MERGE.apply_proceeds(1.0);
        let pnl_set = proceeds_set - cost_set;

        let dump_slippage_assumed = crate::schema::DUMP_SLIPPAGE_ASSUMED;
        let mut pnl_left_total: f64 = 0.0;
        for (i, leg) in legs_sorted.iter().take(3).enumerate() {
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

        let window_stats = window_stats_for_signal(
            trades_by_key,
            &s.market_id,
            &legs_sorted[..legs_n],
            window_start_ms,
            window_end_ms,
        );

        let mut reasons: Vec<ShadowNoteReason> = s.reasons.clone();
        if fill_share_used <= 0.0 || !fill_share_used.is_finite() {
            reasons.push(ShadowNoteReason::FillShareP25Zero);
        }
        if s.q_req <= 0.0 || !s.q_req.is_finite() {
            reasons.push(ShadowNoteReason::InvalidQty);
        }
        if invalid_limit {
            reasons.push(ShadowNoteReason::InvalidPrice);
        }

        let mut bid_missing_any = false;
        let mut book_missing_any = false;
        for l in &legs_sorted[..legs_n] {
            let bid_missing = !l.best_bid_at_signal.is_finite() || l.best_bid_at_signal <= 0.0;
            if bid_missing {
                bid_missing_any = true;
                let ask_missing = !l.best_ask_at_signal.is_finite() || l.best_ask_at_signal <= 0.0;
                if ask_missing {
                    book_missing_any = true;
                }
            }
        }
        if bid_missing_any {
            reasons.push(ShadowNoteReason::MissingBid);
        }
        if book_missing_any {
            reasons.push(ShadowNoteReason::MissingBook);
        }

        if window_stats.trades_in_window == 0 {
            reasons.push(ShadowNoteReason::WindowEmpty);
        }
        if cfg.shadow.max_trade_gap_ms > 0
            && window_stats.trades_in_window > 1
            && window_stats.max_gap_ms > cfg.shadow.max_trade_gap_ms
        {
            reasons.push(ShadowNoteReason::WindowDataGap);
        }

        let v_mkt_sum: f64 = v_mkt[..legs_n].iter().sum();
        if v_mkt_sum <= 0.0 {
            reasons.push(ShadowNoteReason::NoTrades);
        }

        let bucket_nan = reasons.iter().any(|r| {
            matches!(
                r,
                ShadowNoteReason::BucketThinNan | ShadowNoteReason::BucketLiquidNan
            )
        });
        let worst_leg_token_id = if bucket_nan {
            String::new()
        } else {
            legs_sorted
                .iter()
                .find(|l| l.leg_index == s.bucket_metrics.worst_leg_index)
                .map(|l| l.token_id.clone())
                .unwrap_or_default()
        };

        if worst_leg_token_id.is_empty() {
            match s.bucket {
                crate::types::LiquidityBucket::Liquid => {
                    reasons.push(ShadowNoteReason::BucketLiquidNan)
                }
                crate::types::LiquidityBucket::Thin => {
                    reasons.push(ShadowNoteReason::BucketThinNan)
                }
            }
        }

        let notes = format_notes(&reasons);

        let mut record: Vec<String> = Vec::with_capacity(SHADOW_HEADER.len());
        record.push(run_id.to_string());
        record.push(SCHEMA_VERSION.to_string());
        record.push(s.signal_id.to_string());
        record.push(s.signal_ts_ms.to_string());
        record.push(cfg.shadow.window_start_ms.to_string());
        record.push(cfg.shadow.window_end_ms.to_string());
        record.push(s.market_id.clone());
        record.push(s.strategy.as_str().to_string());
        record.push(s.bucket.as_str().to_ascii_lowercase());
        record.push(worst_leg_token_id);
        record.push(s.q_req.to_string());
        record.push((legs_n as u8).to_string());
        record.push(q_set.to_string());

        for i in 0..3 {
            if i < legs_n {
                let leg = &s.legs[i];
                record.push(leg.token_id.clone());
                record.push(leg.limit_price.to_string());
                record.push(leg.best_bid_at_signal.to_string());
                record.push(v_mkt[i].to_string());
                record.push(q_fill[i].to_string());
            } else {
                record.push(String::new());
                record.push("0".to_string());
                record.push("0".to_string());
                record.push("0".to_string());
                record.push("0".to_string());
            }
        }

        record.push(cost_set.to_string());
        record.push(proceeds_set.to_string());
        record.push(pnl_set.to_string());
        record.push(pnl_left_total.to_string());
        record.push(total_pnl.to_string());
        record.push(q_fill_avg.to_string());
        record.push(set_ratio.to_string());
        record.push(fill_share_used.to_string());
        record.push(dump_slippage_assumed.to_string());
        record.push(notes);
        debug_assert_eq!(record.len(), SHADOW_HEADER.len());
        wtr.write_record(record).context("write replay row")?;
    }

    wtr.flush().context("flush replay shadow_log")?;
    Ok(())
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
        // We keep this strict so that replay is reproducible and errors are explicit.
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

fn window_stats_for_signal(
    trades_by_key: &HashMap<(String, String), Vec<TradeLite>>,
    market_id: &str,
    legs: &[SignalLeg],
    start_ms: u64,
    end_ms: u64,
) -> crate::trade_store::WindowStats {
    if market_id.trim().is_empty() || start_ms > end_ms || legs.is_empty() {
        return crate::trade_store::WindowStats::default();
    }

    let mut leg_trades: Vec<&[TradeLite]> = Vec::with_capacity(legs.len());
    for leg in legs.iter().take(3) {
        let key = (market_id.to_string(), leg.token_id.clone());
        if let Some(v) = trades_by_key.get(&key) {
            leg_trades.push(v.as_slice());
        } else {
            leg_trades.push(&[]);
        }
    }

    let mut idx: [usize; 3] = [0, 0, 0];
    for (i, t) in leg_trades.iter().enumerate().take(3) {
        idx[i] = lower_bound(t, start_ms);
    }

    let mut trades_in_window: usize = 0;
    let mut max_gap_ms: u64 = 0;
    let mut prev_ts: Option<u64> = None;

    loop {
        let mut best_leg: Option<usize> = None;
        let mut best_ts: u64 = 0;

        for i in 0..leg_trades.len().min(3) {
            let t = leg_trades[i];
            if idx[i] >= t.len() {
                continue;
            }
            let ts = t[idx[i]].ts_ms;
            if ts > end_ms {
                continue;
            }
            if best_leg.is_none() || ts < best_ts {
                best_leg = Some(i);
                best_ts = ts;
            }
        }

        let Some(i) = best_leg else {
            break;
        };

        idx[i] += 1;
        trades_in_window += 1;
        if let Some(prev) = prev_ts {
            max_gap_ms = max_gap_ms.max(best_ts.saturating_sub(prev));
        }
        prev_ts = Some(best_ts);
    }

    if trades_in_window == 0 {
        return crate::trade_store::WindowStats::default();
    }

    crate::trade_store::WindowStats {
        trades_in_window,
        max_gap_ms,
        max_trade_size: 0.0,
        max_trade_notional: 0.0,
    }
}

fn link_or_copy(src: &Path, dst: &Path) -> anyhow::Result<()> {
    if dst.exists() {
        let _ = std::fs::remove_file(dst);
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let target = src.file_name().unwrap_or_default();
        if let Err(e) = symlink(target, dst) {
            tracing::warn!(error = %e, "symlink failed; copying instead");
            std::fs::copy(src, dst).with_context(|| format!("copy {}", dst.display()))?;
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        std::fs::copy(src, dst).with_context(|| format!("copy {}", dst.display()))?;
        Ok(())
    }
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
    fn snapshots_header_is_strict() {
        assert_eq!(
            SNAPSHOTS_HEADER.join(","),
            "ts_ms,market_id,legs_n,leg0_token_id,leg0_best_bid,leg0_best_ask,leg0_depth3_usdc,leg1_token_id,leg1_best_bid,leg1_best_ask,leg1_depth3_usdc,leg2_token_id,leg2_best_bid,leg2_best_ask,leg2_depth3_usdc"
        );
    }

    #[test]
    fn trades_header_is_strict() {
        assert_eq!(
            TRADES_HEADER.join(","),
            "ts_ms,market_id,token_id,price,size,trade_id,ingest_ts_ms,exchange_ts_ms"
        );
    }
}
