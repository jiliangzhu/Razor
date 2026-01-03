use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use serde::Serialize;

use crate::schema::{FILE_SHADOW_LOG, SCHEMA_VERSION, SHADOW_HEADER};
use crate::shadow_sweep::{recompute_ledger_row, RecomputeLeg};

pub const FILE_DAILY_SCORES: &str = "daily_scores.csv";
pub const FILE_WALK_FORWARD_JSON: &str = "walk_forward.json";

pub const DAILY_SCORES_HEADER: [&str; 8] = [
    "run_id",
    "day_start_unix_ms",
    "signals",
    "total_pnl_sum",
    "total_pnl_avg",
    "avg_set_ratio",
    "legging_rate",
    "worst_20_pnl_sum",
];

const DAY_MS: u64 = 86_400_000;

#[derive(Debug, Clone)]
pub struct DatasetSplitResult {
    pub run_dir: PathBuf,
    pub out_dir: PathBuf,
    pub run_id: String,
    pub days: Vec<u64>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ParamTriple {
    pub fill_share_liquid: f64,
    pub fill_share_thin: f64,
    pub dump_slippage_assumed: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalkForwardMetrics {
    pub signals: u64,
    pub total_pnl_sum: f64,
    pub total_pnl_avg: f64,
    pub avg_set_ratio: f64,
    pub legging_rate: f64,
    pub worst_20_pnl_sum: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalkForwardStep {
    pub train_days: Vec<u64>,
    pub val_day: u64,
    pub best_params: ParamTriple,
    pub train_metrics: WalkForwardMetrics,
    pub val_metrics: WalkForwardMetrics,
    pub pnl_drop_ratio: f64,
    pub legging_drift: f64,
    pub step_risk: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalkForwardReport {
    pub version: String,
    pub run_id: String,
    pub set_ratio_threshold: f64,
    pub grid: WalkForwardGrid,
    pub selection_rule: String,
    pub steps: Vec<WalkForwardStep>,
    pub overfit_risk_score: f64,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalkForwardGrid {
    pub fill_share_liquid_values: Vec<f64>,
    pub fill_share_thin_values: Vec<f64>,
    pub dump_slippage_values: Vec<f64>,
}

#[derive(Debug, Clone)]
struct Row {
    day_start_ms: u64,
    bucket: BucketKey,
    q_req: f64,
    legs: Vec<RecomputeLeg>,
    total_pnl_logged: f64,
    set_ratio_logged: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BucketKey {
    Liquid,
    Thin,
}

impl BucketKey {
    fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "liquid" => Some(BucketKey::Liquid),
            "thin" => Some(BucketKey::Thin),
            _ => None,
        }
    }
}

pub fn run_dataset_split(
    run_dir: &Path,
    out_dir: &Path,
    set_ratio_threshold: f64,
) -> anyhow::Result<DatasetSplitResult> {
    std::fs::create_dir_all(out_dir).with_context(|| format!("create {}", out_dir.display()))?;

    let run_id = crate::run_meta::RunMeta::read_from_dir(run_dir)
        .map(|m| m.run_id)
        .unwrap_or_else(|_| "unknown".to_string());

    let shadow_path = run_dir.join(FILE_SHADOW_LOG);
    let rows = parse_rows(&shadow_path, &run_id).context("parse shadow_log rows")?;

    let mut by_day: BTreeMap<u64, Vec<Row>> = BTreeMap::new();
    for r in rows {
        by_day.entry(r.day_start_ms).or_default().push(r);
    }
    let days: Vec<u64> = by_day.keys().copied().collect();

    write_daily_scores(out_dir, &run_id, &by_day, set_ratio_threshold)
        .context("write daily_scores.csv")?;
    write_walk_forward_json(out_dir, &run_id, &days, &by_day, set_ratio_threshold)
        .context("write walk_forward.json")?;

    Ok(DatasetSplitResult {
        run_dir: run_dir.to_path_buf(),
        out_dir: out_dir.to_path_buf(),
        run_id,
        days,
    })
}

fn write_daily_scores(
    out_dir: &Path,
    run_id: &str,
    by_day: &BTreeMap<u64, Vec<Row>>,
    set_ratio_threshold: f64,
) -> anyhow::Result<()> {
    let path = out_dir.join(FILE_DAILY_SCORES);
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(false)
        .from_path(&path)
        .with_context(|| format!("open {}", path.display()))?;
    wtr.write_record(DAILY_SCORES_HEADER)
        .context("write daily header")?;

    for (day, rows) in by_day {
        let m = compute_metrics_logged(rows, set_ratio_threshold);
        wtr.write_record([
            run_id.to_string(),
            day.to_string(),
            m.signals.to_string(),
            fmt_f64(m.total_pnl_sum),
            fmt_f64(m.total_pnl_avg),
            fmt_f64(m.avg_set_ratio),
            fmt_f64(m.legging_rate),
            fmt_f64(m.worst_20_pnl_sum),
        ])
        .context("write daily row")?;
    }

    wtr.flush().context("flush daily_scores.csv")?;
    Ok(())
}

fn write_walk_forward_json(
    out_dir: &Path,
    run_id: &str,
    days: &[u64],
    by_day: &BTreeMap<u64, Vec<Row>>,
    set_ratio_threshold: f64,
) -> anyhow::Result<()> {
    let grid = default_grid();
    let selection_rule = "max total_pnl_sum, then max avg_set_ratio, then min legging_rate, then max worst_20_pnl_sum".to_string();

    let mut steps: Vec<WalkForwardStep> = Vec::new();
    let mut notes: Vec<String> = Vec::new();

    if days.len() < 2 {
        notes.push("insufficient_days: need >=2 distinct UTC days for walk-forward".to_string());
    }

    for i in 1..days.len() {
        let train_days: Vec<u64> = days[..i].to_vec();
        let val_day = days[i];

        let train_rows = concat_days(by_day, &train_days);
        let val_rows = by_day.get(&val_day).cloned().unwrap_or_default();

        if train_rows.is_empty() || val_rows.is_empty() {
            continue;
        }

        let (best_params, train_metrics) =
            select_best_params(&train_rows, &grid, set_ratio_threshold);
        let val_metrics = compute_metrics_recomputed(&val_rows, best_params, set_ratio_threshold);

        let pnl_drop = train_metrics.total_pnl_sum - val_metrics.total_pnl_sum;
        let denom = train_metrics.total_pnl_sum.abs().max(1e-9);
        let pnl_drop_ratio = (pnl_drop / denom).max(0.0);

        let legging_drift = (val_metrics.legging_rate - train_metrics.legging_rate).abs();
        let step_risk = pnl_drop_ratio + legging_drift;

        steps.push(WalkForwardStep {
            train_days,
            val_day,
            best_params,
            train_metrics,
            val_metrics,
            pnl_drop_ratio,
            legging_drift,
            step_risk,
        });
    }

    let overfit_risk_score = if steps.is_empty() {
        1.0
    } else {
        steps.iter().map(|s| s.step_risk).sum::<f64>() / (steps.len() as f64)
    };

    let report = WalkForwardReport {
        version: "walk_forward_v1".to_string(),
        run_id: run_id.to_string(),
        set_ratio_threshold,
        grid: WalkForwardGrid {
            fill_share_liquid_values: grid.fill_share_liquid_values.clone(),
            fill_share_thin_values: grid.fill_share_thin_values.clone(),
            dump_slippage_values: grid.dump_slippage_values.clone(),
        },
        selection_rule,
        steps,
        overfit_risk_score,
        notes,
    };

    let json = serde_json::to_vec_pretty(&report).context("serialize walk_forward.json")?;
    std::fs::write(out_dir.join(FILE_WALK_FORWARD_JSON), json)
        .context("write walk_forward.json")?;
    Ok(())
}

#[derive(Debug, Clone)]
struct Grid {
    fill_share_liquid_values: Vec<f64>,
    fill_share_thin_values: Vec<f64>,
    dump_slippage_values: Vec<f64>,
}

fn default_grid() -> Grid {
    Grid {
        fill_share_liquid_values: vec![0.20, 0.30, 0.40],
        fill_share_thin_values: vec![0.05, 0.10, 0.15],
        dump_slippage_values: vec![0.03, 0.05, 0.10],
    }
}

fn select_best_params(
    rows: &[Row],
    grid: &Grid,
    set_ratio_threshold: f64,
) -> (ParamTriple, WalkForwardMetrics) {
    let mut best: Option<(ParamTriple, WalkForwardMetrics)> = None;

    for &fill_share_liquid in &grid.fill_share_liquid_values {
        for &fill_share_thin in &grid.fill_share_thin_values {
            for &dump_slippage_assumed in &grid.dump_slippage_values {
                let params = ParamTriple {
                    fill_share_liquid,
                    fill_share_thin,
                    dump_slippage_assumed,
                };
                let m = compute_metrics_recomputed(rows, params, set_ratio_threshold);
                best = match best {
                    None => Some((params, m)),
                    Some((bp, bm)) => {
                        if is_better(&m, &bm, params, bp) {
                            Some((params, m))
                        } else {
                            Some((bp, bm))
                        }
                    }
                };
            }
        }
    }

    best.unwrap_or_else(|| {
        let params = ParamTriple {
            fill_share_liquid: 0.30,
            fill_share_thin: 0.10,
            dump_slippage_assumed: 0.05,
        };
        let m = compute_metrics_recomputed(rows, params, set_ratio_threshold);
        (params, m)
    })
}

fn is_better(
    a: &WalkForwardMetrics,
    b: &WalkForwardMetrics,
    a_params: ParamTriple,
    b_params: ParamTriple,
) -> bool {
    cmp_f64_desc(a.total_pnl_sum, b.total_pnl_sum)
        .then_with(|| cmp_f64_desc(a.avg_set_ratio, b.avg_set_ratio))
        .then_with(|| cmp_f64_asc(a.legging_rate, b.legging_rate))
        .then_with(|| cmp_f64_desc(a.worst_20_pnl_sum, b.worst_20_pnl_sum))
        .then_with(|| fmt_params(a_params).cmp(&fmt_params(b_params)))
        == std::cmp::Ordering::Less
}

fn fmt_params(p: ParamTriple) -> String {
    format!(
        "{:.6},{:.6},{:.6}",
        p.fill_share_liquid, p.fill_share_thin, p.dump_slippage_assumed
    )
}

fn compute_metrics_logged(rows: &[Row], set_ratio_threshold: f64) -> WalkForwardMetrics {
    let mut pnls: Vec<f64> = Vec::with_capacity(rows.len());
    let mut sum_pnl = 0.0;
    let mut set_ratio_sum = 0.0;
    let mut legging_miss = 0u64;

    for r in rows {
        sum_pnl += r.total_pnl_logged;
        pnls.push(r.total_pnl_logged);
        set_ratio_sum += r.set_ratio_logged;
        if r.set_ratio_logged < set_ratio_threshold {
            legging_miss += 1;
        }
    }

    pnls.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let worst_20_pnl_sum: f64 = pnls.iter().take(pnls.len().min(20)).copied().sum();

    let n = rows.len() as f64;
    let total_pnl_avg = if n > 0.0 { sum_pnl / n } else { 0.0 };
    let avg_set_ratio = if n > 0.0 { set_ratio_sum / n } else { 0.0 };
    let legging_rate = if n > 0.0 {
        (legging_miss as f64) / n
    } else {
        0.0
    };

    WalkForwardMetrics {
        signals: rows.len() as u64,
        total_pnl_sum: sum_pnl,
        total_pnl_avg,
        avg_set_ratio,
        legging_rate,
        worst_20_pnl_sum,
    }
}

fn compute_metrics_recomputed(
    rows: &[Row],
    params: ParamTriple,
    set_ratio_threshold: f64,
) -> WalkForwardMetrics {
    let mut pnls: Vec<f64> = Vec::with_capacity(rows.len());
    let mut sum_pnl = 0.0;
    let mut set_ratio_sum = 0.0;
    let mut legging_miss = 0u64;

    for r in rows {
        let fill_share_used = match r.bucket {
            BucketKey::Liquid => params.fill_share_liquid,
            BucketKey::Thin => params.fill_share_thin,
        };
        let (total_pnl, set_ratio) = recompute_ledger_row(
            r.q_req,
            &r.legs,
            fill_share_used,
            params.dump_slippage_assumed,
        );
        sum_pnl += total_pnl;
        pnls.push(total_pnl);
        set_ratio_sum += set_ratio;
        if set_ratio < set_ratio_threshold {
            legging_miss += 1;
        }
    }

    pnls.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let worst_20_pnl_sum: f64 = pnls.iter().take(pnls.len().min(20)).copied().sum();

    let n = rows.len() as f64;
    let total_pnl_avg = if n > 0.0 { sum_pnl / n } else { 0.0 };
    let avg_set_ratio = if n > 0.0 { set_ratio_sum / n } else { 0.0 };
    let legging_rate = if n > 0.0 {
        (legging_miss as f64) / n
    } else {
        0.0
    };

    WalkForwardMetrics {
        signals: rows.len() as u64,
        total_pnl_sum: sum_pnl,
        total_pnl_avg,
        avg_set_ratio,
        legging_rate,
        worst_20_pnl_sum,
    }
}

fn concat_days(by_day: &BTreeMap<u64, Vec<Row>>, days: &[u64]) -> Vec<Row> {
    let mut out: Vec<Row> = Vec::new();
    for d in days {
        if let Some(v) = by_day.get(d) {
            out.extend(v.clone());
        }
    }
    out
}

fn parse_rows(shadow_log_path: &Path, run_id: &str) -> anyhow::Result<Vec<Row>> {
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .from_path(shadow_log_path)
        .with_context(|| format!("open {}", shadow_log_path.display()))?;

    let header = rdr
        .headers()
        .with_context(|| format!("read header {}", shadow_log_path.display()))?
        .clone();

    if header.iter().map(|s| s.trim()).collect::<Vec<_>>() != SHADOW_HEADER {
        anyhow::bail!("shadow_log.csv header mismatch (expected frozen SHADOW_HEADER)");
    }

    let idx_run_id = idx(&header, "run_id")?;
    let idx_schema = idx(&header, "schema_version")?;
    let idx_ts = idx(&header, "signal_ts_unix_ms")?;
    let idx_bucket = idx(&header, "bucket")?;
    let idx_legs_n = idx(&header, "legs_n")?;
    let idx_q_req = idx(&header, "q_req")?;
    let idx_total_pnl = idx(&header, "total_pnl")?;
    let idx_set_ratio = idx(&header, "set_ratio")?;

    let leg0 = LegIdxs::new(0)?;
    let leg1 = LegIdxs::new(1)?;
    let leg2 = LegIdxs::new(2)?;

    let mut out: Vec<Row> = Vec::new();
    for record in rdr.records() {
        let record = match record {
            Ok(r) => r,
            Err(_) => continue,
        };

        if record.get(idx_run_id).unwrap_or("").trim() != run_id {
            continue;
        }
        if !record
            .get(idx_schema)
            .unwrap_or("")
            .trim()
            .eq_ignore_ascii_case(SCHEMA_VERSION)
        {
            continue;
        }

        let ts_ms = record
            .get(idx_ts)
            .and_then(parse_u64)
            .context("signal_ts_unix_ms")?;
        let day_start_ms = (ts_ms / DAY_MS) * DAY_MS;

        let bucket = record
            .get(idx_bucket)
            .and_then(BucketKey::parse)
            .context("bucket")?;

        let legs_n = record
            .get(idx_legs_n)
            .and_then(parse_u64)
            .context("legs_n")? as usize;
        if !(2..=3).contains(&legs_n) {
            continue;
        }

        let q_req = record.get(idx_q_req).and_then(parse_f64).context("q_req")?;

        let total_pnl_logged = record
            .get(idx_total_pnl)
            .and_then(parse_f64)
            .context("total_pnl")?;
        let set_ratio_logged = record
            .get(idx_set_ratio)
            .and_then(parse_f64)
            .context("set_ratio")?;

        let mut legs: Vec<RecomputeLeg> = Vec::with_capacity(legs_n);
        for (i, idxs) in [leg0, leg1, leg2].into_iter().enumerate() {
            if i >= legs_n {
                break;
            }
            let p_limit = record
                .get(idxs.p_limit)
                .and_then(parse_f64)
                .context("p_limit")?;
            let best_bid = record.get(idxs.best_bid).and_then(parse_f64).unwrap_or(0.0);
            let v_mkt = record
                .get(idxs.v_mkt)
                .and_then(parse_f64)
                .context("v_mkt")?;
            legs.push(RecomputeLeg {
                p_limit,
                best_bid,
                v_mkt,
            });
        }
        if legs.len() != legs_n {
            continue;
        }

        out.push(Row {
            day_start_ms,
            bucket,
            q_req,
            legs,
            total_pnl_logged,
            set_ratio_logged,
        });
    }

    Ok(out)
}

#[derive(Clone, Copy)]
struct LegIdxs {
    p_limit: usize,
    best_bid: usize,
    v_mkt: usize,
}

impl LegIdxs {
    fn new(i: u8) -> anyhow::Result<Self> {
        Ok(Self {
            p_limit: SHADOW_HEADER
                .iter()
                .position(|h| h.eq_ignore_ascii_case(&format!("leg{i}_p_limit")))
                .context("leg p_limit idx")?,
            best_bid: SHADOW_HEADER
                .iter()
                .position(|h| h.eq_ignore_ascii_case(&format!("leg{i}_best_bid")))
                .context("leg best_bid idx")?,
            v_mkt: SHADOW_HEADER
                .iter()
                .position(|h| h.eq_ignore_ascii_case(&format!("leg{i}_v_mkt")))
                .context("leg v_mkt idx")?,
        })
    }
}

fn idx(header: &csv::StringRecord, name: &str) -> anyhow::Result<usize> {
    header
        .iter()
        .position(|h| h.trim().eq_ignore_ascii_case(name))
        .with_context(|| format!("missing column: {name}"))
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

fn fmt_f64(v: f64) -> String {
    if !v.is_finite() {
        return "NaN".to_string();
    }
    format!("{v:.6}")
}

fn cmp_f64_desc(a: f64, b: f64) -> std::cmp::Ordering {
    b.partial_cmp(&a).unwrap_or(std::cmp::Ordering::Equal)
}

fn cmp_f64_asc(a: f64, b: f64) -> std::cmp::Ordering {
    a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daily_scores_header_is_frozen() {
        assert_eq!(
            DAILY_SCORES_HEADER.join(","),
            "run_id,day_start_unix_ms,signals,total_pnl_sum,total_pnl_avg,avg_set_ratio,legging_rate,worst_20_pnl_sum"
        );
    }

    #[test]
    fn splits_rows_into_days_and_writes_outputs() -> anyhow::Result<()> {
        let tmp = std::env::temp_dir().join(format!(
            "razor_dataset_split_test_{}_{}",
            std::process::id(),
            crate::types::now_ms()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp)?;

        // Minimal run_meta.json.
        crate::run_meta::RunMeta {
            run_id: "run_x".to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            git_sha: "unknown".to_string(),
            start_ts_unix_ms: 0,
            config_path: "config.toml".to_string(),
            trade_ts_source: "local".to_string(),
            notes_enum_version: "v1".to_string(),
            trade_poll_taker_only: None,
            sim_stress: crate::run_meta::SimStressProfile::default(),
        }
        .write_to_dir(&tmp)?;

        // Build a tiny shadow_log.csv with 3 days.
        let mut csv = String::new();
        csv.push_str(&SHADOW_HEADER.join(","));
        csv.push('\n');

        fn mk_row(ts_ms: u64, total_pnl: f64, set_ratio: f64) -> Vec<String> {
            let mut row = vec![String::new(); SHADOW_HEADER.len()];
            let idx = |name: &str| SHADOW_HEADER.iter().position(|h| *h == name).unwrap();
            row[idx("run_id")] = "run_x".to_string();
            row[idx("schema_version")] = SCHEMA_VERSION.to_string();
            row[idx("signal_ts_unix_ms")] = ts_ms.to_string();
            row[idx("bucket")] = "liquid".to_string();
            row[idx("legs_n")] = "2".to_string();
            row[idx("q_req")] = "10".to_string();
            row[idx("leg0_p_limit")] = "0.48".to_string();
            row[idx("leg0_best_bid")] = "0.47".to_string();
            row[idx("leg0_v_mkt")] = "10".to_string();
            row[idx("leg1_p_limit")] = "0.49".to_string();
            row[idx("leg1_best_bid")] = "0.48".to_string();
            row[idx("leg1_v_mkt")] = "10".to_string();
            row[idx("total_pnl")] = total_pnl.to_string();
            row[idx("set_ratio")] = set_ratio.to_string();
            row[idx("notes")] = "".to_string();
            row
        }

        for (i, (pnl, ratio)) in [(-0.1, 0.7), (0.2, 1.0), (0.1, 1.0)]
            .into_iter()
            .enumerate()
        {
            let ts = (i as u64) * DAY_MS;
            csv.push_str(&mk_row(ts, pnl, ratio).join(","));
            csv.push('\n');
        }

        std::fs::write(tmp.join(FILE_SHADOW_LOG), csv.as_bytes())?;

        let out_dir = tmp.join("out");
        run_dataset_split(&tmp, &out_dir, 0.85)?;

        assert!(out_dir.join(FILE_DAILY_SCORES).exists());
        assert!(out_dir.join(FILE_WALK_FORWARD_JSON).exists());

        Ok(())
    }
}
