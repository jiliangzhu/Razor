use std::path::{Path, PathBuf};

use anyhow::Context as _;
use serde::Serialize;

use crate::schema::SCHEMA_VERSION;
use crate::types::Bps;

pub const FILE_SWEEP_SCORES: &str = "sweep_scores.csv";
pub const FILE_BEST_PATCH: &str = "best_patch.toml";
pub const FILE_SWEEP_RECOMMENDATION: &str = "sweep_recommendation.json";

pub const SWEEP_SCORES_HEADER: [&str; 13] = [
    "run_id",
    "rows_total",
    "rows_ok",
    "rows_bad",
    "fill_share_liquid",
    "fill_share_thin",
    "dump_slippage_assumed",
    "set_ratio_threshold",
    "total_pnl_sum",
    "total_pnl_avg",
    "set_ratio_avg",
    "legging_rate",
    "worst_20_pnl_sum",
];

#[derive(Debug, Clone)]
pub struct SweepGrid {
    pub fill_share_liquid_values: Vec<f64>,
    pub fill_share_thin_values: Vec<f64>,
    pub dump_slippage_values: Vec<f64>,
    pub set_ratio_threshold: f64,
}

impl SweepGrid {
    pub fn sanitize(mut self) -> Self {
        self.fill_share_liquid_values
            .retain(|v| v.is_finite() && *v >= 0.0 && *v <= 1.0);
        self.fill_share_thin_values
            .retain(|v| v.is_finite() && *v >= 0.0 && *v <= 1.0);
        self.dump_slippage_values
            .retain(|v| v.is_finite() && *v >= 0.0 && *v < 1.0);
        self.fill_share_liquid_values
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        self.fill_share_thin_values
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        self.dump_slippage_values
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        self
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SweepScoreRow {
    pub run_id: String,
    pub rows_total: u64,
    pub rows_ok: u64,
    pub rows_bad: u64,
    pub fill_share_liquid: f64,
    pub fill_share_thin: f64,
    pub dump_slippage_assumed: f64,
    pub set_ratio_threshold: f64,
    pub total_pnl_sum: f64,
    pub total_pnl_avg: f64,
    pub set_ratio_avg: f64,
    pub legging_rate: f64,
    pub worst_20_pnl_sum: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct RecomputeLeg {
    pub p_limit: f64,
    pub best_bid: f64,
    pub v_mkt: f64,
}

/// Recompute a single shadow ledger entry under a hypothetical `(fill_share_used, dump_slippage_assumed)`.
///
/// This is intentionally independent of bucket logic so that other tools (day14_report stress)
/// can reuse it while keeping the Frozen Spec accounting formula identical.
pub fn recompute_ledger_row(
    q_req: f64,
    legs: &[RecomputeLeg],
    fill_share_used: f64,
    dump_slippage_assumed: f64,
) -> (f64, f64) {
    if !q_req.is_finite() || q_req <= 0.0 || !fill_share_used.is_finite() || legs.is_empty() {
        return (0.0, 0.0);
    }

    let mut q_fills: Vec<f64> = Vec::with_capacity(legs.len());
    for leg in legs {
        let v_mkt = if leg.v_mkt.is_finite() && leg.v_mkt > 0.0 {
            leg.v_mkt
        } else {
            0.0
        };
        let q_fill = (v_mkt * fill_share_used).min(q_req);
        q_fills.push(q_fill);
    }

    let q_set = q_fills
        .iter()
        .copied()
        .fold(f64::INFINITY, f64::min)
        .min(q_req);
    let q_set = if q_set.is_finite() { q_set } else { 0.0 };

    let mut cost_set_per_unit: f64 = 0.0;
    for leg in legs {
        let p = if leg.p_limit.is_finite() && leg.p_limit > 0.0 {
            leg.p_limit
        } else {
            0.0
        };
        cost_set_per_unit += Bps::FEE_POLY.apply_cost(p);
    }
    let cost_set = q_set * cost_set_per_unit;
    let proceeds_set = q_set * Bps::FEE_MERGE.apply_proceeds(1.0);
    let pnl_set = proceeds_set - cost_set;

    let dump_slippage_assumed = if dump_slippage_assumed.is_finite() {
        dump_slippage_assumed.clamp(0.0, 0.99)
    } else {
        0.0
    };

    let mut pnl_left_total: f64 = 0.0;
    for (idx, leg) in legs.iter().enumerate() {
        let q_left = q_fills.get(idx).copied().unwrap_or(0.0) - q_set;
        if q_left <= 0.0 {
            continue;
        }
        let p_limit = if leg.p_limit.is_finite() && leg.p_limit > 0.0 {
            leg.p_limit
        } else {
            0.0
        };
        let best_bid = if leg.best_bid.is_finite() && leg.best_bid > 0.0 {
            leg.best_bid
        } else {
            0.0
        };
        let exit = best_bid * (1.0 - dump_slippage_assumed);
        let proceeds_left_per_unit = Bps::FEE_POLY.apply_proceeds(exit);
        let cost_left_per_unit = Bps::FEE_POLY.apply_cost(p_limit);
        pnl_left_total += q_left * (proceeds_left_per_unit - cost_left_per_unit);
    }

    let total_pnl = pnl_set + pnl_left_total;

    let q_fill_avg = q_fills.iter().sum::<f64>() / (legs.len() as f64);
    let set_ratio = if q_fill_avg > 0.0 {
        q_set / q_fill_avg
    } else {
        0.0
    };

    (total_pnl, set_ratio)
}

impl SweepScoreRow {
    pub fn to_record(&self) -> [String; 13] {
        [
            self.run_id.clone(),
            self.rows_total.to_string(),
            self.rows_ok.to_string(),
            self.rows_bad.to_string(),
            fmt_f64(self.fill_share_liquid),
            fmt_f64(self.fill_share_thin),
            fmt_f64(self.dump_slippage_assumed),
            fmt_f64(self.set_ratio_threshold),
            fmt_f64(self.total_pnl_sum),
            fmt_f64(self.total_pnl_avg),
            fmt_f64(self.set_ratio_avg),
            fmt_f64(self.legging_rate),
            fmt_f64(self.worst_20_pnl_sum),
        ]
    }
}

#[derive(Debug, Clone)]
struct LedgerRow {
    bucket: BucketKey,
    q_req: f64,
    legs: Vec<LedgerLeg>,
}

#[derive(Debug, Clone)]
struct LedgerLeg {
    p_limit: f64,
    best_bid: f64,
    v_mkt: f64,
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

#[derive(Debug)]
pub struct ShadowSweepResult {
    pub run_id: String,
    pub rows_total: u64,
    pub rows_ok: u64,
    pub rows_bad: u64,
    pub scores: Vec<SweepScoreRow>,
    pub best: Option<SweepScoreRow>,
    pub out_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub struct StressMetrics {
    pub rows_ok: u64,
    pub rows_bad: u64,
    pub total_pnl_sum: f64,
    pub set_ratio_avg: f64,
    pub legging_rate: f64,
    pub worst_20_pnl_sum: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StressSummary {
    pub baseline: StressMetrics,
    pub dump_0_10: StressMetrics,
    pub fill_share_x0_70: StressMetrics,
    pub dump_0_10_fill_share_x0_70: StressMetrics,
}

/// Compute stress variants using only fields already present in `shadow_log.csv`.
///
/// This does NOT change the Day14 verdict; it is intended to quantify sensitivity to
/// (a) leftover dump slippage, and (b) fill_share pessimism.
pub fn compute_stress_summary(
    shadow_log_path: &Path,
    run_id: &str,
    set_ratio_threshold: f64,
) -> anyhow::Result<StressSummary> {
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .from_path(shadow_log_path)
        .with_context(|| format!("open {}", shadow_log_path.display()))?;

    let header = rdr
        .headers()
        .with_context(|| format!("read header {}", shadow_log_path.display()))?
        .clone();

    let idx_run_id = find_col(&header, "run_id").context("missing column: run_id")?;
    let idx_schema =
        find_col(&header, "schema_version").context("missing column: schema_version")?;
    let idx_legs_n = find_col(&header, "legs_n").context("missing column: legs_n")?;
    let idx_q_req = find_col(&header, "q_req").context("missing column: q_req")?;
    let idx_fill_share =
        find_col(&header, "fill_share_p25_used").context("missing column: fill_share_p25_used")?;
    let idx_dump = find_col(&header, "dump_slippage_assumed")
        .context("missing column: dump_slippage_assumed")?;

    let leg0 = StressLegIdxs::new(&header, 0)?;
    let leg1 = StressLegIdxs::new(&header, 1)?;
    let leg2 = StressLegIdxs::new(&header, 2)?;

    let mut base = StressAgg::new(set_ratio_threshold);
    let mut dump10 = StressAgg::new(set_ratio_threshold);
    let mut fill70 = StressAgg::new(set_ratio_threshold);
    let mut dump10_fill70 = StressAgg::new(set_ratio_threshold);

    for record in rdr.records() {
        let record = match record {
            Ok(r) => r,
            Err(_) => continue,
        };

        if record.get(idx_run_id).unwrap_or("").trim() != run_id {
            continue;
        }

        let row_schema = record.get(idx_schema).unwrap_or("").trim();
        if !row_schema.eq_ignore_ascii_case(SCHEMA_VERSION) {
            continue;
        }

        let legs_n = match record.get(idx_legs_n).and_then(parse_u64) {
            Some(v) => v as usize,
            None => {
                base.bad();
                dump10.bad();
                fill70.bad();
                dump10_fill70.bad();
                continue;
            }
        };
        if !(2..=3).contains(&legs_n) {
            base.bad();
            dump10.bad();
            fill70.bad();
            dump10_fill70.bad();
            continue;
        }

        let q_req = match record.get(idx_q_req).and_then(parse_f64) {
            Some(v) => v,
            None => {
                base.bad();
                dump10.bad();
                fill70.bad();
                dump10_fill70.bad();
                continue;
            }
        };

        let fill_share_base = match record.get(idx_fill_share).and_then(parse_f64) {
            Some(v) => v,
            None => {
                base.bad();
                dump10.bad();
                fill70.bad();
                dump10_fill70.bad();
                continue;
            }
        };
        let dump_base = record.get(idx_dump).and_then(parse_f64).unwrap_or(0.05);

        let mut legs: Vec<RecomputeLeg> = Vec::with_capacity(legs_n);
        for (i, idxs) in [leg0, leg1, leg2].into_iter().enumerate() {
            if i >= legs_n {
                break;
            }
            let p_limit = match record.get(idxs.p_limit).and_then(parse_f64) {
                Some(v) => v,
                None => {
                    legs.clear();
                    break;
                }
            };
            let v_mkt = match record.get(idxs.v_mkt).and_then(parse_f64) {
                Some(v) => v,
                None => {
                    legs.clear();
                    break;
                }
            };
            let best_bid = record.get(idxs.best_bid).and_then(parse_f64).unwrap_or(0.0);
            legs.push(RecomputeLeg {
                p_limit,
                best_bid,
                v_mkt,
            });
        }

        if legs.len() != legs_n {
            base.bad();
            dump10.bad();
            fill70.bad();
            dump10_fill70.bad();
            continue;
        }

        let (pnl_base, sr_base) = recompute_ledger_row(q_req, &legs, fill_share_base, dump_base);
        base.ok(pnl_base, sr_base);

        let (pnl_dump10, sr_dump10) = recompute_ledger_row(q_req, &legs, fill_share_base, 0.10);
        dump10.ok(pnl_dump10, sr_dump10);

        let (pnl_fill70, sr_fill70) =
            recompute_ledger_row(q_req, &legs, fill_share_base * 0.70, dump_base);
        fill70.ok(pnl_fill70, sr_fill70);

        let (pnl_dump10_fill70, sr_dump10_fill70) =
            recompute_ledger_row(q_req, &legs, fill_share_base * 0.70, 0.10);
        dump10_fill70.ok(pnl_dump10_fill70, sr_dump10_fill70);
    }

    Ok(StressSummary {
        baseline: base.finish(),
        dump_0_10: dump10.finish(),
        fill_share_x0_70: fill70.finish(),
        dump_0_10_fill_share_x0_70: dump10_fill70.finish(),
    })
}

#[derive(Clone, Copy)]
struct StressLegIdxs {
    p_limit: usize,
    best_bid: usize,
    v_mkt: usize,
}

impl StressLegIdxs {
    fn new(header: &csv::StringRecord, i: u8) -> anyhow::Result<Self> {
        let p_limit = find_col(header, &format!("leg{i}_p_limit"))
            .with_context(|| format!("missing column: leg{i}_p_limit"))?;
        let best_bid = find_col(header, &format!("leg{i}_best_bid"))
            .with_context(|| format!("missing column: leg{i}_best_bid"))?;
        let v_mkt = find_col(header, &format!("leg{i}_v_mkt"))
            .with_context(|| format!("missing column: leg{i}_v_mkt"))?;
        Ok(Self {
            p_limit,
            best_bid,
            v_mkt,
        })
    }
}

#[derive(Debug)]
struct StressAgg {
    set_ratio_threshold: f64,
    rows_ok: u64,
    rows_bad: u64,
    sum_total_pnl: f64,
    set_ratio_sum: f64,
    legging_miss: u64,
    pnls: Vec<f64>,
}

impl StressAgg {
    fn new(set_ratio_threshold: f64) -> Self {
        Self {
            set_ratio_threshold,
            rows_ok: 0,
            rows_bad: 0,
            sum_total_pnl: 0.0,
            set_ratio_sum: 0.0,
            legging_miss: 0,
            pnls: Vec::new(),
        }
    }

    fn ok(&mut self, total_pnl: f64, set_ratio: f64) {
        self.rows_ok += 1;
        self.sum_total_pnl += total_pnl;
        self.set_ratio_sum += set_ratio;
        if set_ratio < self.set_ratio_threshold {
            self.legging_miss += 1;
        }
        self.pnls.push(total_pnl);
    }

    fn bad(&mut self) {
        self.rows_bad += 1;
    }

    fn finish(mut self) -> StressMetrics {
        self.pnls
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let worst_n = self.pnls.len().min(20);
        let worst_20_pnl_sum: f64 = self.pnls.iter().take(worst_n).sum();

        let set_ratio_avg = if self.rows_ok == 0 {
            0.0
        } else {
            self.set_ratio_sum / (self.rows_ok as f64)
        };
        let legging_rate = if self.rows_ok == 0 {
            0.0
        } else {
            (self.legging_miss as f64) / (self.rows_ok as f64)
        };

        StressMetrics {
            rows_ok: self.rows_ok,
            rows_bad: self.rows_bad,
            total_pnl_sum: self.sum_total_pnl,
            set_ratio_avg,
            legging_rate,
            worst_20_pnl_sum,
        }
    }
}

pub fn run_shadow_sweep(
    input: &Path,
    run_id: Option<&str>,
    grid: SweepGrid,
    out_dir: &Path,
) -> anyhow::Result<ShadowSweepResult> {
    std::fs::create_dir_all(out_dir).with_context(|| format!("create {}", out_dir.display()))?;

    let inferred_run_id = match run_id {
        Some(v) => v.to_string(),
        None => infer_last_run_id(input).context("infer run_id from shadow_log.csv")?,
    };

    let (ledger_rows, rows_total, rows_bad) =
        parse_ledger_rows(input, &inferred_run_id).context("parse shadow_log ledger rows")?;
    let rows_ok = ledger_rows.len() as u64;

    let grid = grid.sanitize();
    if grid.fill_share_liquid_values.is_empty()
        || grid.fill_share_thin_values.is_empty()
        || grid.dump_slippage_values.is_empty()
    {
        anyhow::bail!("sweep grid is empty after sanitization");
    }

    let mut scores: Vec<SweepScoreRow> = Vec::new();

    for &fill_share_liquid in &grid.fill_share_liquid_values {
        for &fill_share_thin in &grid.fill_share_thin_values {
            for &dump_slippage_assumed in &grid.dump_slippage_values {
                let (sum_total_pnl, set_ratio_avg, legging_rate, worst_20_pnl_sum) =
                    aggregate_combo(
                        &ledger_rows,
                        fill_share_liquid,
                        fill_share_thin,
                        dump_slippage_assumed,
                        grid.set_ratio_threshold,
                    );
                let total_pnl_avg = if rows_ok == 0 {
                    0.0
                } else {
                    sum_total_pnl / (rows_ok as f64)
                };

                scores.push(SweepScoreRow {
                    run_id: inferred_run_id.clone(),
                    rows_total,
                    rows_ok,
                    rows_bad,
                    fill_share_liquid,
                    fill_share_thin,
                    dump_slippage_assumed,
                    set_ratio_threshold: grid.set_ratio_threshold,
                    total_pnl_sum: sum_total_pnl,
                    total_pnl_avg,
                    set_ratio_avg,
                    legging_rate,
                    worst_20_pnl_sum,
                });
            }
        }
    }

    let best = select_best(&scores);

    write_sweep_scores_csv(out_dir, &scores).context("write sweep_scores.csv")?;
    write_best_patch_toml(out_dir, &best, grid.set_ratio_threshold)
        .context("write best_patch.toml")?;
    write_sweep_recommendation_json(
        out_dir,
        input,
        &inferred_run_id,
        rows_total,
        rows_ok,
        rows_bad,
        &grid,
        &best,
        &scores,
    )
    .context("write sweep_recommendation.json")?;

    Ok(ShadowSweepResult {
        run_id: inferred_run_id,
        rows_total,
        rows_ok,
        rows_bad,
        scores,
        best,
        out_dir: out_dir.to_path_buf(),
    })
}

fn aggregate_combo(
    rows: &[LedgerRow],
    fill_share_liquid: f64,
    fill_share_thin: f64,
    dump_slippage_assumed: f64,
    set_ratio_threshold: f64,
) -> (f64, f64, f64, f64) {
    let mut total_pnls: Vec<f64> = Vec::with_capacity(rows.len());
    let mut sum_total_pnl: f64 = 0.0;
    let mut set_ratio_sum: f64 = 0.0;
    let mut legging_miss: u64 = 0;

    for row in rows {
        let fill_share = match row.bucket {
            BucketKey::Liquid => fill_share_liquid,
            BucketKey::Thin => fill_share_thin,
        };

        let legs: Vec<RecomputeLeg> = row
            .legs
            .iter()
            .map(|l| RecomputeLeg {
                p_limit: l.p_limit,
                best_bid: l.best_bid,
                v_mkt: l.v_mkt,
            })
            .collect();
        let (total_pnl, set_ratio) =
            recompute_ledger_row(row.q_req, &legs, fill_share, dump_slippage_assumed);
        sum_total_pnl += total_pnl;
        total_pnls.push(total_pnl);
        set_ratio_sum += set_ratio;
        if set_ratio < set_ratio_threshold {
            legging_miss += 1;
        }
    }

    let n = rows.len() as f64;
    let set_ratio_avg = if n == 0.0 { 0.0 } else { set_ratio_sum / n };
    let legging_rate = if n == 0.0 {
        0.0
    } else {
        (legging_miss as f64) / n
    };

    total_pnls.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let worst_n = total_pnls.len().min(20);
    let worst_20_pnl_sum: f64 = total_pnls.iter().take(worst_n).sum();

    (sum_total_pnl, set_ratio_avg, legging_rate, worst_20_pnl_sum)
}

fn select_best(scores: &[SweepScoreRow]) -> Option<SweepScoreRow> {
    let mut sorted: Vec<&SweepScoreRow> = scores.iter().collect();
    sorted.sort_by(|a, b| {
        cmp_f64_desc(a.total_pnl_sum, b.total_pnl_sum)
            .then_with(|| cmp_f64_desc(a.set_ratio_avg, b.set_ratio_avg))
            .then_with(|| cmp_f64_asc(a.legging_rate, b.legging_rate))
            .then_with(|| cmp_f64_desc(a.worst_20_pnl_sum, b.worst_20_pnl_sum))
            .then_with(|| {
                // Deterministic tie-break: compare the parameter tuple as strings.
                let ak = format!(
                    "{:.6},{:.6},{:.6}",
                    a.fill_share_liquid, a.fill_share_thin, a.dump_slippage_assumed
                );
                let bk = format!(
                    "{:.6},{:.6},{:.6}",
                    b.fill_share_liquid, b.fill_share_thin, b.dump_slippage_assumed
                );
                ak.cmp(&bk)
            })
    });
    sorted.first().map(|r| (*r).clone())
}

fn write_sweep_scores_csv(out_dir: &Path, rows: &[SweepScoreRow]) -> anyhow::Result<()> {
    let path = out_dir.join(FILE_SWEEP_SCORES);
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(false)
        .from_path(&path)
        .with_context(|| format!("open {}", path.display()))?;
    wtr.write_record(SWEEP_SCORES_HEADER)
        .context("write header")?;
    for r in rows {
        wtr.write_record(r.to_record()).context("write row")?;
    }
    wtr.flush().context("flush sweep_scores.csv")?;
    Ok(())
}

fn write_best_patch_toml(
    out_dir: &Path,
    best: &Option<SweepScoreRow>,
    set_ratio_threshold: f64,
) -> anyhow::Result<()> {
    let path = out_dir.join(FILE_BEST_PATCH);
    let now_ms = crate::types::now_ms();

    let content = match best {
        Some(b) => format!(
            "[shadow_sweep_best]\nrun_id = \"{}\"\ngenerated_at_ms = {}\nrows_ok = {}\ntotal_pnl_sum = {:.6}\nset_ratio_avg = {:.6}\nlegging_rate = {:.6}\nworst_20_pnl_sum = {:.6}\n\n[buckets]\nfill_share_liquid_p25 = {:.6}\nfill_share_thin_p25 = {:.6}\n\n[shadow_sweep]\ndump_slippage_assumed = {:.6}\nset_ratio_threshold = {:.6}\n",
            b.run_id,
            now_ms,
            b.rows_ok,
            b.total_pnl_sum,
            b.set_ratio_avg,
            b.legging_rate,
            b.worst_20_pnl_sum,
            b.fill_share_liquid,
            b.fill_share_thin,
            b.dump_slippage_assumed,
            set_ratio_threshold,
        ),
        None => format!(
            "[shadow_sweep_best]\nrun_id = \"\"\ngenerated_at_ms = {now_ms}\ninsufficient_data = true\nset_ratio_threshold = {set_ratio_threshold:.6}\n",
        ),
    };

    std::fs::write(&path, content.as_bytes())
        .with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_sweep_recommendation_json(
    out_dir: &Path,
    input: &Path,
    run_id: &str,
    rows_total: u64,
    rows_ok: u64,
    rows_bad: u64,
    grid: &SweepGrid,
    best: &Option<SweepScoreRow>,
    scores: &[SweepScoreRow],
) -> anyhow::Result<()> {
    let path = out_dir.join(FILE_SWEEP_RECOMMENDATION);

    let mut top: Vec<SweepScoreRow> = scores.to_vec();
    top.sort_by(|a, b| {
        cmp_f64_desc(a.total_pnl_sum, b.total_pnl_sum)
            .then_with(|| cmp_f64_desc(a.set_ratio_avg, b.set_ratio_avg))
            .then_with(|| cmp_f64_asc(a.legging_rate, b.legging_rate))
            .then_with(|| cmp_f64_desc(a.worst_20_pnl_sum, b.worst_20_pnl_sum))
    });
    top.truncate(10);

    let out = SweepRecommendation {
        version: "shadow_sweep_v1".to_string(),
        input: input.display().to_string(),
        run_id: run_id.to_string(),
        rows_total,
        rows_ok,
        rows_bad,
        grid: GridOut {
            fill_share_liquid_values: grid.fill_share_liquid_values.clone(),
            fill_share_thin_values: grid.fill_share_thin_values.clone(),
            dump_slippage_values: grid.dump_slippage_values.clone(),
            set_ratio_threshold: grid.set_ratio_threshold,
        },
        selection_rule: "max total_pnl_sum, then max set_ratio_avg, then min legging_rate, then max worst_20_pnl_sum".to_string(),
        best: best.clone(),
        top,
    };

    let json = serde_json::to_vec_pretty(&out).context("serialize sweep_recommendation.json")?;
    std::fs::write(&path, json).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

#[derive(Debug, Serialize)]
struct SweepRecommendation {
    pub version: String,
    pub input: String,
    pub run_id: String,
    pub rows_total: u64,
    pub rows_ok: u64,
    pub rows_bad: u64,
    pub grid: GridOut,
    pub selection_rule: String,
    pub best: Option<SweepScoreRow>,
    pub top: Vec<SweepScoreRow>,
}

#[derive(Debug, Serialize)]
struct GridOut {
    pub fill_share_liquid_values: Vec<f64>,
    pub fill_share_thin_values: Vec<f64>,
    pub dump_slippage_values: Vec<f64>,
    pub set_ratio_threshold: f64,
}

fn parse_ledger_rows(input: &Path, run_id: &str) -> anyhow::Result<(Vec<LedgerRow>, u64, u64)> {
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .from_path(input)
        .with_context(|| format!("open {}", input.display()))?;

    let header = rdr
        .headers()
        .with_context(|| format!("read header {}", input.display()))?
        .clone();

    let idx_run_id = find_col(&header, "run_id").context("missing column: run_id")?;
    let idx_schema =
        find_col(&header, "schema_version").context("missing column: schema_version")?;
    let idx_bucket = find_col(&header, "bucket").context("missing column: bucket")?;
    let idx_legs_n = find_col(&header, "legs_n").context("missing column: legs_n")?;
    let idx_q_req = find_col(&header, "q_req").context("missing column: q_req")?;

    let leg0 = LegIdxs::new(&header, 0)?;
    let leg1 = LegIdxs::new(&header, 1)?;
    let leg2 = LegIdxs::new(&header, 2)?;

    // Counts are scoped to rows that match `(run_id, schema_version)`.
    let mut rows_total: u64 = 0;
    let mut rows_bad: u64 = 0;
    let mut out: Vec<LedgerRow> = Vec::new();

    for record in rdr.records() {
        let record = match record {
            Ok(r) => r,
            Err(_) => continue,
        };

        if record.get(idx_run_id).unwrap_or("").trim() != run_id {
            continue;
        }

        let row_schema = record.get(idx_schema).unwrap_or("").trim();
        if !row_schema.eq_ignore_ascii_case(SCHEMA_VERSION) {
            continue;
        }

        rows_total += 1;

        let bucket = match record.get(idx_bucket).and_then(BucketKey::parse) {
            Some(v) => v,
            None => {
                rows_bad += 1;
                continue;
            }
        };

        let legs_n = match record.get(idx_legs_n).and_then(parse_u64) {
            Some(v) => v as usize,
            None => {
                rows_bad += 1;
                continue;
            }
        };
        if !(2..=3).contains(&legs_n) {
            rows_bad += 1;
            continue;
        }

        let q_req = match record.get(idx_q_req).and_then(parse_f64) {
            Some(v) => v,
            None => {
                rows_bad += 1;
                continue;
            }
        };

        let mut legs: Vec<LedgerLeg> = Vec::with_capacity(legs_n);
        for (i, idxs) in [leg0, leg1, leg2].into_iter().enumerate() {
            if i >= legs_n {
                break;
            }
            let p_limit = match record.get(idxs.p_limit).and_then(parse_f64) {
                Some(v) => v,
                None => {
                    rows_bad += 1;
                    continue;
                }
            };
            let v_mkt = match record.get(idxs.v_mkt).and_then(parse_f64) {
                Some(v) => v,
                None => {
                    rows_bad += 1;
                    continue;
                }
            };
            let best_bid = record.get(idxs.best_bid).and_then(parse_f64).unwrap_or(0.0);
            legs.push(LedgerLeg {
                p_limit,
                best_bid,
                v_mkt,
            });
        }

        if legs.len() != legs_n {
            rows_bad += 1;
            continue;
        }

        out.push(LedgerRow {
            bucket,
            q_req,
            legs,
        });
    }

    Ok((out, rows_total, rows_bad))
}

#[derive(Clone, Copy)]
struct LegIdxs {
    p_limit: usize,
    best_bid: usize,
    v_mkt: usize,
}

impl LegIdxs {
    fn new(header: &csv::StringRecord, i: u8) -> anyhow::Result<Self> {
        let p_limit = find_col(header, &format!("leg{i}_p_limit"))
            .with_context(|| format!("missing column: leg{i}_p_limit"))?;
        let best_bid = find_col(header, &format!("leg{i}_best_bid"))
            .with_context(|| format!("missing column: leg{i}_best_bid"))?;
        let v_mkt = find_col(header, &format!("leg{i}_v_mkt"))
            .with_context(|| format!("missing column: leg{i}_v_mkt"))?;
        Ok(Self {
            p_limit,
            best_bid,
            v_mkt,
        })
    }
}

fn infer_last_run_id(path: &Path) -> anyhow::Result<String> {
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .from_path(path)
        .with_context(|| format!("open {}", path.display()))?;

    let header = rdr
        .headers()
        .with_context(|| format!("read header {}", path.display()))?
        .clone();
    let idx_run_id = find_col(&header, "run_id").context("missing column: run_id")?;

    let mut last: Option<String> = None;
    for record in rdr.records() {
        let record = match record {
            Ok(r) => r,
            Err(_) => continue,
        };
        let v = record.get(idx_run_id).unwrap_or("").trim();
        if !v.is_empty() {
            last = Some(v.to_string());
        }
    }
    last.context("run_id not found in shadow_log.csv")
}

fn find_col(header: &csv::StringRecord, name: &str) -> Option<usize> {
    header
        .iter()
        .position(|h| h.trim().eq_ignore_ascii_case(name))
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
    use assert_approx_eq::assert_approx_eq;

    use super::*;

    #[test]
    fn sweep_scores_header_is_frozen() {
        assert_eq!(SWEEP_SCORES_HEADER.join(","), "run_id,rows_total,rows_ok,rows_bad,fill_share_liquid,fill_share_thin,dump_slippage_assumed,set_ratio_threshold,total_pnl_sum,total_pnl_avg,set_ratio_avg,legging_rate,worst_20_pnl_sum");
    }

    #[test]
    fn recompute_matches_spec_for_simple_binary() {
        let row = LedgerRow {
            bucket: BucketKey::Liquid,
            q_req: 10.0,
            legs: vec![
                LedgerLeg {
                    p_limit: 0.49,
                    best_bid: 0.48,
                    v_mkt: 100.0,
                },
                LedgerLeg {
                    p_limit: 0.48,
                    best_bid: 0.47,
                    v_mkt: 60.0,
                },
            ],
        };

        let (sum_pnl, set_ratio_avg, legging_rate, worst_20) =
            aggregate_combo(&[row], 0.10, 0.10, 0.05, 0.85);

        // q_fill0 = min(10, 100*0.1)=10
        // q_fill1 = min(10, 60*0.1)=6
        // q_set = 6, q_left0=4
        // pnl_set = 6*(0.999 - 1.02*(0.49+0.48)) = 6*(0.999 - 0.9894)=0.0576
        // exit0=0.48*0.95=0.456, pnl_left0 = 4*(0.98*0.456 - 1.02*0.49) = -0.21168
        // total = -0.15408
        assert_approx_eq!(sum_pnl, -0.15408, 1e-6);
        assert_approx_eq!(worst_20, -0.15408, 1e-6);
        assert_approx_eq!(set_ratio_avg, 6.0 / 8.0, 1e-9);
        assert_approx_eq!(legging_rate, 1.0, 1e-12);
    }
}
