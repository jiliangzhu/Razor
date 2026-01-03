use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use serde::Serialize;

use crate::reasons::parse_notes_reasons;
use crate::run_meta::RunMeta;
use crate::schema::{FILE_SHADOW_LOG, SCHEMA_VERSION};

pub const FILE_RUNS_SUMMARY_CSV: &str = "runs_summary.csv";
pub const FILE_RUNS_SUMMARY_MD: &str = "runs_summary.md";

pub const RUNS_SUMMARY_HEADER: [&str; 24] = [
    "run_id",
    "run_dir",
    "rows_total",
    "rows_ok",
    "rows_bad",
    "rows_schema_mismatch",
    "signals",
    "total_pnl_sum",
    "pnl_set_sum",
    "pnl_left_total_sum",
    "avg_set_ratio",
    "legging_rate",
    "liquid_signals",
    "liquid_pnl_sum",
    "liquid_avg_set_ratio",
    "thin_signals",
    "thin_pnl_sum",
    "thin_avg_set_ratio",
    "unknown_signals",
    "unknown_pnl_sum",
    "unknown_avg_set_ratio",
    "top_reason_1",
    "top_reason_1_count",
    "top_reason_2",
];

const SET_RATIO_THRESHOLD: f64 = 0.85;

#[derive(Debug, Clone, Serialize)]
pub struct RunSummary {
    pub run_id: String,
    pub run_dir: PathBuf,

    pub rows_total: u64,
    pub rows_ok: u64,
    pub rows_bad: u64,
    pub rows_schema_mismatch: u64,

    pub signals: u64,
    pub total_pnl_sum: f64,
    pub pnl_set_sum: f64,
    pub pnl_left_total_sum: f64,
    pub avg_set_ratio: f64,
    pub legging_rate: f64,

    pub by_bucket: BTreeMap<String, BucketAgg>,
    pub by_reason: BTreeMap<String, ReasonAgg>,
    pub by_bucket_reason: BTreeMap<(String, String), ReasonAgg>,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct BucketAgg {
    pub signals: u64,
    pub pnl_sum: f64,
    pub set_ratio_sum: f64,
}

impl BucketAgg {
    fn push(&mut self, pnl: f64, set_ratio: f64) {
        self.signals += 1;
        self.pnl_sum += pnl;
        self.set_ratio_sum += set_ratio;
    }

    pub fn avg_set_ratio(&self) -> f64 {
        if self.signals == 0 {
            0.0
        } else {
            self.set_ratio_sum / (self.signals as f64)
        }
    }
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct ReasonAgg {
    pub count: u64,
    pub sum_pnl: f64,
}

impl ReasonAgg {
    fn push(&mut self, pnl: f64) {
        self.count += 1;
        self.sum_pnl += pnl;
    }
}

pub fn discover_run_dirs(data_dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut out: Vec<PathBuf> = Vec::new();
    if !data_dir.exists() {
        return Ok(out);
    }

    for entry in
        std::fs::read_dir(data_dir).with_context(|| format!("read {}", data_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with("run_") {
            continue;
        }
        if path.join(FILE_SHADOW_LOG).exists() {
            out.push(path);
        }
    }

    out.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
    Ok(out)
}

pub fn summarize_run_dir(run_dir: &Path) -> anyhow::Result<RunSummary> {
    let shadow_path = run_dir.join(FILE_SHADOW_LOG);
    if !shadow_path.exists() {
        anyhow::bail!("missing {}", shadow_path.display());
    }

    let run_id = match RunMeta::read_from_dir(run_dir) {
        Ok(m) => m.run_id,
        Err(_) => infer_last_run_id(&shadow_path)?,
    };

    summarize_shadow_log(&shadow_path, &run_id, run_dir)
}

fn summarize_shadow_log(
    shadow_path: &Path,
    run_id: &str,
    run_dir: &Path,
) -> anyhow::Result<RunSummary> {
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .from_path(shadow_path)
        .with_context(|| format!("open {}", shadow_path.display()))?;

    let header = rdr
        .headers()
        .with_context(|| format!("read header {}", shadow_path.display()))?
        .clone();

    let idx_run_id = find_col(&header, "run_id").context("missing column: run_id")?;
    let idx_schema =
        find_col(&header, "schema_version").context("missing column: schema_version")?;
    let idx_bucket = find_col(&header, "bucket").context("missing column: bucket")?;
    let idx_total_pnl = find_col(&header, "total_pnl").context("missing column: total_pnl")?;
    let idx_pnl_set = find_col(&header, "pnl_set").context("missing column: pnl_set")?;
    let idx_pnl_left =
        find_col(&header, "pnl_left_total").context("missing column: pnl_left_total")?;
    let idx_set_ratio = find_col(&header, "set_ratio").context("missing column: set_ratio")?;
    let idx_notes = find_col(&header, "notes").context("missing column: notes")?;

    let mut rows_total: u64 = 0;
    let mut rows_ok: u64 = 0;
    let mut rows_bad: u64 = 0;
    let mut rows_schema_mismatch: u64 = 0;

    let mut signals: u64 = 0;
    let mut total_pnl_sum: f64 = 0.0;
    let mut pnl_set_sum: f64 = 0.0;
    let mut pnl_left_total_sum: f64 = 0.0;
    let mut set_ratio_sum: f64 = 0.0;
    let mut legging_miss: u64 = 0;

    let mut by_bucket: BTreeMap<String, BucketAgg> = BTreeMap::new();
    let mut by_reason: BTreeMap<String, ReasonAgg> = BTreeMap::new();
    let mut by_bucket_reason: BTreeMap<(String, String), ReasonAgg> = BTreeMap::new();

    for record in rdr.records() {
        rows_total += 1;
        let record = match record {
            Ok(r) => r,
            Err(_) => {
                rows_bad += 1;
                continue;
            }
        };

        if record.get(idx_run_id).unwrap_or("").trim() != run_id {
            continue;
        }

        let row_schema = record.get(idx_schema).unwrap_or("").trim();
        if !row_schema.eq_ignore_ascii_case(SCHEMA_VERSION) {
            rows_schema_mismatch += 1;
            continue;
        }

        let bucket = record
            .get(idx_bucket)
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        let bucket_key = match bucket.as_str() {
            "liquid" => "liquid",
            "thin" => "thin",
            _ => "unknown",
        }
        .to_string();

        let total_pnl = match record.get(idx_total_pnl).and_then(parse_f64) {
            Some(v) => v,
            None => {
                rows_bad += 1;
                continue;
            }
        };
        let pnl_set = match record.get(idx_pnl_set).and_then(parse_f64) {
            Some(v) => v,
            None => {
                rows_bad += 1;
                continue;
            }
        };
        let pnl_left_total = match record.get(idx_pnl_left).and_then(parse_f64) {
            Some(v) => v,
            None => {
                rows_bad += 1;
                continue;
            }
        };
        let set_ratio = match record.get(idx_set_ratio).and_then(parse_f64) {
            Some(v) => v,
            None => {
                rows_bad += 1;
                continue;
            }
        };

        rows_ok += 1;
        signals += 1;
        total_pnl_sum += total_pnl;
        pnl_set_sum += pnl_set;
        pnl_left_total_sum += pnl_left_total;
        set_ratio_sum += set_ratio;
        if set_ratio < SET_RATIO_THRESHOLD {
            legging_miss += 1;
        }

        by_bucket
            .entry(bucket_key.clone())
            .or_default()
            .push(total_pnl, set_ratio);

        let notes = record.get(idx_notes).unwrap_or("");
        for r in parse_notes_reasons(notes) {
            by_reason.entry(r.clone()).or_default().push(total_pnl);
            by_bucket_reason
                .entry((bucket_key.clone(), r))
                .or_default()
                .push(total_pnl);
        }
    }

    let avg_set_ratio = if signals == 0 {
        0.0
    } else {
        set_ratio_sum / (signals as f64)
    };
    let legging_rate = if signals == 0 {
        0.0
    } else {
        (legging_miss as f64) / (signals as f64)
    };

    Ok(RunSummary {
        run_id: run_id.to_string(),
        run_dir: run_dir.to_path_buf(),
        rows_total,
        rows_ok,
        rows_bad,
        rows_schema_mismatch,
        signals,
        total_pnl_sum,
        pnl_set_sum,
        pnl_left_total_sum,
        avg_set_ratio,
        legging_rate,
        by_bucket,
        by_reason,
        by_bucket_reason,
    })
}

pub fn write_runs_summary_csv(out_dir: &Path, runs: &[RunSummary]) -> anyhow::Result<PathBuf> {
    let path = out_dir.join(FILE_RUNS_SUMMARY_CSV);
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(false)
        .from_path(&path)
        .with_context(|| format!("open {}", path.display()))?;

    wtr.write_record(RUNS_SUMMARY_HEADER)
        .context("write header")?;

    for r in runs {
        let liquid = r.by_bucket.get("liquid").cloned().unwrap_or_default();
        let thin = r.by_bucket.get("thin").cloned().unwrap_or_default();
        let unknown = r.by_bucket.get("unknown").cloned().unwrap_or_default();

        let top_reasons = top_reasons(&r.by_reason, 2);
        let top1 = top_reasons.first().cloned().unwrap_or_default();
        let top2 = top_reasons.get(1).cloned().unwrap_or_default();

        let rec: [String; 24] = [
            r.run_id.clone(),
            r.run_dir.display().to_string(),
            r.rows_total.to_string(),
            r.rows_ok.to_string(),
            r.rows_bad.to_string(),
            r.rows_schema_mismatch.to_string(),
            r.signals.to_string(),
            fmt_f64(r.total_pnl_sum),
            fmt_f64(r.pnl_set_sum),
            fmt_f64(r.pnl_left_total_sum),
            fmt_f64(r.avg_set_ratio),
            fmt_f64(r.legging_rate),
            liquid.signals.to_string(),
            fmt_f64(liquid.pnl_sum),
            fmt_f64(liquid.avg_set_ratio()),
            thin.signals.to_string(),
            fmt_f64(thin.pnl_sum),
            fmt_f64(thin.avg_set_ratio()),
            unknown.signals.to_string(),
            fmt_f64(unknown.pnl_sum),
            fmt_f64(unknown.avg_set_ratio()),
            top1.0,
            top1.1.to_string(),
            top2.0,
        ];
        wtr.write_record(rec).context("write row")?;
    }

    wtr.flush().context("flush runs_summary.csv")?;
    Ok(path)
}

pub fn write_runs_summary_md(out_dir: &Path, runs: &[RunSummary]) -> anyhow::Result<PathBuf> {
    let path = out_dir.join(FILE_RUNS_SUMMARY_MD);
    let mut out = String::new();
    out.push_str("# Razor Run Compare\n\n");
    out.push_str("| run_id | signals | total_pnl_sum | avg_set_ratio | legging_rate | liquid_pnl | thin_pnl |\n");
    out.push_str("|---|---:|---:|---:|---:|---:|---:|\n");
    for r in runs {
        let liquid = r.by_bucket.get("liquid").cloned().unwrap_or_default();
        let thin = r.by_bucket.get("thin").cloned().unwrap_or_default();
        out.push_str(&format!(
            "| {} | {} | {:.6} | {:.6} | {:.6} | {:.6} | {:.6} |\n",
            r.run_id,
            r.signals,
            r.total_pnl_sum,
            r.avg_set_ratio,
            r.legging_rate,
            liquid.pnl_sum,
            thin.pnl_sum
        ));
    }
    out.push('\n');

    for r in runs {
        out.push_str(&format!("## Run `{}`\n\n", r.run_id));
        out.push_str(&format!("- run_dir: `{}`\n", r.run_dir.display()));
        out.push_str(&format!(
            "- totals: signals={}, total_pnl_sum={:.6}, pnl_set_sum={:.6}, pnl_left_total_sum={:.6}, avg_set_ratio={:.6}, legging_rate={:.6}\n\n",
            r.signals,
            r.total_pnl_sum,
            r.pnl_set_sum,
            r.pnl_left_total_sum,
            r.avg_set_ratio,
            r.legging_rate,
        ));

        out.push_str("### Top Reasons (global)\n\n");
        out.push_str("| reason | count | sum_pnl |\n");
        out.push_str("|---|---:|---:|\n");
        for (reason, count, sum_pnl) in top_reasons_full(&r.by_reason, 5) {
            out.push_str(&format!("| {reason} | {count} | {sum_pnl:.6} |\n"));
        }
        out.push('\n');

        out.push_str("### Top Reasons by Bucket\n\n");
        for bucket in ["liquid", "thin", "unknown"] {
            out.push_str(&format!("#### bucket={bucket}\n\n"));
            out.push_str("| reason | count | sum_pnl |\n");
            out.push_str("|---|---:|---:|\n");

            let mut agg: BTreeMap<String, ReasonAgg> = BTreeMap::new();
            for ((b, reason), v) in &r.by_bucket_reason {
                if b != bucket {
                    continue;
                }
                agg.insert(reason.clone(), v.clone());
            }
            for (reason, count, sum_pnl) in top_reasons_full(&agg, 5) {
                out.push_str(&format!("| {reason} | {count} | {sum_pnl:.6} |\n"));
            }
            out.push('\n');
        }
    }

    std::fs::write(&path, out.as_bytes()).with_context(|| format!("write {}", path.display()))?;
    Ok(path)
}

fn top_reasons(agg: &BTreeMap<String, ReasonAgg>, n: usize) -> Vec<(String, u64)> {
    let mut v: Vec<(&String, &ReasonAgg)> = agg.iter().collect();
    v.sort_by(|(ra, a), (rb, b)| b.count.cmp(&a.count).then_with(|| ra.cmp(rb)));
    v.into_iter()
        .take(n)
        .map(|(r, a)| (r.clone(), a.count))
        .collect()
}

fn top_reasons_full(agg: &BTreeMap<String, ReasonAgg>, n: usize) -> Vec<(String, u64, f64)> {
    let mut v: Vec<(&String, &ReasonAgg)> = agg.iter().collect();
    v.sort_by(|(ra, a), (rb, b)| b.count.cmp(&a.count).then_with(|| ra.cmp(rb)));
    v.into_iter()
        .take(n)
        .map(|(r, a)| (r.clone(), a.count, a.sum_pnl))
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SHADOW_HEADER;

    #[test]
    fn runs_summary_header_is_frozen() {
        assert_eq!(RUNS_SUMMARY_HEADER.join(","), "run_id,run_dir,rows_total,rows_ok,rows_bad,rows_schema_mismatch,signals,total_pnl_sum,pnl_set_sum,pnl_left_total_sum,avg_set_ratio,legging_rate,liquid_signals,liquid_pnl_sum,liquid_avg_set_ratio,thin_signals,thin_pnl_sum,thin_avg_set_ratio,unknown_signals,unknown_pnl_sum,unknown_avg_set_ratio,top_reason_1,top_reason_1_count,top_reason_2");
    }

    #[test]
    fn summarizes_basic_metrics_and_reasons() {
        let tmp = std::env::temp_dir().join(format!(
            "razor_run_compare_test_{}_{}",
            std::process::id(),
            crate::types::now_ms()
        ));
        std::fs::create_dir_all(&tmp).expect("create tmp dir");

        // Minimal run_meta.json so summarize_run_dir uses it.
        let meta = RunMeta {
            run_id: "run_x".to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            git_sha: "unknown".to_string(),
            start_ts_unix_ms: 0,
            config_path: "config.toml".to_string(),
            trade_ts_source: "local".to_string(),
            notes_enum_version: "v1".to_string(),
            trade_poll_taker_only: None,
            sim_stress: crate::run_meta::SimStressProfile::default(),
        };
        meta.write_to_dir(&tmp).expect("write run_meta.json");

        let mut csv = String::new();
        csv.push_str(&SHADOW_HEADER.join(","));
        csv.push('\n');

        let mut row1: Vec<String> = vec![String::new(); SHADOW_HEADER.len()];
        row1[idx("run_id")] = "run_x".to_string();
        row1[idx("schema_version")] = SCHEMA_VERSION.to_string();
        row1[idx("bucket")] = "liquid".to_string();
        row1[idx("total_pnl")] = "1.0".to_string();
        row1[idx("pnl_set")] = "0.5".to_string();
        row1[idx("pnl_left_total")] = "0.5".to_string();
        row1[idx("set_ratio")] = "0.9".to_string();
        row1[idx("notes")] = "NO_TRADES".to_string();
        csv.push_str(&row1.join(","));
        csv.push('\n');

        let mut row2: Vec<String> = vec![String::new(); SHADOW_HEADER.len()];
        row2[idx("run_id")] = "run_x".to_string();
        row2[idx("schema_version")] = SCHEMA_VERSION.to_string();
        row2[idx("bucket")] = "thin".to_string();
        row2[idx("total_pnl")] = "-0.2".to_string();
        row2[idx("pnl_set")] = "-0.1".to_string();
        row2[idx("pnl_left_total")] = "-0.1".to_string();
        row2[idx("set_ratio")] = "0.8".to_string();
        row2[idx("notes")] = "\"MISSING_BID,NO_TRADES\"".to_string();
        csv.push_str(&row2.join(","));
        csv.push('\n');

        std::fs::write(tmp.join(FILE_SHADOW_LOG), csv.as_bytes()).expect("write shadow_log");

        let s = summarize_run_dir(&tmp).expect("summary");
        assert_eq!(s.signals, 2);
        assert!((s.total_pnl_sum - 0.8).abs() < 1e-12);
        assert!((s.pnl_set_sum - 0.4).abs() < 1e-12);
        assert!((s.pnl_left_total_sum - 0.4).abs() < 1e-12);
        assert!((s.avg_set_ratio - 0.85).abs() < 1e-12);
        assert!((s.legging_rate - 0.5).abs() < 1e-12);

        assert_eq!(s.by_reason.get("NO_TRADES").unwrap().count, 2);
        assert_eq!(s.by_reason.get("MISSING_BID").unwrap().count, 1);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    fn idx(name: &str) -> usize {
        SHADOW_HEADER
            .iter()
            .position(|h| h.trim().eq_ignore_ascii_case(name))
            .unwrap_or_else(|| panic!("missing column {name} in SHADOW_HEADER"))
    }
}
