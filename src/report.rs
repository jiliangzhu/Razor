use std::path::Path;

use anyhow::Context as _;
use serde::Serialize;

use crate::schema::{FILE_REPORT_JSON, FILE_REPORT_MD, FILE_SHADOW_LOG, SCHEMA_VERSION};

#[derive(Clone, Copy, Debug)]
pub struct ReportThresholds {
    pub min_total_shadow_pnl: f64,
    pub min_avg_set_ratio: f64,
}

impl Default for ReportThresholds {
    fn default() -> Self {
        Self {
            min_total_shadow_pnl: 0.0,
            min_avg_set_ratio: 0.85,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub schema_version: String,
    pub run_id: String,
    pub period: Period,
    pub totals: Totals,
    pub by_bucket: ByBucket,
    pub by_strategy: ByStrategy,
    pub worst_20: Vec<WorstEntry>,
    pub verdict: Verdict,

    #[serde(skip_serializing)]
    pub rows_total: u64,
    #[serde(skip_serializing)]
    pub rows_bad: u64,
}

#[derive(Debug, Serialize)]
pub struct Period {
    pub start_unix_ms: u64,
    pub end_unix_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct Totals {
    pub signals: u64,
    pub total_shadow_pnl: f64,
    pub avg_set_ratio: f64,
}

#[derive(Debug, Default, Serialize)]
pub struct BucketStats {
    pub signals: u64,
    pub pnl: f64,
    pub avg_set_ratio: f64,
}

#[derive(Debug, Default, Serialize)]
pub struct ByBucket {
    pub liquid: BucketStats,
    pub thin: BucketStats,
}

#[derive(Debug, Default, Serialize)]
pub struct ByStrategy {
    pub binary: BucketStats,
    pub triangle: BucketStats,
}

#[derive(Debug, Serialize)]
pub struct WorstEntry {
    pub signal_id: u64,
    pub market_id: String,
    pub strategy: String,
    pub bucket: String,
    pub total_pnl: f64,
    pub set_ratio: f64,
}

#[derive(Debug, Serialize)]
pub struct Verdict {
    pub go: bool,
    pub reasons: Vec<String>,
    pub thresholds: VerdictThresholds,
}

#[derive(Debug, Serialize)]
pub struct VerdictThresholds {
    pub min_total_shadow_pnl: f64,
    pub min_avg_set_ratio: f64,
}

pub fn generate_report_files(
    data_dir: &Path,
    run_id: &str,
    thresholds: ReportThresholds,
) -> anyhow::Result<Report> {
    let shadow_path = data_dir.join(FILE_SHADOW_LOG);
    let out_json = data_dir.join(FILE_REPORT_JSON);
    let out_md = data_dir.join(FILE_REPORT_MD);

    let report = compute_report(&shadow_path, run_id, thresholds)?;

    let json = serde_json::to_vec_pretty(&report).context("serialize report.json")?;
    std::fs::write(&out_json, json).with_context(|| format!("write {}", out_json.display()))?;

    let md = render_report_md(&report);
    std::fs::write(&out_md, md.as_bytes())
        .with_context(|| format!("write {}", out_md.display()))?;

    Ok(report)
}

pub fn compute_report(
    shadow_log_path: &Path,
    run_id: &str,
    thresholds: ReportThresholds,
) -> anyhow::Result<Report> {
    if !shadow_log_path.exists() {
        let (go, reasons) = verdict(0.0, 0.0, thresholds);
        return Ok(Report {
            schema_version: SCHEMA_VERSION.to_string(),
            run_id: run_id.to_string(),
            period: Period {
                start_unix_ms: 0,
                end_unix_ms: 0,
            },
            totals: Totals {
                signals: 0,
                total_shadow_pnl: 0.0,
                avg_set_ratio: 0.0,
            },
            by_bucket: ByBucket::default(),
            by_strategy: ByStrategy::default(),
            worst_20: Vec::new(),
            verdict: Verdict {
                go,
                reasons: vec!["shadow_log.csv missing".to_string()]
                    .into_iter()
                    .chain(reasons)
                    .collect(),
                thresholds: VerdictThresholds {
                    min_total_shadow_pnl: thresholds.min_total_shadow_pnl,
                    min_avg_set_ratio: thresholds.min_avg_set_ratio,
                },
            },
            rows_total: 0,
            rows_bad: 0,
        });
    }

    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .from_path(shadow_log_path)
        .with_context(|| format!("open {}", shadow_log_path.display()))?;

    let header = rdr
        .headers()
        .with_context(|| format!("read header {}", shadow_log_path.display()))?
        .clone();
    let meta = HeaderMeta::new(&header)?;

    let mut rows_total: u64 = 0;
    let mut rows_bad: u64 = 0;

    let mut min_ts: Option<u64> = None;
    let mut max_ts: Option<u64> = None;

    let mut totals_signals: u64 = 0;
    let mut total_shadow_pnl: f64 = 0.0;
    let mut set_ratio_sum: f64 = 0.0;

    let mut acc_bucket_liquid = Accum::default();
    let mut acc_bucket_thin = Accum::default();
    let mut acc_strategy_binary = Accum::default();
    let mut acc_strategy_triangle = Accum::default();

    let mut worst: Vec<WorstEntry> = Vec::new();

    for record in rdr.records() {
        rows_total += 1;
        let record = match record {
            Ok(r) => r,
            Err(_) => {
                rows_bad += 1;
                continue;
            }
        };

        let row = match parse_row(&record, &meta, run_id) {
            Some(v) => v,
            None => {
                rows_bad += 1;
                continue;
            }
        };

        match row {
            RowParse::OtherRun => {}
            RowParse::Bad => {
                rows_bad += 1;
            }
            RowParse::Ok(r) => {
                let bucket = match r.bucket.as_str() {
                    "liquid" => "liquid",
                    "thin" => "thin",
                    _ => {
                        rows_bad += 1;
                        continue;
                    }
                };
                let strategy = match r.strategy.as_str() {
                    "binary" => "binary",
                    "triangle" => "triangle",
                    _ => {
                        rows_bad += 1;
                        continue;
                    }
                };

                totals_signals += 1;
                total_shadow_pnl += r.total_pnl;
                set_ratio_sum += r.set_ratio;

                min_ts = Some(min_ts.map_or(r.signal_ts_unix_ms, |v| v.min(r.signal_ts_unix_ms)));
                max_ts = Some(max_ts.map_or(r.signal_ts_unix_ms, |v| v.max(r.signal_ts_unix_ms)));

                match bucket {
                    "liquid" => acc_bucket_liquid.push(r.total_pnl, r.set_ratio),
                    "thin" => acc_bucket_thin.push(r.total_pnl, r.set_ratio),
                    _ => unreachable!("validated bucket"),
                }
                match strategy {
                    "binary" => acc_strategy_binary.push(r.total_pnl, r.set_ratio),
                    "triangle" => acc_strategy_triangle.push(r.total_pnl, r.set_ratio),
                    _ => unreachable!("validated strategy"),
                }

                worst.push(WorstEntry {
                    signal_id: r.signal_id,
                    market_id: r.market_id,
                    strategy: strategy.to_string(),
                    bucket: bucket.to_string(),
                    total_pnl: r.total_pnl,
                    set_ratio: r.set_ratio,
                });
            }
        }
    }

    worst.sort_by(|a, b| {
        a.total_pnl
            .partial_cmp(&b.total_pnl)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    if worst.len() > 20 {
        worst.truncate(20);
    }

    let avg_set_ratio = if totals_signals > 0 {
        set_ratio_sum / (totals_signals as f64)
    } else {
        0.0
    };

    let (go, reasons) = verdict(total_shadow_pnl, avg_set_ratio, thresholds);

    Ok(Report {
        schema_version: SCHEMA_VERSION.to_string(),
        run_id: run_id.to_string(),
        period: Period {
            start_unix_ms: min_ts.unwrap_or(0),
            end_unix_ms: max_ts.unwrap_or(0),
        },
        totals: Totals {
            signals: totals_signals,
            total_shadow_pnl,
            avg_set_ratio,
        },
        by_bucket: ByBucket {
            liquid: acc_bucket_liquid.finish(),
            thin: acc_bucket_thin.finish(),
        },
        by_strategy: ByStrategy {
            binary: acc_strategy_binary.finish(),
            triangle: acc_strategy_triangle.finish(),
        },
        worst_20: worst,
        verdict: Verdict {
            go,
            reasons,
            thresholds: VerdictThresholds {
                min_total_shadow_pnl: thresholds.min_total_shadow_pnl,
                min_avg_set_ratio: thresholds.min_avg_set_ratio,
            },
        },
        rows_total,
        rows_bad,
    })
}

fn verdict(
    total_shadow_pnl: f64,
    avg_set_ratio: f64,
    thresholds: ReportThresholds,
) -> (bool, Vec<String>) {
    let mut reasons: Vec<String> = Vec::new();

    let pnl_ok = total_shadow_pnl > thresholds.min_total_shadow_pnl;
    if pnl_ok {
        reasons.push(format!(
            "TotalShadowPnL > {}",
            thresholds.min_total_shadow_pnl
        ));
    } else {
        reasons.push(format!(
            "TotalShadowPnL <= {}",
            thresholds.min_total_shadow_pnl
        ));
    }

    let ratio_ok = avg_set_ratio >= thresholds.min_avg_set_ratio;
    if ratio_ok {
        reasons.push(format!("AvgSetRatio >= {}", thresholds.min_avg_set_ratio));
    } else {
        reasons.push(format!("AvgSetRatio < {}", thresholds.min_avg_set_ratio));
    }

    (pnl_ok && ratio_ok, reasons)
}

fn render_report_md(report: &Report) -> String {
    let verdict_str = if report.verdict.go { "GO" } else { "NO GO" };

    let mut out = String::new();
    out.push_str("# Razor Day14 Report\n\n");
    out.push_str(&format!("schema_version: `{}`\n\n", report.schema_version));
    out.push_str(&format!("run_id: `{}`\n\n", report.run_id));
    out.push_str(&format!(
        "period: {} .. {}\n\n",
        report.period.start_unix_ms, report.period.end_unix_ms
    ));

    out.push_str("## Totals\n\n");
    out.push_str(&format!("- signals: {}\n", report.totals.signals));
    out.push_str(&format!(
        "- total_shadow_pnl: {:.6}\n",
        report.totals.total_shadow_pnl
    ));
    out.push_str(&format!(
        "- avg_set_ratio: {:.6}\n",
        report.totals.avg_set_ratio
    ));
    out.push_str(&format!(
        "- bad_rows: {} / {}\n\n",
        report.rows_bad, report.rows_total
    ));

    out.push_str("## By Bucket\n\n");
    out.push_str("| bucket | signals | pnl | avg_set_ratio |\n");
    out.push_str("|---|---:|---:|---:|\n");
    out.push_str(&format!(
        "| liquid | {} | {:.6} | {:.6} |\n",
        report.by_bucket.liquid.signals,
        report.by_bucket.liquid.pnl,
        report.by_bucket.liquid.avg_set_ratio
    ));
    out.push_str(&format!(
        "| thin | {} | {:.6} | {:.6} |\n\n",
        report.by_bucket.thin.signals,
        report.by_bucket.thin.pnl,
        report.by_bucket.thin.avg_set_ratio
    ));

    out.push_str("## By Strategy\n\n");
    out.push_str("| strategy | signals | pnl | avg_set_ratio |\n");
    out.push_str("|---|---:|---:|---:|\n");
    out.push_str(&format!(
        "| binary | {} | {:.6} | {:.6} |\n",
        report.by_strategy.binary.signals,
        report.by_strategy.binary.pnl,
        report.by_strategy.binary.avg_set_ratio
    ));
    out.push_str(&format!(
        "| triangle | {} | {:.6} | {:.6} |\n\n",
        report.by_strategy.triangle.signals,
        report.by_strategy.triangle.pnl,
        report.by_strategy.triangle.avg_set_ratio
    ));

    out.push_str("## Worst 20\n\n");
    out.push_str("| # | signal_id | market_id | strategy | bucket | total_pnl | set_ratio |\n");
    out.push_str("|---:|---:|---|---|---|---:|---:|\n");
    for (idx, w) in report.worst_20.iter().enumerate() {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {:.6} | {:.6} |\n",
            idx + 1,
            w.signal_id,
            w.market_id,
            w.strategy,
            w.bucket,
            w.total_pnl,
            w.set_ratio
        ));
    }
    if report.worst_20.is_empty() {
        out.push_str("|  |  |  |  |  |  |  |\n");
    }
    out.push('\n');

    out.push_str("## Verdict\n\n");
    out.push_str(&format!(
        "thresholds: min_total_shadow_pnl={}, min_avg_set_ratio={}\n\n",
        report.verdict.thresholds.min_total_shadow_pnl, report.verdict.thresholds.min_avg_set_ratio,
    ));
    out.push_str(&format!(
        "reasons: {}\n\n",
        report.verdict.reasons.join("; ")
    ));
    out.push_str(&format!("VERDICT: {verdict_str}\n"));

    out
}

#[derive(Default)]
struct Accum {
    signals: u64,
    pnl_sum: f64,
    set_ratio_sum: f64,
}

impl Accum {
    fn push(&mut self, pnl: f64, set_ratio: f64) {
        self.signals += 1;
        self.pnl_sum += pnl;
        self.set_ratio_sum += set_ratio;
    }

    fn finish(self) -> BucketStats {
        let avg_set_ratio = if self.signals > 0 {
            self.set_ratio_sum / (self.signals as f64)
        } else {
            0.0
        };
        BucketStats {
            signals: self.signals,
            pnl: self.pnl_sum,
            avg_set_ratio,
        }
    }
}

struct HeaderMeta {
    run_id: usize,
    signal_id: usize,
    signal_ts_unix_ms: usize,
    market_id: usize,
    strategy: usize,
    bucket: usize,
    total_pnl: usize,
    set_ratio: usize,
}

impl HeaderMeta {
    fn new(header: &csv::StringRecord) -> anyhow::Result<Self> {
        let run_id = find_col(header, "run_id").context("missing column: run_id")?;
        let signal_id = find_col(header, "signal_id").context("missing column: signal_id")?;
        let signal_ts_unix_ms =
            find_col(header, "signal_ts_unix_ms").context("missing column: signal_ts_unix_ms")?;
        let market_id = find_col(header, "market_id").context("missing column: market_id")?;
        let strategy = find_col(header, "strategy").context("missing column: strategy")?;
        let bucket = find_col(header, "bucket").context("missing column: bucket")?;
        let total_pnl = find_col(header, "total_pnl").context("missing column: total_pnl")?;
        let set_ratio = find_col(header, "set_ratio").context("missing column: set_ratio")?;

        Ok(Self {
            run_id,
            signal_id,
            signal_ts_unix_ms,
            market_id,
            strategy,
            bucket,
            total_pnl,
            set_ratio,
        })
    }
}

fn find_col(header: &csv::StringRecord, name: &str) -> Option<usize> {
    header
        .iter()
        .position(|h| h.trim().eq_ignore_ascii_case(name))
}

enum RowParse {
    OtherRun,
    Bad,
    Ok(ParsedRow),
}

struct ParsedRow {
    signal_id: u64,
    signal_ts_unix_ms: u64,
    market_id: String,
    strategy: String,
    bucket: String,
    total_pnl: f64,
    set_ratio: f64,
}

fn parse_row(record: &csv::StringRecord, meta: &HeaderMeta, run_id: &str) -> Option<RowParse> {
    let row_run = record.get(meta.run_id)?.trim();
    if row_run.is_empty() {
        return Some(RowParse::Bad);
    }
    if row_run != run_id {
        return Some(RowParse::OtherRun);
    }

    let signal_id = parse_u64(record.get(meta.signal_id)?)?;
    let signal_ts_unix_ms = parse_u64(record.get(meta.signal_ts_unix_ms)?)?;

    let market_id = record.get(meta.market_id)?.trim().to_string();
    if market_id.is_empty() {
        return Some(RowParse::Bad);
    }

    let strategy = record.get(meta.strategy)?.trim().to_ascii_lowercase();
    let bucket = record.get(meta.bucket)?.trim().to_ascii_lowercase();
    if strategy.is_empty() || bucket.is_empty() {
        return Some(RowParse::Bad);
    }

    let total_pnl = parse_f64(record.get(meta.total_pnl)?)?;
    let set_ratio = parse_f64(record.get(meta.set_ratio)?)?;

    Some(RowParse::Ok(ParsedRow {
        signal_id,
        signal_ts_unix_ms,
        market_id,
        strategy,
        bucket,
        total_pnl,
        set_ratio,
    }))
}

fn parse_f64(s: &str) -> Option<f64> {
    let v = s.trim().parse::<f64>().ok()?;
    if v.is_finite() {
        Some(v)
    } else {
        None
    }
}

fn parse_u64(s: &str) -> Option<u64> {
    s.trim().parse::<u64>().ok()
}
