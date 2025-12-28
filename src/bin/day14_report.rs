use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use clap::Parser;

use razor::reasons::parse_notes_reasons;
use razor::report::{compute_report, write_report_files, ReportThresholds};
use razor::run_meta::RunMeta;

#[derive(Parser, Debug)]
#[command(
    name = "day14_report",
    about = "Project Razor Day14 report (report.json + report.md)"
)]
struct Args {
    #[arg(long, default_value = "data/run_latest")]
    data_dir: PathBuf,
    /// Shadow log CSV path.
    #[arg(long, alias = "shadow-log")]
    input: Option<PathBuf>,
    /// If omitted, uses the last non-empty run_id found in shadow_log.csv.
    #[arg(long)]
    run_id: Option<String>,
    #[arg(long, default_value_t = 0.0)]
    min_total_shadow_pnl: f64,
    #[arg(long, default_value_t = 0.85)]
    min_avg_set_ratio: f64,
    /// Optional: used only for displaying PnL% (does not affect verdict).
    #[arg(long)]
    starting_capital: Option<f64>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.data_dir).context("create data_dir")?;

    let shadow_path = args
        .input
        .clone()
        .unwrap_or_else(|| args.data_dir.join(razor::schema::FILE_SHADOW_LOG));

    let run_id = match args.run_id {
        Some(v) => v,
        None => infer_last_run_id(&shadow_path).or_else(|_| {
            RunMeta::read_from_dir(&args.data_dir)
                .map(|m| m.run_id)
                .context("read run_meta.json")
        })?,
    };

    let thresholds = ReportThresholds {
        min_total_shadow_pnl: args.min_total_shadow_pnl,
        min_avg_set_ratio: args.min_avg_set_ratio,
    };

    let report = compute_report(&shadow_path, &run_id, thresholds)?;
    write_report_files(&args.data_dir, &report)?;

    let analysis = analyze_shadow_log(&shadow_path, &run_id)?;

    print_run_meta_section(&args.data_dir, &run_id)?;
    print_overall_verdict_section(&report, args.starting_capital);
    print_by_bucket_section(&analysis.by_bucket, report.totals.signals);
    print_reasons_breakdown_section(&analysis.reasons_global, report.totals.signals);
    print_tail_slice_section(&analysis.tail);

    println!(
        "report_json={}",
        args.data_dir
            .join(razor::schema::FILE_REPORT_JSON)
            .display()
    );
    println!(
        "report_md={}",
        args.data_dir.join(razor::schema::FILE_REPORT_MD).display()
    );

    Ok(())
}

#[derive(Default, Clone)]
struct BucketAgg {
    signals: u64,
    sum_total_pnl: f64,
    set_ratio_sum: f64,
    q_set_sum: f64,
    reason_counts: BTreeMap<String, u64>,
}

impl BucketAgg {
    fn push(&mut self, total_pnl: f64, set_ratio: f64, q_set: f64, reasons: &BTreeSet<String>) {
        self.signals += 1;
        self.sum_total_pnl += total_pnl;
        self.set_ratio_sum += set_ratio;
        self.q_set_sum += q_set;
        for r in reasons {
            *self.reason_counts.entry(r.clone()).or_insert(0) += 1;
        }
    }

    fn avg_set_ratio(&self) -> f64 {
        if self.signals == 0 {
            0.0
        } else {
            self.set_ratio_sum / (self.signals as f64)
        }
    }

    fn avg_q_set(&self) -> f64 {
        if self.signals == 0 {
            0.0
        } else {
            self.q_set_sum / (self.signals as f64)
        }
    }
}

#[derive(Default)]
struct ReasonBucketAgg {
    total: u64,
    liquid: u64,
    thin: u64,
}

struct ShadowAnalysis {
    by_bucket: BTreeMap<String, BucketAgg>,
    reasons_global: BTreeMap<String, ReasonBucketAgg>,
    tail: Vec<TailRow>,
}

#[derive(Debug, Clone)]
struct TailRow {
    signal_id: u64,
    market_id: String,
    bucket: String,
    legs_n: u64,
    q_req: f64,
    q_set: f64,
    total_pnl: f64,
    pnl_left_total: f64,
    notes: String,
}

fn analyze_shadow_log(shadow_log_path: &Path, run_id: &str) -> anyhow::Result<ShadowAnalysis> {
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
    let idx_bucket = find_col(&header, "bucket").context("missing column: bucket")?;
    let idx_total_pnl = find_col(&header, "total_pnl").context("missing column: total_pnl")?;
    let idx_set_ratio = find_col(&header, "set_ratio").context("missing column: set_ratio")?;
    let idx_q_set = find_col(&header, "q_set").context("missing column: q_set")?;
    let idx_q_req = find_col(&header, "q_req").context("missing column: q_req")?;
    let idx_legs_n = find_col(&header, "legs_n").context("missing column: legs_n")?;
    let idx_pnl_left_total =
        find_col(&header, "pnl_left_total").context("missing column: pnl_left_total")?;
    let idx_notes = find_col(&header, "notes").context("missing column: notes")?;
    let idx_market_id = find_col(&header, "market_id").context("missing column: market_id")?;
    let idx_signal_id = find_col(&header, "signal_id").context("missing column: signal_id")?;

    let mut by_bucket: BTreeMap<String, BucketAgg> = BTreeMap::new();
    let mut reasons_global: BTreeMap<String, ReasonBucketAgg> = BTreeMap::new();
    let mut tail: Vec<TailRow> = Vec::new();

    for record in rdr.records() {
        let record = match record {
            Ok(r) => r,
            Err(_) => continue,
        };
        if record.get(idx_run_id).unwrap_or("").trim() != run_id {
            continue;
        }

        let bucket_raw = record
            .get(idx_bucket)
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        let bucket = match bucket_raw.as_str() {
            "liquid" => "liquid",
            "thin" => "thin",
            _ => "unknown",
        }
        .to_string();

        let total_pnl = match record.get(idx_total_pnl).and_then(parse_f64) {
            Some(v) => v,
            None => continue,
        };
        let set_ratio = match record.get(idx_set_ratio).and_then(parse_f64) {
            Some(v) => v,
            None => continue,
        };
        let q_set = match record.get(idx_q_set).and_then(parse_f64) {
            Some(v) => v,
            None => continue,
        };
        let q_req = match record.get(idx_q_req).and_then(parse_f64) {
            Some(v) => v,
            None => continue,
        };
        let legs_n = match record.get(idx_legs_n).and_then(parse_u64) {
            Some(v) => v,
            None => continue,
        };
        let pnl_left_total = match record.get(idx_pnl_left_total).and_then(parse_f64) {
            Some(v) => v,
            None => continue,
        };

        let notes = record.get(idx_notes).unwrap_or("").trim().to_string();
        let market_id = record.get(idx_market_id).unwrap_or("").trim().to_string();
        let signal_id = match record.get(idx_signal_id).and_then(parse_u64) {
            Some(v) => v,
            None => continue,
        };

        let mut reason_set: BTreeSet<String> = BTreeSet::new();
        for r in parse_notes_reasons(&notes) {
            reason_set.insert(r);
        }
        if reason_set.is_empty() {
            reason_set.insert("OK".to_string());
        }

        by_bucket
            .entry(bucket.clone())
            .or_default()
            .push(total_pnl, set_ratio, q_set, &reason_set);

        for r in &reason_set {
            let e = reasons_global.entry(r.clone()).or_default();
            e.total += 1;
            match bucket.as_str() {
                "liquid" => e.liquid += 1,
                "thin" => e.thin += 1,
                _ => {}
            }
        }

        tail.push(TailRow {
            signal_id,
            market_id,
            bucket,
            legs_n,
            q_req,
            q_set,
            total_pnl,
            pnl_left_total,
            notes,
        });
    }

    tail.sort_by(|a, b| {
        a.total_pnl
            .partial_cmp(&b.total_pnl)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    if tail.len() > 20 {
        tail.truncate(20);
    }

    Ok(ShadowAnalysis {
        by_bucket,
        reasons_global,
        tail,
    })
}

fn print_run_meta_section(data_dir: &Path, run_id: &str) -> anyhow::Result<()> {
    println!("== Run Meta ==");
    match RunMeta::read_from_dir(data_dir) {
        Ok(m) => {
            println!("run_id={}", m.run_id);
            println!("schema_version={}", m.schema_version);
            println!("git_sha={}", m.git_sha);
            println!("trade_ts_source={}", m.trade_ts_source);
            println!("start_ts_unix_ms={}", m.start_ts_unix_ms);
            println!("config_path={}", m.config_path);
            println!("notes_enum_version={}", m.notes_enum_version);
        }
        Err(e) => {
            println!("run_id={run_id}");
            println!("run_meta_error={e}");
        }
    }
    println!();
    Ok(())
}

fn print_overall_verdict_section(report: &razor::report::Report, starting_capital: Option<f64>) {
    let decision = if report.verdict.go { "GO" } else { "NO_GO" };
    println!("== Overall Verdict ==");
    println!("count={}", report.totals.signals);
    println!("sum_total_pnl={:.6}", report.totals.total_shadow_pnl);
    println!("avg_set_ratio={:.6}", report.totals.avg_set_ratio);
    if let Some(c) = starting_capital.filter(|v| v.is_finite() && *v > 0.0) {
        let pct = (report.totals.total_shadow_pnl / c) * 100.0;
        println!("starting_capital={c:.6}");
        println!("pnl_pct={pct:.6}");
    }
    println!("GO_NO_GO={decision}");
    println!("reasons={}", report.verdict.reasons.join("; "));
    println!();
}

fn print_by_bucket_section(by_bucket: &BTreeMap<String, BucketAgg>, total_signals: u64) {
    println!("== By Bucket Summary ==");
    println!("bucket,count,sum_total_pnl,avg_set_ratio,avg_q_set,top_reasons");
    for bucket in ["liquid", "thin", "unknown"] {
        let agg = by_bucket.get(bucket).cloned().unwrap_or_default();
        let top = top_reasons(&agg.reason_counts, 5);
        let share = if total_signals > 0 {
            (agg.signals as f64) / (total_signals as f64) * 100.0
        } else {
            0.0
        };
        println!(
            "{bucket},{},{:.6},{:.6},{:.6},{}  # {:.1}%",
            agg.signals,
            agg.sum_total_pnl,
            agg.avg_set_ratio(),
            agg.avg_q_set(),
            top.join(";"),
            share
        );
    }
    println!();
}

fn print_reasons_breakdown_section(
    reasons: &BTreeMap<String, ReasonBucketAgg>,
    total_signals: u64,
) {
    println!("== Reasons Breakdown ==");
    println!("reason,count,pct,liquid,thin");
    let mut rows: Vec<_> = reasons.iter().collect();
    rows.sort_by(|a, b| b.1.total.cmp(&a.1.total).then_with(|| a.0.cmp(b.0)));
    for (reason, agg) in rows {
        let pct = if total_signals > 0 {
            (agg.total as f64) / (total_signals as f64) * 100.0
        } else {
            0.0
        };
        println!(
            "{reason},{},{pct:.1},{},{}",
            agg.total, agg.liquid, agg.thin
        );
    }
    println!();
}

fn print_tail_slice_section(tail: &[TailRow]) {
    println!("== Tail Risk Slice (Worst 20) ==");
    println!("signal_id,market_id,bucket,legs_n,q_req,q_set,total_pnl,pnl_left_total,notes");
    for r in tail {
        println!(
            "{},{},{},{},{:.6},{:.6},{:.6},{:.6},{}",
            r.signal_id,
            r.market_id,
            r.bucket,
            r.legs_n,
            r.q_req,
            r.q_set,
            r.total_pnl,
            r.pnl_left_total,
            r.notes.replace('\n', " "),
        );
    }
    if tail.is_empty() {
        println!("(empty)");
    }
    println!();
}

fn top_reasons(map: &BTreeMap<String, u64>, k: usize) -> Vec<String> {
    let mut rows: Vec<(&String, &u64)> = map.iter().collect();
    rows.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
    rows.into_iter()
        .take(k)
        .map(|(r, c)| format!("{r}({c})"))
        .collect()
}

fn infer_last_run_id(shadow_path: &Path) -> anyhow::Result<String> {
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .from_path(shadow_path)
        .with_context(|| format!("open {}", shadow_path.display()))?;

    let header = rdr
        .headers()
        .with_context(|| format!("read header {}", shadow_path.display()))?
        .clone();

    let Some(run_id_idx) = header
        .iter()
        .position(|h| h.trim().eq_ignore_ascii_case("run_id"))
    else {
        anyhow::bail!("missing column run_id in {}", shadow_path.display());
    };

    let mut last: Option<String> = None;
    for record in rdr.records() {
        let record = match record {
            Ok(r) => r,
            Err(_) => continue,
        };
        let Some(v) = record.get(run_id_idx) else {
            continue;
        };
        let v = v.trim();
        if !v.is_empty() {
            last = Some(v.to_string());
        }
    }

    last.context("no run_id found in shadow_log.csv")
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

fn parse_u64(s: &str) -> Option<u64> {
    s.trim().parse::<u64>().ok()
}
