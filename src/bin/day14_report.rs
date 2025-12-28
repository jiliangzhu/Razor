use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use clap::Parser;

use razor::reasons::parse_notes_reasons;
use razor::run_meta::RunMeta;
use razor::schema::SCHEMA_VERSION;

const SET_RATIO_OK_THRESHOLD: f64 = 0.85;
const MAX_LEGGING_FAIL_SHARE: f64 = 0.15;
const PNL_THRESHOLD: f64 = 0.0;

#[derive(Parser, Debug)]
#[command(
    name = "day14_report",
    about = "Project Razor Day14 report (Phase 1 frozen verdict)"
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

    print_run_meta_section(&args.data_dir, &run_id)?;
    let analysis = analyze_shadow_log(&shadow_path, &run_id)?;
    print_overall_section(&analysis, args.starting_capital);
    print_group_section("By Notes (reasons)", "notes", &analysis.by_notes);
    print_group_section("By Strategy", "strategy", &analysis.by_strategy);
    print_group_section("By Bucket", "bucket", &analysis.by_bucket);
    print_combo_section(&analysis.by_combo);
    print_tail_slice_section(&analysis.tail);

    Ok(())
}

#[derive(Default, Clone, Copy)]
struct Agg {
    count: u64,
    sum_total_pnl: f64,
    sum_pnl_set: f64,
    sum_pnl_left_total: f64,
    miss_set_ratio: u64,
}

impl Agg {
    fn push(&mut self, total_pnl: f64, pnl_set: f64, pnl_left_total: f64, set_ratio: f64) {
        self.count += 1;
        self.sum_total_pnl += total_pnl;
        self.sum_pnl_set += pnl_set;
        self.sum_pnl_left_total += pnl_left_total;
        if set_ratio < SET_RATIO_OK_THRESHOLD {
            self.miss_set_ratio += 1;
        }
    }

    fn avg_total_pnl(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum_total_pnl / (self.count as f64)
        }
    }

    fn miss_rate(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            (self.miss_set_ratio as f64) / (self.count as f64)
        }
    }
}

struct ShadowAnalysis {
    rows_total: u64,
    rows_other_run: u64,
    rows_schema_mismatch: u64,
    rows_bad: u64,
    rows_ok: u64,

    signals_binary: u64,
    signals_triangle: u64,
    signals_other: u64,

    buckets_liquid: u64,
    buckets_thin: u64,
    buckets_unknown: u64,

    sum_total_pnl: f64,
    sum_pnl_set: f64,
    sum_pnl_left_total: f64,

    set_ratio_samples: Vec<f64>,

    by_notes: BTreeMap<String, Agg>,
    by_strategy: BTreeMap<String, Agg>,
    by_bucket: BTreeMap<String, Agg>,
    by_combo: BTreeMap<(String, String, String), Agg>,

    tail: Vec<TailRow>,
}

#[derive(Debug, Clone)]
struct TailRow {
    signal_id: u64,
    market_id: String,
    strategy: String,
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
    let idx_schema_version =
        find_col(&header, "schema_version").context("missing column: schema_version")?;
    let idx_bucket = find_col(&header, "bucket").context("missing column: bucket")?;
    let idx_total_pnl = find_col(&header, "total_pnl").context("missing column: total_pnl")?;
    let idx_pnl_set = find_col(&header, "pnl_set").context("missing column: pnl_set")?;
    let idx_pnl_left_total =
        find_col(&header, "pnl_left_total").context("missing column: pnl_left_total")?;
    let idx_set_ratio = find_col(&header, "set_ratio").context("missing column: set_ratio")?;
    let idx_q_set = find_col(&header, "q_set").context("missing column: q_set")?;
    let idx_q_req = find_col(&header, "q_req").context("missing column: q_req")?;
    let idx_legs_n = find_col(&header, "legs_n").context("missing column: legs_n")?;
    let idx_notes = find_col(&header, "notes").context("missing column: notes")?;
    let idx_market_id = find_col(&header, "market_id").context("missing column: market_id")?;
    let idx_signal_id = find_col(&header, "signal_id").context("missing column: signal_id")?;
    let idx_strategy = find_col(&header, "strategy").context("missing column: strategy")?;

    let mut rows_total: u64 = 0;
    let mut rows_other_run: u64 = 0;
    let mut rows_schema_mismatch: u64 = 0;
    let mut rows_bad: u64 = 0;
    let mut rows_ok: u64 = 0;

    let mut signals_binary: u64 = 0;
    let mut signals_triangle: u64 = 0;
    let mut signals_other: u64 = 0;

    let mut buckets_liquid: u64 = 0;
    let mut buckets_thin: u64 = 0;
    let mut buckets_unknown: u64 = 0;

    let mut sum_total_pnl: f64 = 0.0;
    let mut sum_pnl_set: f64 = 0.0;
    let mut sum_pnl_left_total: f64 = 0.0;

    let mut set_ratio_samples: Vec<f64> = Vec::new();

    let mut by_notes: BTreeMap<String, Agg> = BTreeMap::new();
    let mut by_strategy: BTreeMap<String, Agg> = BTreeMap::new();
    let mut by_bucket: BTreeMap<String, Agg> = BTreeMap::new();
    let mut by_combo: BTreeMap<(String, String, String), Agg> = BTreeMap::new();
    let mut tail: Vec<TailRow> = Vec::new();

    for record in rdr.records() {
        let record = match record {
            Ok(r) => r,
            Err(_) => continue,
        };
        rows_total += 1;

        if record.get(idx_run_id).unwrap_or("").trim() != run_id {
            rows_other_run += 1;
            continue;
        }

        let row_schema = record.get(idx_schema_version).unwrap_or("").trim();
        if !row_schema.eq_ignore_ascii_case(SCHEMA_VERSION) {
            rows_schema_mismatch += 1;
            continue;
        }

        let bucket_raw = record
            .get(idx_bucket)
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        let bucket_key = match bucket_raw.as_str() {
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
        let set_ratio = match record.get(idx_set_ratio).and_then(parse_f64) {
            Some(v) => v,
            None => {
                rows_bad += 1;
                continue;
            }
        };
        let q_set = match record.get(idx_q_set).and_then(parse_f64) {
            Some(v) => v,
            None => {
                rows_bad += 1;
                continue;
            }
        };
        let q_req = match record.get(idx_q_req).and_then(parse_f64) {
            Some(v) => v,
            None => {
                rows_bad += 1;
                continue;
            }
        };
        let legs_n = match record.get(idx_legs_n).and_then(parse_u64) {
            Some(v) => v,
            None => {
                rows_bad += 1;
                continue;
            }
        };
        let pnl_left_total = match record.get(idx_pnl_left_total).and_then(parse_f64) {
            Some(v) => v,
            None => {
                rows_bad += 1;
                continue;
            }
        };

        let strategy_raw = record
            .get(idx_strategy)
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        let strategy_key = match strategy_raw.as_str() {
            "binary" => "binary",
            "triangle" => "triangle",
            _ => "other",
        }
        .to_string();

        let notes_raw = record.get(idx_notes).unwrap_or("").trim().to_string();
        let notes_key = canonical_notes_key(&notes_raw);

        let market_id = record.get(idx_market_id).unwrap_or("").trim().to_string();
        let signal_id = match record.get(idx_signal_id).and_then(parse_u64) {
            Some(v) => v,
            None => {
                rows_bad += 1;
                continue;
            }
        };

        rows_ok += 1;
        sum_total_pnl += total_pnl;
        sum_pnl_set += pnl_set;
        sum_pnl_left_total += pnl_left_total;
        set_ratio_samples.push(set_ratio);

        match strategy_key.as_str() {
            "binary" => signals_binary += 1,
            "triangle" => signals_triangle += 1,
            _ => signals_other += 1,
        }
        match bucket_key.as_str() {
            "liquid" => buckets_liquid += 1,
            "thin" => buckets_thin += 1,
            _ => buckets_unknown += 1,
        }

        by_notes.entry(notes_key.clone()).or_default().push(
            total_pnl,
            pnl_set,
            pnl_left_total,
            set_ratio,
        );
        by_strategy.entry(strategy_key.clone()).or_default().push(
            total_pnl,
            pnl_set,
            pnl_left_total,
            set_ratio,
        );
        by_bucket.entry(bucket_key.clone()).or_default().push(
            total_pnl,
            pnl_set,
            pnl_left_total,
            set_ratio,
        );
        by_combo
            .entry((strategy_key.clone(), bucket_key.clone(), notes_key.clone()))
            .or_default()
            .push(total_pnl, pnl_set, pnl_left_total, set_ratio);

        tail.push(TailRow {
            signal_id,
            market_id,
            strategy: strategy_key,
            bucket: bucket_key,
            legs_n,
            q_req,
            q_set,
            total_pnl,
            pnl_left_total,
            notes: notes_key,
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
        rows_total,
        rows_other_run,
        rows_schema_mismatch,
        rows_bad,
        rows_ok,
        signals_binary,
        signals_triangle,
        signals_other,
        buckets_liquid,
        buckets_thin,
        buckets_unknown,
        sum_total_pnl,
        sum_pnl_set,
        sum_pnl_left_total,
        set_ratio_samples,
        by_notes,
        by_strategy,
        by_bucket,
        by_combo,
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

fn print_overall_section(a: &ShadowAnalysis, starting_capital: Option<f64>) {
    println!("== Overall ==");
    println!("rows_total={}", a.rows_total);
    println!("rows_ok={}", a.rows_ok);
    println!("rows_bad={}", a.rows_bad);
    println!("rows_other_run={}", a.rows_other_run);
    println!("rows_schema_version_mismatch={}", a.rows_schema_mismatch);
    println!(
        "signals_by_strategy=binary:{} triangle:{} other:{}",
        a.signals_binary, a.signals_triangle, a.signals_other
    );
    println!(
        "signals_by_bucket=liquid:{} thin:{} unknown:{}",
        a.buckets_liquid, a.buckets_thin, a.buckets_unknown
    );
    println!("sum_total_pnl={:.6}", a.sum_total_pnl);
    println!("sum_pnl_set={:.6}", a.sum_pnl_set);
    println!("sum_pnl_left_total={:.6}", a.sum_pnl_left_total);

    let (p50, p25, p10) = set_ratio_quantiles(&a.set_ratio_samples);
    println!("set_ratio_p50={p50:.6}");
    println!("set_ratio_p25={p25:.6}");
    println!("set_ratio_p10={p10:.6}");

    let miss = a
        .set_ratio_samples
        .iter()
        .filter(|v| **v < SET_RATIO_OK_THRESHOLD)
        .count() as u64;
    let miss_share = if a.rows_ok > 0 {
        (miss as f64) / (a.rows_ok as f64)
    } else {
        1.0
    };
    println!("legging_fail_share={miss_share:.3} (threshold={MAX_LEGGING_FAIL_SHARE})");

    let mut verdict_reasons: Vec<String> = Vec::new();
    let pnl_ok = a.sum_total_pnl > PNL_THRESHOLD;
    if pnl_ok {
        verdict_reasons.push(format!("TotalPnL > {PNL_THRESHOLD}"));
    } else {
        verdict_reasons.push(format!("TotalPnL <= {PNL_THRESHOLD}"));
    }
    let legging_ok = miss_share <= MAX_LEGGING_FAIL_SHARE;
    if legging_ok {
        verdict_reasons.push(format!(
            "LeggingFailShare <= {MAX_LEGGING_FAIL_SHARE} (set_ratio < {SET_RATIO_OK_THRESHOLD} share={miss_share:.3})"
        ));
    } else {
        verdict_reasons.push(format!(
            "LeggingFailShare > {MAX_LEGGING_FAIL_SHARE} (set_ratio < {SET_RATIO_OK_THRESHOLD} share={miss_share:.3})"
        ));
    }

    let decision = if pnl_ok && legging_ok { "GO" } else { "NO GO" };
    println!("GO_NO_GO={decision}");
    println!("reasons={}", verdict_reasons.join("; "));

    if let Some(c) = starting_capital.filter(|v| v.is_finite() && *v > 0.0) {
        let pct = (a.sum_total_pnl / c) * 100.0;
        println!("starting_capital={c:.6}");
        println!("pnl_pct={pct:.6}");
    }
    println!();
}

fn print_group_section(title: &str, key_name: &str, map: &BTreeMap<String, Agg>) {
    println!("== {title} ==");
    println!("{key_name},count,sum_total_pnl,avg_total_pnl,miss_rate");
    let mut rows: Vec<_> = map.iter().collect();
    rows.sort_by(|a, b| b.1.count.cmp(&a.1.count).then_with(|| a.0.cmp(b.0)));
    for (k, agg) in rows {
        println!(
            "{},{},{:.6},{:.6},{:.3}",
            k,
            agg.count,
            agg.sum_total_pnl,
            agg.avg_total_pnl(),
            agg.miss_rate()
        );
    }
    println!();
}

fn print_combo_section(map: &BTreeMap<(String, String, String), Agg>) {
    println!("== (strategy,bucket,notes) Worst 20 (by sum_total_pnl) ==");
    println!("strategy,bucket,notes,count,sum_total_pnl,avg_total_pnl,miss_rate");
    let mut rows: Vec<_> = map.iter().collect();
    rows.sort_by(|a, b| {
        a.1.sum_total_pnl
            .partial_cmp(&b.1.sum_total_pnl)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    for ((strategy, bucket, notes), agg) in rows.into_iter().take(20) {
        println!(
            "{},{},{},{},{:.6},{:.6},{:.3}",
            strategy,
            bucket,
            notes,
            agg.count,
            agg.sum_total_pnl,
            agg.avg_total_pnl(),
            agg.miss_rate()
        );
    }
    println!();
}

fn print_tail_slice_section(tail: &[TailRow]) {
    println!("== Tail Risk Slice (Worst 20) ==");
    println!(
        "signal_id,market_id,strategy,bucket,legs_n,q_req,q_set,total_pnl,pnl_left_total,notes"
    );
    for r in tail {
        println!(
            "{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{}",
            r.signal_id,
            r.market_id,
            r.strategy,
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

fn canonical_notes_key(notes: &str) -> String {
    let notes = notes.trim();
    if notes.is_empty() {
        return "OK".to_string();
    }
    let mut parts = parse_notes_reasons(notes);
    parts.sort();
    parts.dedup();
    if parts.is_empty() {
        "OK".to_string()
    } else {
        parts.join(",")
    }
}

fn set_ratio_quantiles(samples: &[f64]) -> (f64, f64, f64) {
    if samples.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    let mut v: Vec<f64> = samples.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p = |q: f64| -> f64 {
        if v.len() == 1 {
            return v[0];
        }
        let idx = ((v.len() - 1) as f64 * q).floor() as usize;
        v[idx]
    };
    (p(0.50), p(0.25), p(0.10))
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
