use std::path::{Path, PathBuf};

use anyhow::Context as _;
use clap::Parser;

use razor::report::{generate_report_files, ReportThresholds};

#[derive(Parser, Debug)]
#[command(
    name = "day14_report",
    about = "Project Razor Day14 report (report.json + report.md)"
)]
struct Args {
    #[arg(long, default_value = "data")]
    data_dir: PathBuf,
    /// If omitted, uses the last non-empty run_id found in shadow_log.csv.
    #[arg(long)]
    run_id: Option<String>,
    #[arg(long, default_value_t = 0.0)]
    min_total_shadow_pnl: f64,
    #[arg(long, default_value_t = 0.85)]
    min_avg_set_ratio: f64,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.data_dir).context("create data_dir")?;

    let run_id = match args.run_id {
        Some(v) => v,
        None => infer_last_run_id(&args.data_dir)?,
    };

    let thresholds = ReportThresholds {
        min_total_shadow_pnl: args.min_total_shadow_pnl,
        min_avg_set_ratio: args.min_avg_set_ratio,
    };

    let report = generate_report_files(&args.data_dir, &run_id, thresholds)?;

    println!("run_id={}", report.run_id);
    println!("total_shadow_pnl={:.6}", report.totals.total_shadow_pnl);
    println!("avg_set_ratio={:.6}", report.totals.avg_set_ratio);
    println!("go={}", report.verdict.go);
    println!("reasons={}", report.verdict.reasons.join("; "));
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

fn infer_last_run_id(data_dir: &Path) -> anyhow::Result<String> {
    let shadow_path = data_dir.join(razor::schema::FILE_SHADOW_LOG);
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::All)
        .from_path(&shadow_path)
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
