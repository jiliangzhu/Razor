use std::path::{Path, PathBuf};

use anyhow::Context as _;
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "run_compare", about = "Compare multiple Razor run directories")]
struct Args {
    /// Root data directory that contains `run_*` folders.
    #[arg(long, default_value = "data")]
    data_dir: PathBuf,

    /// Explicit run directories (comma-separated). If omitted, scans `data_dir` for `run_*`.
    #[arg(long, value_delimiter = ',')]
    runs: Vec<PathBuf>,

    /// Output directory (default: data/run_compare/<run_id>/).
    #[arg(long)]
    out_dir: Option<PathBuf>,
}

fn default_out_dir(data_dir: &Path) -> PathBuf {
    let id = format!("rcmp_{}", razor::types::now_ms());
    data_dir.join("run_compare").join(id)
}

fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = Args::parse();

    let run_dirs = if args.runs.is_empty() {
        razor::run_compare::discover_run_dirs(&args.data_dir)?
    } else {
        let mut v = args.runs.clone();
        v.sort();
        v
    };

    if run_dirs.is_empty() {
        anyhow::bail!("no run dirs found (use --runs or ensure data/run_* exists)");
    }

    let out_dir = args
        .out_dir
        .unwrap_or_else(|| default_out_dir(&args.data_dir));
    std::fs::create_dir_all(&out_dir).with_context(|| format!("create {}", out_dir.display()))?;

    let mut summaries: Vec<razor::run_compare::RunSummary> = Vec::new();
    for dir in run_dirs {
        match razor::run_compare::summarize_run_dir(&dir) {
            Ok(s) => summaries.push(s),
            Err(e) => {
                tracing::warn!(run_dir = %dir.display(), error = %e, "skip run_dir");
            }
        }
    }

    if summaries.is_empty() {
        anyhow::bail!("no usable runs after filtering");
    }

    summaries.sort_by(|a, b| a.run_id.cmp(&b.run_id));

    let csv_path = razor::run_compare::write_runs_summary_csv(&out_dir, &summaries)?;
    let md_path = razor::run_compare::write_runs_summary_md(&out_dir, &summaries)?;

    info!(
        out_dir = %out_dir.display(),
        runs = summaries.len(),
        csv = %csv_path.display(),
        md = %md_path.display(),
        "run_compare done"
    );
    Ok(())
}
