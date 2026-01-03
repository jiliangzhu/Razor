use std::path::{Path, PathBuf};

use anyhow::Context as _;
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "shadow_sweep",
    about = "Sweep Phase1 shadow ledger assumptions (fill_share / dump_slippage) on a fixed shadow_log.csv"
)]
struct Args {
    /// Shadow log CSV path (default: data/run_latest/shadow_log.csv).
    #[arg(long, default_value = "data/run_latest/shadow_log.csv")]
    input: PathBuf,

    /// Optional run_id filter. If omitted, uses the last run_id in the file.
    #[arg(long)]
    run_id: Option<String>,

    /// Output directory (default: data/sweep/<run_id>/).
    #[arg(long)]
    out_dir: Option<PathBuf>,

    /// Liquid fill_share grid values (comma-separated).
    #[arg(long, value_delimiter = ',', default_value = "0.20,0.30,0.40")]
    fill_share_liquid_values: Vec<f64>,

    /// Thin fill_share grid values (comma-separated).
    #[arg(long, value_delimiter = ',', default_value = "0.05,0.10,0.15")]
    fill_share_thin_values: Vec<f64>,

    /// Dump slippage assumptions (comma-separated).
    #[arg(long, value_delimiter = ',', default_value = "0.03,0.05,0.10")]
    dump_slippage_values: Vec<f64>,

    /// Set ratio threshold used only for legging_rate statistics.
    #[arg(long, default_value = "0.85")]
    set_ratio_threshold: f64,
}

fn default_out_dir(run_id: &str) -> PathBuf {
    PathBuf::from("data").join("sweep").join(run_id)
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
    let idx_run_id = header
        .iter()
        .position(|h| h.trim().eq_ignore_ascii_case("run_id"))
        .context("missing column: run_id")?;

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

fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = Args::parse();
    let run_id = match args.run_id.clone() {
        Some(v) => v,
        None => infer_last_run_id(&args.input)?,
    };
    let out_dir = args
        .out_dir
        .clone()
        .unwrap_or_else(|| default_out_dir(&run_id));

    let grid = razor::shadow_sweep::SweepGrid {
        fill_share_liquid_values: args.fill_share_liquid_values,
        fill_share_thin_values: args.fill_share_thin_values,
        dump_slippage_values: args.dump_slippage_values,
        set_ratio_threshold: args.set_ratio_threshold,
    };

    let res = razor::shadow_sweep::run_shadow_sweep(&args.input, Some(&run_id), grid, &out_dir)
        .context("run shadow_sweep")?;

    info!(
        out_dir = %res.out_dir.display(),
        run_id = %res.run_id,
        rows_ok = res.rows_ok,
        best_total_pnl_sum = res.best.as_ref().map(|b| b.total_pnl_sum).unwrap_or(0.0),
        "shadow_sweep done"
    );
    Ok(())
}
