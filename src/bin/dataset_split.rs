use std::path::PathBuf;

use anyhow::Context as _;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "dataset_split")]
struct Args {
    /// Input run directory (expects shadow_log.csv and run_meta.json).
    #[arg(long)]
    run_dir: PathBuf,

    /// Output directory (default: <run_dir>/walk_forward).
    #[arg(long)]
    out_dir: Option<PathBuf>,

    /// Set ratio threshold used for legging_rate statistics.
    #[arg(long, default_value = "0.85")]
    set_ratio_threshold: f64,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let out_dir = args
        .out_dir
        .unwrap_or_else(|| args.run_dir.join("walk_forward"));

    let res =
        razor::dataset_split::run_dataset_split(&args.run_dir, &out_dir, args.set_ratio_threshold)
            .with_context(|| format!("dataset_split {}", args.run_dir.display()))?;

    println!("run_id={}", res.run_id);
    println!("out_dir={}", res.out_dir.display());
    println!(
        "daily_scores_csv={}",
        res.out_dir
            .join(razor::dataset_split::FILE_DAILY_SCORES)
            .display()
    );
    println!(
        "walk_forward_json={}",
        res.out_dir
            .join(razor::dataset_split::FILE_WALK_FORWARD_JSON)
            .display()
    );
    println!("days={}", res.days.len());
    Ok(())
}
