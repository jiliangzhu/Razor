use std::path::PathBuf;

use anyhow::Context as _;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "razor_replay")]
struct Args {
    /// Input run directory (expects snapshots.csv, trades.csv, config.toml).
    #[arg(long)]
    run_dir: PathBuf,

    /// Output directory (default: <run_dir>/replay).
    #[arg(long)]
    out_dir: Option<PathBuf>,

    /// Override replay run_id written into replay outputs (default: replay_<run_id>).
    #[arg(long)]
    replay_run_id: Option<String>,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let out_dir = args.out_dir.unwrap_or_else(|| args.run_dir.join("replay"));

    let replay_run_id = match args.replay_run_id {
        Some(v) => v,
        None => {
            let orig = razor::run_meta::RunMeta::read_from_dir(&args.run_dir)
                .map(|m| m.run_id)
                .unwrap_or_else(|_| "unknown".to_string());
            format!("replay_{orig}")
        }
    };

    let res = razor::replay::run_replay(
        &args.run_dir,
        razor::replay::ReplayOptions {
            out_dir: out_dir.clone(),
            replay_run_id: replay_run_id.clone(),
        },
    )
    .with_context(|| format!("replay {}", args.run_dir.display()))?;

    println!("replay_run_id={}", res.replay_run_id);
    println!("signals={}", res.signals);
    println!("shadow_rows={}", res.shadow_rows);
    println!("out_dir={}", res.out_dir.display());
    println!(
        "shadow_csv={}",
        res.out_dir
            .join(razor::replay::FILE_REPLAY_SHADOW_LOG)
            .display()
    );
    println!(
        "report_json={}",
        res.out_dir
            .join(razor::replay::FILE_REPLAY_REPORT_JSON)
            .display()
    );
    println!(
        "report_md={}",
        res.out_dir
            .join(razor::replay::FILE_REPLAY_REPORT_MD)
            .display()
    );

    Ok(())
}
