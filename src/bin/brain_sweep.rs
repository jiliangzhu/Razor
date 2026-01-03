use std::path::PathBuf;

use anyhow::Context as _;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "brain_sweep")]
struct Args {
    /// Input run directory (expects snapshots.csv, trades.csv, config.toml).
    #[arg(long)]
    run_dir: PathBuf,

    /// Output directory (default: <run_dir>/brain_sweep).
    #[arg(long)]
    out_dir: Option<PathBuf>,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let out_dir = args
        .out_dir
        .unwrap_or_else(|| args.run_dir.join("brain_sweep"));

    let res = razor::brain_sweep::run_brain_sweep(&args.run_dir, &out_dir)
        .with_context(|| format!("brain sweep {}", args.run_dir.display()))?;

    println!("base_run_id={}", res.base_run_id);
    println!("out_dir={}", res.out_dir.display());
    println!(
        "scores_csv={}",
        res.out_dir
            .join(razor::brain_sweep::FILE_BRAIN_SWEEP_SCORES)
            .display()
    );
    println!(
        "best_patch={}",
        res.out_dir
            .join(razor::brain_sweep::FILE_BEST_BRAIN_PATCH)
            .display()
    );
    if let Some(best) = res.best {
        println!(
            "best: min_net_edge_bps={} risk_premium_bps={} signal_cooldown_ms={} total_pnl_sum={:.6} legging_rate={:.6} signals_ok={}",
            best.min_net_edge_bps,
            best.risk_premium_bps,
            best.signal_cooldown_ms,
            best.total_pnl_sum,
            best.legging_rate,
            best.signals_ok,
        );
    } else {
        println!("best: <none> (insufficient signals in sweep grid)");
    }

    Ok(())
}
