use std::path::PathBuf;

use anyhow::Context as _;
use clap::{Parser, ValueEnum};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "market_select",
    version,
    about = "Project Razor Phase 1 market selector (read-only)"
)]
struct Args {
    #[arg(long, default_value = "config/config.toml")]
    config: PathBuf,

    /// Probe duration per market (seconds). Default: 3600.
    #[arg(long)]
    probe_seconds: Option<u64>,

    /// Gamma candidate pool limit. Default: 200.
    #[arg(long)]
    pool_limit: Option<usize>,

    /// Prefer a single strategy to control variables (binary/triangle) or allow any.
    #[arg(long, value_enum)]
    prefer_strategy: Option<PreferStrategyArg>,

    /// Output directory. Default: `<data_dir>/market_select/<run_id>/`.
    #[arg(long)]
    out_dir: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum PreferStrategyArg {
    Binary,
    Triangle,
    Any,
}

impl From<PreferStrategyArg> for razor::market_select::PreferStrategy {
    fn from(v: PreferStrategyArg) -> Self {
        match v {
            PreferStrategyArg::Binary => razor::market_select::PreferStrategy::Binary,
            PreferStrategyArg::Triangle => razor::market_select::PreferStrategy::Triangle,
            PreferStrategyArg::Any => razor::market_select::PreferStrategy::Any,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = Args::parse();
    let cfg_raw = std::fs::read_to_string(&args.config).context("read config")?;
    let cfg: razor::config::Config = toml::from_str(&cfg_raw).context("parse config")?;

    let opts = razor::market_select::MarketSelectOptions {
        probe_seconds: args
            .probe_seconds
            .unwrap_or(cfg.market_select.probe_seconds),
        pool_limit: args.pool_limit.unwrap_or(cfg.market_select.pool_limit),
        prefer_strategy: args.prefer_strategy.map(Into::into).unwrap_or_else(|| {
            cfg.market_select
                .prefer_strategy
                .parse::<razor::market_select::PreferStrategy>()
                .unwrap()
        }),
        out_dir: args.out_dir,
    };

    info!(
        config = %args.config.display(),
        probe_seconds = opts.probe_seconds,
        pool_limit = opts.pool_limit,
        prefer_strategy = %opts.prefer_strategy.as_str(),
        "market_select start"
    );

    razor::market_select::run(&cfg, opts).await?;
    Ok(())
}
