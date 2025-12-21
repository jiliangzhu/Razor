mod brain;
mod buckets;
mod config;
mod feed;
mod recorder;
mod shadow;
mod types;

use anyhow::{anyhow, Context as _};
use clap::Parser;
use tokio::sync::{mpsc, watch};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::types::{MarketSnapshot, Signal, Strategy, TradeTick};

#[derive(Parser, Debug)]
#[command(
    name = "razor",
    version,
    about = "Project Razor Phase 1 (dry-run only)"
)]
struct Args {
    #[arg(long, default_value = "config.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    enforce_dry_run_env()?;

    let args = Args::parse();
    let cfg = config::Config::load(&args.config).context("load config")?;

    std::fs::create_dir_all(&cfg.run.data_dir).context("create data_dir")?;

    let markets = feed::fetch_markets(&cfg).await.context("fetch markets")?;
    let (mut binary, mut triangle) = (0usize, 0usize);
    for m in &markets {
        match m.strategy().context("market strategy")? {
            Strategy::Binary => binary += 1,
            Strategy::Triangle => triangle += 1,
        }
    }
    info!(
        market_count = markets.len(),
        token_count = markets.iter().map(|m| m.token_ids.len()).sum::<usize>(),
        binary,
        triangle,
        "loaded markets"
    );

    let (trade_tx, trade_rx) = mpsc::channel::<TradeTick>(50_000);
    let (signal_tx, signal_rx) = mpsc::channel::<Signal>(10_000);
    let (snap_tx, snap_rx) = watch::channel::<Option<MarketSnapshot>>(None);

    let ticks_path = cfg.run.data_dir.join("ticks.csv");
    let trades_path = cfg.run.data_dir.join("trades.csv");
    let shadow_path = cfg.run.data_dir.join("shadow_log.csv");
    let raw_ws_path = cfg.run.data_dir.join("raw_ws.jsonl");

    let ws_handle = tokio::spawn(feed::run_market_ws(
        cfg.clone(),
        markets.clone(),
        snap_tx,
        ticks_path,
        raw_ws_path,
    ));

    let trades_handle = tokio::spawn(feed::run_trades_poller(
        cfg.clone(),
        markets.clone(),
        trade_tx,
        trades_path,
    ));

    let brain_handle = tokio::spawn(brain::run(cfg.clone(), markets.clone(), snap_rx, signal_tx));

    let shadow_handle = tokio::spawn(shadow::run(
        cfg.clone(),
        markets.clone(),
        trade_rx,
        signal_rx,
        shadow_path,
    ));

    tokio::select! {
        res = ws_handle => { res.context("ws task join")??; }
        res = trades_handle => { res.context("trades task join")??; }
        res = brain_handle => { res.context("brain task join")??; }
        res = shadow_handle => { res.context("shadow task join")??; }
        _ = tokio::signal::ctrl_c() => {
            info!("ctrl-c received; shutting down");
        }
    }

    info!("done");
    Ok(())
}

fn enforce_dry_run_env() -> anyhow::Result<()> {
    let mode = std::env::var("RAZOR_MODE").unwrap_or_else(|_| "dry_run".to_string());
    if mode != "dry_run" {
        error!(%mode, "RAZOR_MODE must be dry_run in Phase 1");
        return Err(anyhow!(
            "RAZOR_MODE must be dry_run (Phase 1 is dry-run only)"
        ));
    }
    Ok(())
}
