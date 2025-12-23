mod brain;
mod buckets;
mod config;
mod feed;
mod recorder;
mod report;
mod schema;
mod shadow;
mod trade_store;
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
    let run_start_ms = crate::types::now_ms();
    if cfg.schema_version != schema::SCHEMA_VERSION {
        return Err(anyhow!(
            "schema_version mismatch: config={} code={}",
            cfg.schema_version,
            schema::SCHEMA_VERSION
        ));
    }
    schema::write_schema_version_json(&cfg.run.data_dir, &cfg.schema_version, run_start_ms)
        .context("write schema_version.json")?;
    let run_id = schema::make_run_id(run_start_ms);
    info!(%run_id, schema_version = %cfg.schema_version, "run start");

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
        run_id.clone(),
    ));

    let mut ws_handle = ws_handle;
    let mut trades_handle = trades_handle;
    let mut brain_handle = brain_handle;
    let mut shadow_handle = shadow_handle;

    enum ExitReason {
        CtrlC,
        Ws,
        Trades,
        Brain,
        Shadow,
    }

    let exit_reason: ExitReason = tokio::select! {
        res = &mut ws_handle => { res.context("ws task join")??; ExitReason::Ws }
        res = &mut trades_handle => { res.context("trades task join")??; ExitReason::Trades }
        res = &mut brain_handle => { res.context("brain task join")??; ExitReason::Brain }
        res = &mut shadow_handle => { res.context("shadow task join")??; ExitReason::Shadow }
        _ = tokio::signal::ctrl_c() => {
            info!("ctrl-c received; shutting down");
            ExitReason::CtrlC
        }
    };

    ws_handle.abort();
    trades_handle.abort();
    brain_handle.abort();
    shadow_handle.abort();

    match exit_reason {
        ExitReason::CtrlC => {}
        ExitReason::Ws => info!("ws task exited"),
        ExitReason::Trades => info!("trades task exited"),
        ExitReason::Brain => info!("brain task exited"),
        ExitReason::Shadow => info!("shadow task exited"),
    }

    let thresholds = report::ReportThresholds {
        min_total_shadow_pnl: cfg.report.min_total_shadow_pnl,
        min_avg_set_ratio: cfg.report.min_avg_set_ratio,
    };
    let report = report::generate_report_files(&cfg.run.data_dir, &run_id, thresholds)
        .context("generate report")?;
    info!(
        run_id = %report.run_id,
        total_shadow_pnl = report.totals.total_shadow_pnl,
        avg_set_ratio = report.totals.avg_set_ratio,
        go = report.verdict.go,
        "report written"
    );

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
