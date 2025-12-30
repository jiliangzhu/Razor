mod brain;
mod buckets;
mod config;
mod feed;
mod graceful_shutdown;
mod health;
mod reasons;
mod recorder;
mod report;
mod run_context;
mod run_meta;
mod schema;
mod shadow;
mod trade_store;
mod types;

use anyhow::{anyhow, Context as _};
use clap::Parser;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::types::{MarketSnapshot, Signal, Strategy, TradeTick};

#[derive(Parser, Debug)]
#[command(
    name = "razor",
    version,
    about = "Project Razor Phase 1 (dry-run only)"
)]
struct Args {
    #[arg(long, default_value = "config/config.toml")]
    config: String,
    /// Override mode (Phase 1 only supports `dry_run`).
    #[arg(long)]
    mode: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = Args::parse();
    let mode = resolve_mode(args.mode.as_deref())?;

    let cfg_path = std::path::PathBuf::from(&args.config);
    let cfg_raw = std::fs::read_to_string(&cfg_path).context("read config")?;
    let cfg: config::Config = toml::from_str(&cfg_raw).context("parse config")?;
    cfg.validate().context("validate config")?;

    std::fs::create_dir_all(&cfg.run.data_dir).context("create data_dir")?;
    let run_ctx = run_context::create_run_context(&cfg.run.data_dir).context("init run context")?;
    if cfg.schema_version != schema::SCHEMA_VERSION {
        return Err(anyhow!(
            "schema_version mismatch: config={} code={}",
            cfg.schema_version,
            schema::SCHEMA_VERSION
        ));
    }
    schema::write_schema_version_json(&run_ctx.run_dir, &cfg.schema_version, run_ctx.start_ts_ms)
        .context("write schema_version.json")?;
    recorder::write_run_config_snapshot(&run_ctx.run_dir, &cfg_raw)?;
    recorder::write_run_meta_json(
        &run_ctx.run_dir,
        &run_ctx.run_id,
        run_ctx.start_ts_ms,
        &mode,
    )?;
    run_meta::RunMeta {
        run_id: run_ctx.run_id.clone(),
        schema_version: schema::SCHEMA_VERSION.to_string(),
        git_sha: run_meta::env_git_sha(),
        start_ts_unix_ms: run_ctx.start_ts_ms,
        config_path: cfg_path.display().to_string(),
        trade_ts_source: "local".to_string(),
        notes_enum_version: "v1".to_string(),
        trade_poll_taker_only: Some(cfg.shadow.trade_poll_taker_only),
    }
    .write_to_dir(&run_ctx.run_dir)
    .context("write run_meta.json")?;
    ensure_data_latest_file_links(&cfg.run.data_dir)
        .context("ensure data/ latest-file symlinks")?;

    let flush_guard = recorder::RecorderGuard::new(run_ctx.run_dir.clone());

    info!(
        run_id = %run_ctx.run_id,
        run_dir = %run_ctx.run_dir.display(),
        schema_version = %cfg.schema_version,
        %mode,
        "run start"
    );

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

    let ticks_path = run_ctx.run_dir.join(schema::FILE_TICKS);
    let trades_path = run_ctx.run_dir.join(schema::FILE_TRADES);
    let shadow_path = run_ctx.run_dir.join(schema::FILE_SHADOW_LOG);
    let raw_ws_path = run_ctx.run_dir.join(schema::FILE_RAW_WS_JSONL);

    let (shutdown_tx, shutdown_rx) = graceful_shutdown::channel();

    let health_counters = std::sync::Arc::new(health::HealthCounters::default());
    let (health_tx, health_handle) = health::spawn_health_writer(
        run_ctx.run_dir.join(schema::FILE_HEALTH_JSONL),
        health_counters.clone(),
        shutdown_rx.clone(),
    )
    .context("start health writer")?;

    let ws_handle = tokio::spawn(feed::run_market_ws(
        cfg.clone(),
        markets.clone(),
        snap_tx,
        ticks_path,
        raw_ws_path,
        health_counters.clone(),
        shutdown_rx.clone(),
    ));

    let trades_handle = tokio::spawn(feed::run_trades_poller(
        cfg.clone(),
        markets.clone(),
        trade_tx,
        trades_path,
        health_counters.clone(),
        health_tx.clone(),
        shutdown_rx.clone(),
    ));

    let brain_handle = tokio::spawn(brain::run(
        cfg.clone(),
        run_ctx.run_id.clone(),
        markets.clone(),
        snap_rx.clone(),
        signal_tx,
        health_counters.clone(),
        shutdown_rx.clone(),
    ));

    let health_log_handle = {
        let counters = health_counters.clone();
        let snap_rx = snap_rx.clone();
        let mut shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            const STALE_WARN_MS: u64 = 30_000;

            loop {
                tokio::select! {
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() { break; }
                    }
                    _ = interval.tick() => {}
                }
                if *shutdown.borrow() {
                    break;
                }

                let snap = counters.snapshot();
                let now_ms = snap.ts_ms;

                let snap_rx_lag_ms: Option<u64> = snap_rx
                    .borrow()
                    .as_ref()
                    .and_then(|s| s.legs.iter().map(|l| l.ts_recv_us).max())
                    .map(|max_recv_us| {
                        let now_us = crate::types::now_us();
                        now_us.saturating_sub(max_recv_us) / 1000
                    });

                info!(
                    last_tick_ingest_ms = snap.last_tick_ingest_ms,
                    last_trade_ingest_ms = snap.last_trade_ingest_ms,
                    last_shadow_write_ms = snap.last_shadow_write_ms,
                    trade_store_len = snap.trade_store_size,
                    snap_rx_lag_ms = snap_rx_lag_ms.unwrap_or(0),
                    ticks_processed = snap.ticks_processed,
                    trades_written = snap.trades_written,
                    trades_invalid = snap.trades_invalid,
                    trades_dropped = snap.trades_dropped,
                    trades_duplicated = snap.trades_duplicated,
                    snapshots_stale_skipped = snap.snapshots_stale_skipped,
                    signals_emitted = snap.signals_emitted,
                    shadow_processed = snap.shadow_processed,
                    "health"
                );

                if snap.last_tick_ingest_ms > 0 {
                    let age = now_ms.saturating_sub(snap.last_tick_ingest_ms);
                    if age > STALE_WARN_MS {
                        warn!(age_ms = age, "no ticks observed recently");
                    }
                }
                if snap.last_trade_ingest_ms > 0 {
                    let age = now_ms.saturating_sub(snap.last_trade_ingest_ms);
                    if age > STALE_WARN_MS {
                        warn!(age_ms = age, "no trades observed recently");
                    }
                }
            }
        })
    };

    let shadow_handle = tokio::spawn(shadow::run(
        cfg.clone(),
        markets.clone(),
        trade_rx,
        signal_rx,
        shadow_path,
        health_counters.clone(),
        shutdown_rx.clone(),
    ));

    let mut ws_handle = Some(ws_handle);
    let mut trades_handle = Some(trades_handle);
    let mut brain_handle = Some(brain_handle);
    let mut shadow_handle = Some(shadow_handle);
    let mut health_handle = Some(health_handle);
    let mut health_log_handle = Some(health_log_handle);

    enum ExitReason {
        CtrlC,
        Ws,
        Trades,
        Brain,
        ShadowOrSniper,
        HealthWriter,
        HealthLog,
    }

    let mut first_err: Option<anyhow::Error> = None;

    let exit_reason: ExitReason = tokio::select! {
        res = ws_handle.as_mut().unwrap() => {
            ws_handle.take();
            match res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if first_err.is_none() { first_err = Some(add_context(e, "ws task failed")); }
                }
                Err(e) => {
                    if first_err.is_none() { first_err = Some(add_context(anyhow!(e), "ws task join failed")); }
                }
            }
            ExitReason::Ws
        }
        res = trades_handle.as_mut().unwrap() => {
            trades_handle.take();
            match res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if first_err.is_none() { first_err = Some(add_context(e, "trades task failed")); }
                }
                Err(e) => {
                    if first_err.is_none() { first_err = Some(add_context(anyhow!(e), "trades task join failed")); }
                }
            }
            ExitReason::Trades
        }
        res = brain_handle.as_mut().unwrap() => {
            brain_handle.take();
            match res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if first_err.is_none() { first_err = Some(add_context(e, "brain task failed")); }
                }
                Err(e) => {
                    if first_err.is_none() { first_err = Some(add_context(anyhow!(e), "brain task join failed")); }
                }
            }
            ExitReason::Brain
        }
        res = shadow_handle.as_mut().unwrap() => {
            shadow_handle.take();
            match res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if first_err.is_none() { first_err = Some(add_context(e, "shadow task failed")); }
                }
                Err(e) => {
                    if first_err.is_none() { first_err = Some(add_context(anyhow!(e), "shadow task join failed")); }
                }
            }
            ExitReason::ShadowOrSniper
        }
        res = health_handle.as_mut().unwrap() => {
            health_handle.take();
            if let Err(e) = res {
                if first_err.is_none() { first_err = Some(add_context(anyhow!(e), "health writer join failed")); }
            }
            ExitReason::HealthWriter
        }
        res = health_log_handle.as_mut().unwrap() => {
            health_log_handle.take();
            if let Err(e) = res {
                if first_err.is_none() { first_err = Some(add_context(anyhow!(e), "health log task join failed")); }
            }
            ExitReason::HealthLog
        }
        _ = tokio::signal::ctrl_c() => {
            info!("ctrl-c received; shutting down");
            ExitReason::CtrlC
        }
    };

    graceful_shutdown::request(&shutdown_tx);

    if let Some(h) = ws_handle.take() {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if first_err.is_none() {
                    first_err = Some(add_context(e, "ws task failed"));
                }
            }
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(add_context(anyhow!(e), "ws task join failed"));
                }
            }
        }
    }
    if let Some(h) = trades_handle.take() {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if first_err.is_none() {
                    first_err = Some(add_context(e, "trades task failed"));
                }
            }
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(add_context(anyhow!(e), "trades task join failed"));
                }
            }
        }
    }
    if let Some(h) = brain_handle.take() {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if first_err.is_none() {
                    first_err = Some(add_context(e, "brain task failed"));
                }
            }
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(add_context(anyhow!(e), "brain task join failed"));
                }
            }
        }
    }
    if let Some(h) = shadow_handle.take() {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if first_err.is_none() {
                    first_err = Some(add_context(e, "shadow task failed"));
                }
            }
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(add_context(anyhow!(e), "shadow task join failed"));
                }
            }
        }
    }
    if let Some(h) = health_log_handle.take() {
        if let Err(e) = h.await {
            if first_err.is_none() {
                first_err = Some(add_context(anyhow!(e), "health log task join failed"));
            }
        }
    }
    if let Some(h) = health_handle.take() {
        if let Err(e) = h.await {
            if first_err.is_none() {
                first_err = Some(add_context(anyhow!(e), "health writer join failed"));
            }
        }
    }

    match exit_reason {
        ExitReason::CtrlC => {}
        ExitReason::Ws => info!("ws task exited"),
        ExitReason::Trades => info!("trades task exited"),
        ExitReason::Brain => info!("brain task exited"),
        ExitReason::ShadowOrSniper => info!("shadow task exited"),
        ExitReason::HealthWriter => info!("health writer task exited"),
        ExitReason::HealthLog => info!("health log task exited"),
    }

    let thresholds = report::ReportThresholds {
        min_total_shadow_pnl: cfg.report.min_total_shadow_pnl,
        min_avg_set_ratio: cfg.report.min_avg_set_ratio,
    };
    let report = report::generate_report_files(&run_ctx.run_dir, &run_ctx.run_id, thresholds)
        .context("generate report")?;
    info!(
        run_id = %report.run_id,
        total_shadow_pnl = report.totals.total_shadow_pnl,
        avg_set_ratio = report.totals.avg_set_ratio,
        go = report.verdict.go,
        "report written"
    );

    flush_guard
        .flush_all()
        .context("final flush/sync of run outputs")?;

    if let Some(e) = first_err {
        return Err(e);
    }

    info!("done");
    Ok(())
}

fn add_context(err: anyhow::Error, ctx: &'static str) -> anyhow::Error {
    Err::<(), _>(err).context(ctx).unwrap_err()
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    DryRun,
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Mode::DryRun => write!(f, "dry_run"),
        }
    }
}

fn resolve_mode(cli: Option<&str>) -> anyhow::Result<Mode> {
    let raw = cli
        .map(|s| s.to_string())
        .or_else(|| std::env::var("RAZOR_MODE").ok())
        .unwrap_or_else(|| "dry_run".to_string());

    match raw.trim().to_ascii_lowercase().as_str() {
        "dry_run" | "dryrun" => Ok(Mode::DryRun),
        "live" => Err(anyhow!(
            "refusing to start: Phase 1 is dry_run only (set RAZOR_MODE=dry_run)"
        )),
        other => Err(anyhow!("unknown mode: {other} (expected dry_run)")),
    }
}

fn ensure_data_latest_file_links(data_dir: &std::path::Path) -> anyhow::Result<()> {
    ensure_latest_file_symlink(data_dir, schema::FILE_TICKS)?;
    ensure_latest_file_symlink(data_dir, schema::FILE_TRADES)?;
    ensure_latest_file_symlink(data_dir, schema::FILE_SHADOW_LOG)?;
    ensure_latest_file_symlink(data_dir, schema::FILE_SCHEMA_VERSION)?;
    Ok(())
}

fn ensure_latest_file_symlink(data_dir: &std::path::Path, file_name: &str) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        use std::path::Path;

        let link_path = data_dir.join(file_name);
        let target = Path::new("run_latest").join(file_name);

        if let Ok(meta) = std::fs::symlink_metadata(&link_path) {
            if meta.file_type().is_symlink() {
                std::fs::remove_file(&link_path)?;
            } else if meta.is_file() {
                let backup_name = format!("{}.legacy_{}", file_name, crate::types::now_ms());
                let backup_path = data_dir.join(backup_name);
                std::fs::rename(&link_path, &backup_path)?;
            } else {
                anyhow::bail!("refusing to replace non-file {}", link_path.display());
            }
        }

        symlink(target, link_path)?;
    }

    #[cfg(not(unix))]
    {
        let _ = (data_dir, file_name);
    }

    Ok(())
}
