pub mod gamma;
pub mod metrics;
pub mod output;
pub mod probe;
pub mod select;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context as _;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::config::Config;
use crate::market_select::gamma::GammaMarket;
use crate::market_select::metrics::MarketScoreRowComputed;
use crate::recorder::CsvAppender;
use crate::types::now_ms;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreferStrategy {
    Binary,
    Triangle,
    Any,
}

impl PreferStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            PreferStrategy::Binary => "binary",
            PreferStrategy::Triangle => "triangle",
            PreferStrategy::Any => "any",
        }
    }
}

impl std::str::FromStr for PreferStrategy {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.trim().to_ascii_lowercase().as_str() {
            "binary" => PreferStrategy::Binary,
            "triangle" => PreferStrategy::Triangle,
            _ => PreferStrategy::Any,
        })
    }
}

#[derive(Clone, Debug)]
pub struct MarketSelectOptions {
    pub probe_seconds: u64,
    pub pool_limit: usize,
    pub prefer_strategy: PreferStrategy,
    pub out_dir: Option<PathBuf>,
}

pub async fn run(cfg: &Config, opts: MarketSelectOptions) -> anyhow::Result<()> {
    let started_at_ms = now_ms();
    let run_id = format_run_id(started_at_ms);
    let out_dir = opts
        .out_dir
        .clone()
        .unwrap_or_else(|| default_out_dir(&cfg.run.data_dir, &run_id));
    std::fs::create_dir_all(&out_dir).with_context(|| format!("create {}", out_dir.display()))?;

    info!(run_id, out_dir = %out_dir.display(), "market_select run initialized");

    // Crash/ctrl-c tolerant output: append completed probe rows as they arrive into market_scores.csv.
    // At the end (normal exit), we rewrite market_scores.csv into a deterministic sorted order.
    let mut market_scores_live = match CsvAppender::open(
        out_dir.join(output::FILE_MARKET_SCORES),
        &output::MARKET_SCORES_HEADER,
    ) {
        Ok(v) => Some(v),
        Err(e) => {
            warn!(
                error = %e,
                "open market_scores.csv failed; continuing without incremental output"
            );
            None
        }
    };

    let markets = gamma::fetch_candidate_pool(cfg, opts.pool_limit)
        .await
        .context("fetch gamma candidate pool")?;
    info!(pool = markets.len(), "gamma candidate pool loaded");

    let markets = filter_by_prefer_strategy(markets, opts.prefer_strategy);
    if markets.is_empty() {
        anyhow::bail!("no gamma candidates remain after prefer_strategy filtering");
    }
    let candidates_total = markets.len();

    let sem = Arc::new(Semaphore::new(cfg.market_select.max_concurrency.max(1)));
    let mut join_set: JoinSet<(GammaMarket, anyhow::Result<MarketScoreRowComputed>)> =
        JoinSet::new();

    for m in markets {
        let cfg = cfg.clone();
        let sem = sem.clone();
        let run_id = run_id.clone();
        let opts = opts.clone();
        join_set.spawn(async move {
            let _permit = sem.acquire().await.expect("semaphore");
            let res = probe::probe_market(&cfg, &run_id, &m, opts.probe_seconds).await;
            (m, res)
        });
    }

    let mut rows: Vec<MarketScoreRowComputed> = Vec::new();
    let mut probes_completed_ok: usize = 0;
    let mut probes_completed_failed: usize = 0;
    let mut last_ok_gamma_id: Option<String> = None;
    let mut aborted = false;

    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    let mut last_progress_write_ms = started_at_ms;
    let progress_write_every_ms: u64 = 2_000;

    loop {
        tokio::select! {
            biased;
            res = &mut ctrl_c => {
                aborted = true;
                warn!(
                    completed_ok = probes_completed_ok,
                    completed_failed = probes_completed_failed,
                    total = candidates_total,
                    "ctrl-c received; aborting remaining probes and writing partial outputs"
                );
                if let Err(e) = res {
                    warn!(error = %e, "ctrl-c handler error");
                }
                join_set.abort_all();
                break;
            }
            next = join_set.join_next() => {
                let Some(next) = next else { break; };
                match next {
                    Ok((_m, Ok(r))) => {
                        probes_completed_ok += 1;
                        last_ok_gamma_id = Some(r.row.gamma_id.clone());
                        if let Some(out) = market_scores_live.as_mut() {
                            if let Err(e) = out.write_record(output::row_to_record(&r.row)) {
                                warn!(error = %e, "write market_scores.csv row failed");
                            }
                        }
                        rows.push(r);
                        if probes_completed_ok % 10 == 0 {
                            info!(completed_ok = probes_completed_ok, total = candidates_total, "probe progress");
                        }
                    }
                    Ok((m, Err(e))) => {
                        probes_completed_failed += 1;
                        warn!(gamma_id = %m.gamma_id, condition_id = %m.condition_id, error = %e, "probe failed");
                    }
                    Err(e) => {
                        probes_completed_failed += 1;
                        warn!(error = %e, "probe task join error");
                    }
                }

                let now_ms = now_ms();
                if now_ms.saturating_sub(last_progress_write_ms) >= progress_write_every_ms {
                    last_progress_write_ms = now_ms;
                    if let Err(e) = output::write_recommendation_json(
                        &out_dir,
                        &run_id,
                        opts.probe_seconds,
                        candidates_total,
                        probes_completed_ok,
                        probes_completed_failed,
                        aborted,
                        started_at_ms,
                        now_ms,
                        last_ok_gamma_id.as_deref(),
                        None,
                        None,
                    ) {
                        warn!(error = %e, "write progress recommendation.json failed");
                    }
                }
            }
        }
    }

    if aborted {
        // Best-effort drain, but do not block output forever (users often Ctrl-C and expect outputs).
        let drain_timeout = tokio::time::sleep(std::time::Duration::from_secs(2));
        tokio::pin!(drain_timeout);
        loop {
            tokio::select! {
                _ = &mut drain_timeout => {
                    warn!("timeout draining aborted probe tasks; writing outputs anyway");
                    break;
                }
                next = join_set.join_next() => {
                    if next.is_none() { break; }
                }
            }
        }
    }

    if let Some(out) = market_scores_live.as_mut() {
        if let Err(e) = out.flush_and_sync() {
            warn!(error = %e, "flush market_scores.csv failed");
        }
    }
    drop(market_scores_live);

    // Deterministic ordering for output: by gamma_volume24hr desc then gamma_id asc.
    rows.sort_by(|a, b| {
        metrics::cmp_f64_desc(a.row.gamma_volume24hr, b.row.gamma_volume24hr)
            .then_with(|| a.row.gamma_id.cmp(&b.row.gamma_id))
    });

    output::write_market_scores_csv(&out_dir, &rows).context("write market_scores.csv")?;

    let (selected_opt, selection_error) =
        match select::select_two_markets(&rows, opts.prefer_strategy) {
            Ok(selected) => (Some(selected), None),
            Err(e) => (None, Some(e)),
        };

    let selection_error_string = selection_error.as_ref().map(|e| e.to_string());

    output::write_suggest_toml(
        &out_dir,
        selected_opt.as_ref(),
        selection_error_string.as_deref(),
    )
    .context("write suggest.toml")?;
    output::write_recommendation_json(
        &out_dir,
        &run_id,
        opts.probe_seconds,
        candidates_total,
        probes_completed_ok,
        probes_completed_failed,
        aborted,
        started_at_ms,
        now_ms(),
        last_ok_gamma_id.as_deref(),
        selected_opt.as_ref(),
        selection_error_string.clone(),
    )
    .context("write recommendation.json")?;

    info!(
        out_dir = %out_dir.display(),
        liquid = selected_opt.as_ref().map(|s| s.liquid.row.gamma_id.as_str()).unwrap_or(""),
        thin = selected_opt.as_ref().map(|s| s.thin.row.gamma_id.as_str()).unwrap_or(""),
        aborted,
        "market_select done"
    );

    if let Some(e) = selection_error {
        anyhow::bail!("selection failed: {e}");
    }
    Ok(())
}

fn filter_by_prefer_strategy(
    markets: Vec<GammaMarket>,
    prefer: PreferStrategy,
) -> Vec<GammaMarket> {
    if prefer == PreferStrategy::Any {
        return markets;
    }
    markets
        .into_iter()
        .filter(|m| match prefer {
            PreferStrategy::Binary => m.strategy == "binary",
            PreferStrategy::Triangle => m.strategy == "triangle",
            PreferStrategy::Any => true,
        })
        .collect()
}

fn default_out_dir(data_dir: &Path, run_id: &str) -> PathBuf {
    data_dir.join("market_select").join(run_id)
}

fn format_run_id(now_ms: u64) -> String {
    // Deterministic-ish: use unix ms plus pid for uniqueness, no external RNG.
    let pid = std::process::id();
    let rand6 = ((now_ms as u32) ^ pid) % 1_000_000;
    format!("msel_{now_ms}_{rand6:06}")
}
