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
    let run_id = format_run_id(now_ms());
    let out_dir = opts
        .out_dir
        .clone()
        .unwrap_or_else(|| default_out_dir(&cfg.run.data_dir, &run_id));
    std::fs::create_dir_all(&out_dir).with_context(|| format!("create {}", out_dir.display()))?;

    // Crash-tolerant progress output: append completed probe rows as they arrive.
    // The final `market_scores.csv` remains deterministically sorted at the end.
    let mut partial = match CsvAppender::open(
        out_dir.join("market_scores.partial.csv"),
        &output::MARKET_SCORES_HEADER,
    ) {
        Ok(v) => Some(v),
        Err(e) => {
            warn!(error = %e, "open market_scores.partial.csv failed; continuing without partial output");
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
    let mut aborted = false;

    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

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
                        if let Some(out) = partial.as_mut() {
                            if let Err(e) = out.write_record(output::row_to_record(&r.row)) {
                                warn!(error = %e, "write market_scores.partial.csv row failed");
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
            }
        }
    }

    if aborted {
        // Drain aborted tasks to avoid running probes in the background after we start writing outputs.
        while let Some(_res) = join_set.join_next().await {}
    }

    if let Some(out) = partial.as_mut() {
        if let Err(e) = out.flush_and_sync() {
            warn!(error = %e, "flush market_scores.partial.csv failed");
        }
    }

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

    output::write_suggest_toml(&out_dir, selected_opt.as_ref()).context("write suggest.toml")?;
    output::write_recommendation_json(
        &out_dir,
        &run_id,
        opts.probe_seconds,
        candidates_total,
        probes_completed_ok,
        probes_completed_failed,
        aborted,
        selected_opt.as_ref(),
        selection_error.as_ref().map(|e| e.to_string()),
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
