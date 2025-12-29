pub mod gamma;
pub mod metrics;
pub mod output;
pub mod probe;
pub mod select;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context as _;
use tokio::sync::Semaphore;
use tracing::{info, warn};

use crate::config::Config;
use crate::market_select::gamma::GammaMarket;
use crate::market_select::metrics::MarketScoreRowComputed;
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

    let markets = gamma::fetch_candidate_pool(cfg, opts.pool_limit)
        .await
        .context("fetch gamma candidate pool")?;
    info!(pool = markets.len(), "gamma candidate pool loaded");

    let markets = filter_by_prefer_strategy(markets, opts.prefer_strategy);
    if markets.is_empty() {
        anyhow::bail!("no gamma candidates remain after prefer_strategy filtering");
    }

    let sem = Arc::new(Semaphore::new(cfg.market_select.max_concurrency.max(1)));
    let mut handles = Vec::with_capacity(markets.len());

    for m in markets {
        let cfg = cfg.clone();
        let sem = sem.clone();
        let run_id = run_id.clone();
        let opts = opts.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.expect("semaphore");
            let res = probe::probe_market(&cfg, &run_id, &m, opts.probe_seconds).await;
            (m, res)
        }));
    }

    let mut rows: Vec<MarketScoreRowComputed> = Vec::new();
    for h in handles {
        let (m, res) = h.await.context("join probe task")?;
        match res {
            Ok(r) => rows.push(r),
            Err(e) => {
                warn!(gamma_id = %m.gamma_id, condition_id = %m.condition_id, error = %e, "probe failed");
            }
        }
    }

    if rows.is_empty() {
        anyhow::bail!("all probes failed; no rows to write");
    }

    // Deterministic ordering for output: by gamma_volume24hr desc then gamma_id asc.
    rows.sort_by(|a, b| {
        metrics::cmp_f64_desc(a.row.gamma_volume24hr, b.row.gamma_volume24hr)
            .then_with(|| a.row.gamma_id.cmp(&b.row.gamma_id))
    });

    output::write_market_scores_csv(&out_dir, &rows).context("write market_scores.csv")?;

    let selected = select::select_two_markets(&rows, opts.prefer_strategy)
        .context("select liquid+thin markets")?;

    output::write_suggest_toml(&out_dir, &selected).context("write suggest.toml")?;
    output::write_recommendation_json(&out_dir, &run_id, opts.probe_seconds, &selected)
        .context("write recommendation.json")?;

    info!(
        out_dir = %out_dir.display(),
        liquid = %selected.liquid.row.gamma_id,
        thin = %selected.thin.row.gamma_id,
        "market_select done"
    );

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
