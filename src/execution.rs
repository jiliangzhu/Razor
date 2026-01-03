use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;

use crate::clob::{self, ApiCreds, ClobSigner};
use crate::clob_order::{self, OrderType};
use crate::config::Config;
use crate::types::{now_ms, Bucket, FillReport, FillStatus, MarketSnapshot, Side};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecKind {
    FireLeg1,
    Chase,
    Flatten,
}

impl ExecKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ExecKind::FireLeg1 => "FIRE_LEG1",
            ExecKind::Chase => "CHASE",
            ExecKind::Flatten => "FLATTEN",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TopOfBook {
    pub best_ask: f64,
    pub best_ask_size_best: f64,
    pub best_bid: f64,
    pub best_bid_size_best: f64,
}

pub fn top_of_book(snap: &MarketSnapshot, token_id: &str) -> Option<TopOfBook> {
    let leg = snap.legs.iter().find(|l| l.token_id == token_id)?;
    Some(TopOfBook {
        best_ask: leg.best_ask,
        best_ask_size_best: leg.best_ask_size_best,
        best_bid: leg.best_bid,
        best_bid_size_best: leg.best_bid_size_best,
    })
}

#[derive(Debug, Clone)]
pub struct ExecResult {
    pub fill: FillReport,
    pub top: TopOfBook,
    pub sim_fill_share_used: f64,
    pub latency_spike_ms_applied: u64,
    pub book_dropped: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct PlaceIocRequest<'a> {
    pub kind: ExecKind,
    pub bucket: Bucket,
    pub token_id: &'a str,
    pub side: Side,
    pub limit_price: f64,
    pub req_qty: f64,
    pub top: TopOfBook,
}

#[derive(Debug, Clone)]
pub enum ExecutionGateway {
    Sim(SimGateway),
    Live(Arc<LiveGateway>),
}

impl ExecutionGateway {
    pub fn new_sim(cfg: &Config, force_chase_fail: bool) -> Self {
        let latency_spike_ms = env_u64("RAZOR_SIM_LATENCY_SPIKE_MS").unwrap_or(0);
        let latency_spike_every = env_u64("RAZOR_SIM_LATENCY_SPIKE_EVERY").unwrap_or(0);
        let drop_book_pct = env_f64("RAZOR_SIM_DROP_BOOK_PCT")
            .unwrap_or(0.0)
            .clamp(0.0, 1.0);

        Self::Sim(SimGateway {
            sim_fill_share_liquid: cfg.sim.sim_fill_share_liquid,
            sim_fill_share_thin: cfg.sim.sim_fill_share_thin,
            sim_network_latency_ms: cfg.sim.sim_network_latency_ms,
            force_chase_fail,
            latency_spike_ms,
            latency_spike_every,
            drop_book_pct,
            req_seq: Arc::new(AtomicU64::new(0)),
        })
    }

    pub async fn new_live(cfg: &Config) -> anyhow::Result<Self> {
        let signer = ClobSigner::from_env(cfg).context("load live signer")?;
        let http = reqwest::Client::builder()
            .user_agent(concat!("razor/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_millis(
                cfg.polymarket.http_connect_timeout_ms,
            ))
            .timeout(Duration::from_millis(cfg.polymarket.http_timeout_ms))
            .build()
            .context("build clob http client")?;

        let creds: ApiCreds = clob::create_or_derive_api_creds(cfg, &signer, &http)
            .await
            .context("create/derive clob api creds")?;

        Ok(Self::Live(Arc::new(LiveGateway {
            base: cfg.polymarket.clob_base.clone(),
            http,
            signer,
            creds,
            place_orders: env_flag("RAZOR_LIVE_PLACE_ORDERS"),
            seq: AtomicU64::new(0),
        })))
    }

    pub async fn place_ioc(&self, req: PlaceIocRequest<'_>) -> anyhow::Result<ExecResult> {
        match self {
            ExecutionGateway::Sim(g) => g.place_ioc(req).await,
            ExecutionGateway::Live(g) => g.place_ioc(req).await,
        }
    }
}

#[derive(Debug)]
pub struct LiveGateway {
    base: String,
    http: reqwest::Client,
    signer: ClobSigner,
    creds: ApiCreds,
    place_orders: bool,
    seq: AtomicU64,
}

impl LiveGateway {
    async fn place_ioc(&self, req: PlaceIocRequest<'_>) -> anyhow::Result<ExecResult> {
        // NOTE: Safety gate. We compute the exact signed request (and HMAC headers) but only send
        // it when explicitly enabled. This prevents accidental real trading while iterating.
        let place_orders = self.place_orders;

        // Fetch per-token tick size / neg-risk / fee-rate from public endpoints.
        let base = self.base.trim_end_matches('/');
        let token_id = req.token_id;

        #[derive(serde::Deserialize)]
        struct TickSizeResp {
            minimum_tick_size: f64,
        }
        #[derive(serde::Deserialize)]
        struct NegRiskResp {
            neg_risk: bool,
        }
        #[derive(serde::Deserialize)]
        struct FeeRateResp {
            base_fee: u32,
        }

        let tick_url = format!("{base}/tick-size?token_id={token_id}");
        let min_tick_size = self
            .http
            .get(&tick_url)
            .send()
            .await
            .context("GET /tick-size")?
            .json::<TickSizeResp>()
            .await
            .context("decode /tick-size")?
            .minimum_tick_size;

        let neg_url = format!("{base}/neg-risk?token_id={token_id}");
        let neg_risk = self
            .http
            .get(&neg_url)
            .send()
            .await
            .context("GET /neg-risk")?
            .json::<NegRiskResp>()
            .await
            .context("decode /neg-risk")?
            .neg_risk;

        let fee_url = format!("{base}/fee-rate?token_id={token_id}");
        let fee_rate_bps = self
            .http
            .get(&fee_url)
            .send()
            .await
            .context("GET /fee-rate")?
            .json::<FeeRateResp>()
            .await
            .context("decode /fee-rate")?
            .base_fee;

        let exchange_addr =
            exchange_address(self.signer.chain_id(), neg_risk).context("exchange_address")?;

        let salt = now_ms()
            .saturating_mul(1_000)
            .saturating_add(self.seq.fetch_add(1, Ordering::Relaxed));

        let signed = clob_order::build_signed_order(
            self.signer.signing_key(),
            clob_order::BuildOrderParams {
                chain_id: self.signer.chain_id(),
                exchange_address: exchange_addr,
                token_id: ethereum_types::U256::from_dec_str(token_id).context("parse token_id")?,
                side: req.side,
                limit_price: req.limit_price,
                qty: req.req_qty,
                min_tick_size,
                fee_rate_bps,
                salt,
            },
        )
        .context("build signed order")?;

        let body = clob_order::PostOrderBody {
            order: signed.to_order_json(),
            owner: &self.creds.api_key,
            order_type: OrderType::Fak.as_str(),
        };
        let body_json = serde_json::to_string(&body).context("serialize order body")?;

        let l2_headers = clob::create_level2_headers(
            &self.signer,
            &self.creds,
            "POST",
            "/order",
            Some(&body_json),
        )
        .context("build l2 headers")?;

        if place_orders {
            // Intentionally not implemented until we have full fill parsing + reconciliation.
            tracing::warn!(
                token_id = %req.token_id,
                "RAZOR_LIVE_PLACE_ORDERS=1 set, but live order placement is not implemented yet; skipping POST /order"
            );
        } else {
            // Use the computed signature/header path so we don't regress auth without noticing.
            tracing::debug!(
                token_id = %req.token_id,
                l2_headers = l2_headers.len(),
                body_len = body_json.len(),
                "live dry-run: built signed order + l2 headers (not sent)"
            );
        }

        Ok(ExecResult {
            fill: FillReport {
                requested_qty: req.req_qty,
                filled_qty: 0.0,
                avg_price: 0.0,
                status: FillStatus::None,
                order_id: format!("LIVE_DRY_{salt}"),
                latency_ms: 0,
            },
            top: req.top,
            sim_fill_share_used: 0.0,
            latency_spike_ms_applied: 0,
            book_dropped: false,
        })
    }
}

fn exchange_address(chain_id: u64, neg_risk: bool) -> anyhow::Result<&'static str> {
    match (chain_id, neg_risk) {
        (137, false) => Ok("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"),
        (137, true) => Ok("0xC5d563A36AE78145C45a50134d48A1215220f80a"),
        (80002, false) => Ok("0xdFE02Eb6733538f8Ea35D585af8DE5958AD99E40"),
        (80002, true) => Ok("0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"),
        _ => anyhow::bail!("unsupported chain_id {chain_id} (neg_risk={neg_risk})"),
    }
}

#[derive(Debug, Clone)]
pub struct SimGateway {
    pub sim_fill_share_liquid: f64,
    pub sim_fill_share_thin: f64,
    pub sim_network_latency_ms: u64,
    pub force_chase_fail: bool,
    pub latency_spike_ms: u64,
    /// If 0, apply spike to every request (K=1).
    pub latency_spike_every: u64,
    pub drop_book_pct: f64,
    pub req_seq: Arc<AtomicU64>,
}

impl SimGateway {
    async fn place_ioc(&self, req: PlaceIocRequest<'_>) -> anyhow::Result<ExecResult> {
        let seq = self
            .req_seq
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);

        let latency_spike_ms_applied = if self.latency_spike_ms > 0
            && (self.latency_spike_every == 0 || seq % self.latency_spike_every == 0)
        {
            self.latency_spike_ms
        } else {
            0
        };

        let book_dropped = should_drop_book(self.drop_book_pct, seq, req.token_id);
        let top = if book_dropped {
            TopOfBook {
                best_ask: 0.0,
                best_ask_size_best: 0.0,
                best_bid: 0.0,
                best_bid_size_best: 0.0,
            }
        } else {
            req.top
        };

        let start_ms = now_ms();
        tokio::time::sleep(Duration::from_millis(self.sim_network_latency_ms)).await;
        if latency_spike_ms_applied > 0 {
            tokio::time::sleep(Duration::from_millis(latency_spike_ms_applied)).await;
        }
        let latency_ms = now_ms().saturating_sub(start_ms);

        let sim_fill_share_used = sim_fill_share(
            req.bucket,
            self.sim_fill_share_liquid,
            self.sim_fill_share_thin,
        );

        let (filled_qty, status, avg_price) =
            if self.force_chase_fail && req.kind == ExecKind::Chase {
                (0.0, FillStatus::None, 0.0)
            } else {
                sim_fill(
                    req.side,
                    req.limit_price,
                    req.req_qty,
                    top.best_ask,
                    top.best_ask_size_best,
                    top.best_bid,
                    top.best_bid_size_best,
                    sim_fill_share_used,
                )
            };

        let order_id = format!(
            "SIM_{}_{}_{}",
            start_ms,
            req.token_id,
            req.kind.as_str().to_ascii_lowercase()
        );

        Ok(ExecResult {
            fill: FillReport {
                requested_qty: req.req_qty,
                filled_qty,
                avg_price,
                status,
                order_id,
                latency_ms,
            },
            top,
            sim_fill_share_used,
            latency_spike_ms_applied,
            book_dropped,
        })
    }
}

fn sim_fill_share(bucket: Bucket, liquid: f64, thin: f64) -> f64 {
    let raw = match bucket {
        Bucket::Liquid => liquid,
        Bucket::Thin => thin,
    };
    if !raw.is_finite() {
        return 0.0;
    }
    raw.clamp(0.0, 1.0)
}

#[allow(clippy::too_many_arguments)]
fn sim_fill(
    side: Side,
    limit_price: f64,
    req_qty: f64,
    best_ask: f64,
    best_ask_size_best: f64,
    best_bid: f64,
    best_bid_size_best: f64,
    sim_fill_share_used: f64,
) -> (f64, FillStatus, f64) {
    if !limit_price.is_finite() || !req_qty.is_finite() || req_qty <= 0.0 {
        return (0.0, FillStatus::None, 0.0);
    }

    match side {
        Side::Buy => {
            if !best_ask.is_finite() || best_ask <= 0.0 {
                return (0.0, FillStatus::None, 0.0);
            }
            if limit_price + 1e-12 < best_ask {
                return (0.0, FillStatus::None, 0.0);
            }
            let cap = (best_ask_size_best.max(0.0)) * sim_fill_share_used;
            let filled = req_qty.min(cap).max(0.0);
            let status = if filled <= 0.0 {
                FillStatus::None
            } else if filled + 1e-9 >= req_qty {
                FillStatus::Full
            } else {
                FillStatus::Partial
            };
            (filled, status, limit_price)
        }
        Side::Sell => {
            if !best_bid.is_finite() || best_bid <= 0.0 {
                return (0.0, FillStatus::None, 0.0);
            }
            if limit_price - 1e-12 > best_bid {
                return (0.0, FillStatus::None, 0.0);
            }
            let cap = (best_bid_size_best.max(0.0)) * sim_fill_share_used;
            let filled = req_qty.min(cap).max(0.0);
            let status = if filled <= 0.0 {
                FillStatus::None
            } else if filled + 1e-9 >= req_qty {
                FillStatus::Full
            } else {
                FillStatus::Partial
            };
            (filled, status, limit_price)
        }
    }
}

fn should_drop_book(drop_book_pct: f64, seq: u64, token_id: &str) -> bool {
    if !(0.0..=1.0).contains(&drop_book_pct) || token_id.trim().is_empty() {
        return false;
    }
    if drop_book_pct >= 1.0 {
        return true;
    }
    if drop_book_pct <= 0.0 {
        return false;
    }

    let threshold = (drop_book_pct * 10_000.0).ceil() as u64;
    let h = fnv1a64(seq, token_id);
    (h % 10_000) < threshold
}

fn fnv1a64(seed: u64, s: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325 ^ seed;
    for b in s.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn env_u64(name: &str) -> Option<u64> {
    let raw = std::env::var(name).ok()?;
    raw.trim().parse::<u64>().ok()
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| {
            let v = v.trim().to_ascii_lowercase();
            v == "1" || v == "true" || v == "yes" || v == "y"
        })
        .unwrap_or(false)
}

fn env_f64(name: &str) -> Option<f64> {
    let raw = std::env::var(name).ok()?;
    let v = raw.trim().parse::<f64>().ok()?;
    if v.is_finite() {
        Some(v)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sim_fill_buy_is_deterministic() {
        // limit < best_ask => none
        let (filled, status, avg_px) =
            sim_fill(Side::Buy, 0.49, 10.0, 0.50, 100.0, 0.49, 100.0, 0.10);
        assert_eq!(filled, 0.0);
        assert_eq!(status, FillStatus::None);
        assert_eq!(avg_px, 0.0);

        // limit >= best_ask, cap >= req => full
        let (filled, status, avg_px) =
            sim_fill(Side::Buy, 0.50, 10.0, 0.50, 200.0, 0.49, 200.0, 0.10);
        assert_eq!(filled, 10.0);
        assert_eq!(status, FillStatus::Full);
        assert_eq!(avg_px, 0.50);

        // cap < req => partial
        let (filled, status, avg_px) =
            sim_fill(Side::Buy, 0.50, 10.0, 0.50, 50.0, 0.49, 50.0, 0.10);
        assert_eq!(filled, 5.0);
        assert_eq!(status, FillStatus::Partial);
        assert_eq!(avg_px, 0.50);
    }

    #[test]
    fn sim_fill_sell_is_deterministic() {
        // limit > best_bid => none
        let (filled, status, avg_px) =
            sim_fill(Side::Sell, 0.51, 10.0, 0.52, 100.0, 0.50, 100.0, 0.10);
        assert_eq!(filled, 0.0);
        assert_eq!(status, FillStatus::None);
        assert_eq!(avg_px, 0.0);

        // limit <= best_bid, cap >= req => full
        let (filled, status, avg_px) =
            sim_fill(Side::Sell, 0.50, 10.0, 0.52, 200.0, 0.50, 200.0, 0.10);
        assert_eq!(filled, 10.0);
        assert_eq!(status, FillStatus::Full);
        assert_eq!(avg_px, 0.50);

        // cap < req => partial
        let (filled, status, avg_px) =
            sim_fill(Side::Sell, 0.50, 10.0, 0.52, 50.0, 0.50, 50.0, 0.10);
        assert_eq!(filled, 5.0);
        assert_eq!(status, FillStatus::Partial);
        assert_eq!(avg_px, 0.50);
    }

    #[tokio::test]
    async fn sim_drop_book_forces_no_fill() -> anyhow::Result<()> {
        let g = SimGateway {
            sim_fill_share_liquid: 1.0,
            sim_fill_share_thin: 1.0,
            sim_network_latency_ms: 0,
            force_chase_fail: false,
            latency_spike_ms: 0,
            latency_spike_every: 0,
            drop_book_pct: 1.0,
            req_seq: Arc::new(AtomicU64::new(0)),
        };

        let exec = ExecutionGateway::Sim(g);
        let res = exec
            .place_ioc(PlaceIocRequest {
                kind: ExecKind::FireLeg1,
                bucket: Bucket::Liquid,
                token_id: "T",
                side: Side::Buy,
                limit_price: 0.50,
                req_qty: 10.0,
                top: TopOfBook {
                    best_ask: 0.50,
                    best_ask_size_best: 100.0,
                    best_bid: 0.49,
                    best_bid_size_best: 100.0,
                },
            })
            .await?;

        assert!(res.book_dropped);
        assert_eq!(res.fill.status, FillStatus::None);
        assert_eq!(res.fill.filled_qty, 0.0);
        Ok(())
    }

    #[tokio::test]
    async fn sim_latency_spike_marks_exec_result() -> anyhow::Result<()> {
        let g = SimGateway {
            sim_fill_share_liquid: 1.0,
            sim_fill_share_thin: 1.0,
            sim_network_latency_ms: 0,
            force_chase_fail: false,
            latency_spike_ms: 2,
            latency_spike_every: 1,
            drop_book_pct: 0.0,
            req_seq: Arc::new(AtomicU64::new(0)),
        };

        let exec = ExecutionGateway::Sim(g);
        let res = exec
            .place_ioc(PlaceIocRequest {
                kind: ExecKind::FireLeg1,
                bucket: Bucket::Liquid,
                token_id: "T",
                side: Side::Buy,
                limit_price: 0.50,
                req_qty: 10.0,
                top: TopOfBook {
                    best_ask: 0.50,
                    best_ask_size_best: 100.0,
                    best_bid: 0.49,
                    best_bid_size_best: 100.0,
                },
            })
            .await?;

        assert_eq!(res.latency_spike_ms_applied, 2);
        Ok(())
    }
}
