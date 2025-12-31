use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context as _;
use futures_util::{SinkExt as _, StreamExt as _};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::health::{HealthCounters, HealthLine};
use crate::recorder::{CsvAppender, JsonlAppender, TICKS_HEADER, TRADES_HEADER};
use crate::types::{now_ms, now_us, LegSnapshot, MarketDef, MarketSnapshot, TradeTick};

const RAW_WS_ROTATE_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug, Deserialize)]
struct GammaMarket {
    #[serde(rename = "conditionId")]
    condition_id: String,
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: String,
    #[serde(default)]
    slug: Option<String>,
    #[serde(rename = "marketSlug", default)]
    market_slug: Option<String>,
    #[serde(rename = "roundSlug", default)]
    round_slug: Option<String>,
    #[serde(flatten)]
    extra: HashMap<String, serde_json::Value>,
}

fn extract_market_slug(market: &GammaMarket) -> Option<String> {
    market
        .slug
        .clone()
        .or_else(|| market.market_slug.clone())
        .or_else(|| market.round_slug.clone())
}

fn extract_round_start_ms(market: &GammaMarket) -> Option<(u64, &'static str)> {
    let keys = [
        "roundStartTime",
        "roundStartAt",
        "roundStart",
        "round_start_time",
        "epochStartTime",
        "epochStartAt",
        "epochStart",
        "epoch_start_time",
        "startAt",
        "startDate",
        "startsAt",
        "startTime",
    ];
    for key in keys {
        if let Some(value) = market.extra.get(key) {
            if let Some(ms) = parse_ts_value_ms(value) {
                return Some((ms, key));
            }
        }
    }
    None
}

fn parse_ts_value_ms(value: &serde_json::Value) -> Option<u64> {
    match value {
        serde_json::Value::Number(num) => num.as_f64().and_then(normalize_ts_value_ms),
        serde_json::Value::String(s) => s.parse::<f64>().ok().and_then(normalize_ts_value_ms),
        _ => None,
    }
}

fn normalize_ts_value_ms(raw: f64) -> Option<u64> {
    if !raw.is_finite() || raw <= 0.0 {
        return None;
    }
    let ms = if raw > 1_000_000_000_000.0 {
        raw
    } else if raw > 1_000_000_000.0 {
        raw * 1000.0
    } else {
        return None;
    };
    if ms.is_finite() {
        Some(ms as u64)
    } else {
        None
    }
}

fn classify_market_type(round_key: Option<&str>) -> Option<String> {
    let key = round_key?.to_ascii_lowercase();
    if key.contains("epoch") {
        Some("epoch".to_string())
    } else if key.contains("round") {
        Some("round".to_string())
    } else {
        None
    }
}

pub async fn fetch_markets(cfg: &Config) -> anyhow::Result<Vec<MarketDef>> {
    let client = reqwest::Client::builder()
        .user_agent(concat!("razor/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_millis(
            cfg.polymarket.http_connect_timeout_ms,
        ))
        .timeout(Duration::from_millis(cfg.polymarket.http_timeout_ms))
        .build()
        .context("build http client")?;

    let mut out = Vec::with_capacity(cfg.run.market_ids.len());
    for id in &cfg.run.market_ids {
        let url = format!(
            "{}/markets",
            cfg.polymarket.gamma_base.trim_end_matches('/')
        );
        let resp = client
            .get(url)
            .query(&[("id", id)])
            .send()
            .await
            .with_context(|| format!("gamma markets?id={id}"))?;
        let markets: Vec<GammaMarket> = resp.json().await.context("decode gamma market")?;
        let Some(m) = markets.into_iter().next() else {
            return Err(anyhow::anyhow!("gamma market id {id} not found"));
        };

        let token_ids: Vec<String> = serde_json::from_str(&m.clob_token_ids)
            .with_context(|| format!("parse clobTokenIds for gamma market {id}"))?;

        if token_ids.len() != 2 && token_ids.len() != 3 {
            warn!(
                market_id = %m.condition_id,
                legs = token_ids.len(),
                "skip market: Phase 1 supports 2-leg binary or 3-leg triangle only"
            );
            continue;
        }

        let market_slug = extract_market_slug(&m);
        let round_start = extract_round_start_ms(&m);
        let market_type = classify_market_type(round_start.as_ref().map(|(_, key)| *key));

        out.push(MarketDef {
            market_id: m.condition_id,
            token_ids,
            market_slug,
            market_type,
            round_start_ms: round_start.map(|(ms, _)| ms),
        });
    }

    if out.is_empty() {
        return Err(anyhow::anyhow!(
            "no usable markets loaded (need 2-leg or 3-leg markets)"
        ));
    }
    Ok(out)
}

struct LegState {
    token_id: String,
    best_ask: f64,
    best_ask_size_best: f64,
    best_bid: f64,
    best_bid_size_best: f64,
    ask_depth3_usdc: f64,
    ts_recv_us: u64,
    last_tick_log_ms: u64,
    ready: bool,
}

struct MarketState {
    market_id: String,
    legs: Vec<LegState>,
}

pub async fn run_market_ws(
    cfg: Config,
    markets: Vec<MarketDef>,
    snap_tx: watch::Sender<Option<MarketSnapshot>>,
    ticks_path: PathBuf,
    raw_ws_path: PathBuf,
    health: Arc<HealthCounters>,
    shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut ticks = CsvAppender::open(ticks_path, &TICKS_HEADER).context("open ticks.csv")?;
    let mut raw = JsonlAppender::open_with_rotation(raw_ws_path, Some(RAW_WS_ROTATE_BYTES))
        .context("open raw_ws.jsonl")?;

    let mut token_to_market: HashMap<String, (String, usize)> = HashMap::new();
    let mut market_states: HashMap<String, MarketState> = HashMap::new();
    let mut subscribe_tokens: Vec<String> = Vec::new();

    for m in markets {
        for (idx, token) in m.token_ids.iter().enumerate() {
            token_to_market.insert(token.clone(), (m.market_id.clone(), idx));
            subscribe_tokens.push(token.clone());
        }

        let legs = m
            .token_ids
            .iter()
            .map(|token_id| LegState {
                token_id: token_id.clone(),
                best_ask: 0.0,
                best_ask_size_best: 0.0,
                best_bid: 0.0,
                best_bid_size_best: 0.0,
                ask_depth3_usdc: 0.0,
                ts_recv_us: 0,
                last_tick_log_ms: 0,
                ready: false,
            })
            .collect();

        market_states.insert(
            m.market_id.clone(),
            MarketState {
                market_id: m.market_id,
                legs,
            },
        );
    }

    subscribe_tokens.sort();
    subscribe_tokens.dedup();

    let ws_url = format!("{}/ws/market", cfg.polymarket.ws_base.trim_end_matches('/'));

    let mut backoff = Duration::from_secs(1);
    loop {
        if *shutdown.borrow() {
            break;
        }
        match ws_run_once(
            &ws_url,
            &subscribe_tokens,
            &token_to_market,
            &mut market_states,
            &mut ticks,
            &mut raw,
            &snap_tx,
            &health,
            Duration::from_millis(cfg.polymarket.ws_connect_timeout_ms),
            Duration::from_millis(cfg.polymarket.ws_write_timeout_ms),
            shutdown.clone(),
        )
        .await
        {
            Ok(()) => {
                backoff = Duration::from_secs(1);
            }
            Err(e) => {
                error!(error = %e, "ws error; reconnecting");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        }
    }

    ticks.flush_and_sync().context("flush ticks.csv")?;
    raw.flush_and_sync().context("flush raw_ws.jsonl")?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn ws_run_once(
    ws_url: &str,
    subscribe_tokens: &[String],
    token_to_market: &HashMap<String, (String, usize)>,
    market_states: &mut HashMap<String, MarketState>,
    ticks: &mut CsvAppender,
    raw: &mut JsonlAppender,
    snap_tx: &watch::Sender<Option<MarketSnapshot>>,
    health: &HealthCounters,
    ws_connect_timeout: Duration,
    ws_write_timeout: Duration,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    info!(%ws_url, tokens = subscribe_tokens.len(), "connecting ws");
    if *shutdown.borrow() {
        return Ok(());
    }
    let (ws, _) =
        tokio::time::timeout(ws_connect_timeout, tokio_tungstenite::connect_async(ws_url))
            .await
            .context("ws connect timeout")?
            .context("connect ws")?;

    let (mut sink, mut stream) = ws.split();

    let subscribe_msg = serde_json::json!({
        "assets_ids": subscribe_tokens,
        "type": "market",
    });

    ws_send(
        &mut sink,
        Message::Text(subscribe_msg.to_string().into()),
        ws_write_timeout,
    )
    .await
    .context("send subscribe")?;

    let mut ping = tokio::time::interval(Duration::from_secs(10));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return Ok(());
                }
            }
            _ = ping.tick() => {
                ws_send(&mut sink, Message::Text("PING".to_string().into()), ws_write_timeout)
                    .await
                    .context("send ping")?;
            }
            msg = stream.next() => {
                let Some(msg) = msg else {
                    return Err(anyhow::anyhow!("ws stream ended"));
                };
                let msg = msg.context("ws read")?;
                match msg {
                    Message::Text(txt) => {
                        handle_ws_text(&txt, token_to_market, market_states, ticks, raw, snap_tx, health).await?;
                    }
                    Message::Binary(bin) => {
                        let txt = String::from_utf8_lossy(&bin);
                        handle_ws_text(&txt, token_to_market, market_states, ticks, raw, snap_tx, health).await?;
                    }
                    Message::Ping(_) | Message::Pong(_) => {}
                    Message::Close(frame) => {
                        return Err(anyhow::anyhow!("ws close: {frame:?}"));
                    }
                    Message::Frame(_) => {}
                }
            }
        }
    }
}

async fn ws_send<S>(sink: &mut S, msg: Message, timeout: Duration) -> anyhow::Result<()>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    tokio::time::timeout(timeout, sink.send(msg))
        .await
        .context("ws send timeout")?
        .context("ws send error")?;
    Ok(())
}

async fn handle_ws_text(
    txt: &str,
    token_to_market: &HashMap<String, (String, usize)>,
    market_states: &mut HashMap<String, MarketState>,
    ticks: &mut CsvAppender,
    raw: &mut JsonlAppender,
    snap_tx: &watch::Sender<Option<MarketSnapshot>>,
    health: &HealthCounters,
) -> anyhow::Result<()> {
    if txt == "PONG" {
        return Ok(());
    }

    if let Err(e) = raw.write_line(txt) {
        warn!(error = %e, "raw ws write failed");
    }

    let v: serde_json::Value = match serde_json::from_str(txt) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "ws non-json message");
            return Ok(());
        }
    };

    match v {
        serde_json::Value::Array(items) => {
            for item in items {
                if let serde_json::Value::Object(obj) = item {
                    handle_ws_obj(obj, token_to_market, market_states, ticks, snap_tx, health)?;
                }
            }
        }
        serde_json::Value::Object(obj) => {
            handle_ws_obj(obj, token_to_market, market_states, ticks, snap_tx, health)?;
        }
        _ => {}
    }

    Ok(())
}

fn handle_ws_obj(
    obj: serde_json::Map<String, serde_json::Value>,
    token_to_market: &HashMap<String, (String, usize)>,
    market_states: &mut HashMap<String, MarketState>,
    ticks: &mut CsvAppender,
    snap_tx: &watch::Sender<Option<MarketSnapshot>>,
    health: &HealthCounters,
) -> anyhow::Result<()> {
    let Some(event_type) = obj.get("event_type").and_then(|v| v.as_str()) else {
        return Ok(());
    };

    match event_type {
        "book" => handle_ws_book(obj, token_to_market, market_states, ticks, snap_tx, health)?,
        "price_change" => {
            handle_ws_price_change(obj, token_to_market, market_states, ticks, snap_tx, health)?
        }
        _ => {}
    }

    Ok(())
}

fn handle_ws_book(
    obj: serde_json::Map<String, serde_json::Value>,
    token_to_market: &HashMap<String, (String, usize)>,
    market_states: &mut HashMap<String, MarketState>,
    ticks: &mut CsvAppender,
    snap_tx: &watch::Sender<Option<MarketSnapshot>>,
    health: &HealthCounters,
) -> anyhow::Result<()> {
    let Some(token_id) = obj.get("asset_id").and_then(|v| v.as_str()) else {
        return Ok(());
    };

    let Some((mapped_market_id, idx)) = token_to_market.get(token_id) else {
        return Ok(());
    };
    let market_id = mapped_market_id.as_str();

    // Some WS messages include a `market` field; it can be inconsistent with our gamma-derived
    // condition_id mapping. Token->market mapping is the Phase 1 authority.
    if let Some(msg_market_id) = obj.get("market").and_then(|v| v.as_str()) {
        if msg_market_id != market_id {
            warn!(
                token_id,
                msg_market_id,
                mapped_market_id = market_id,
                "ws book market_id mismatch; using token->market mapping"
            );
        }
    }

    let bids: &[serde_json::Value] = obj
        .get("bids")
        .and_then(|v| v.as_array())
        .map(|v| v.as_slice())
        .unwrap_or(&[]);
    let asks: &[serde_json::Value] = obj
        .get("asks")
        .and_then(|v| v.as_array())
        .map(|v| v.as_slice())
        .unwrap_or(&[]);

    // Phase 1 hardening:
    // - Some markets can publish one-sided books (bids=[] or asks=[]). We still want to
    //   progress the pipeline (ticks/snapshots) without panicking or stalling.
    // - Missing bid => 0.0 (Shadow will penalize MISSING_BID).
    // - Missing ask => 1.0 (conservative: prevents false-positive edge).
    let (best_bid, best_bid_size_best) = best_level(bids, PriceSide::Bid).unwrap_or((0.0, 0.0));
    let (best_ask, best_ask_size_best) = best_level(asks, PriceSide::Ask).unwrap_or((1.0, 0.0));

    // Depth uses top-3 asks; when asks are missing, this is 0 => bucket degrades to Thin.
    let ask_depth3_usdc = ask_depth3_usdc(asks);

    let ts_recv_us = now_us();
    ticks.write_record([
        ts_recv_us.to_string(),
        market_id.to_string(),
        token_id.to_string(),
        best_bid.to_string(),
        best_ask.to_string(),
        ask_depth3_usdc.to_string(),
    ])?;
    health.inc_ticks_processed(1);
    health.set_last_tick_ingest_ms(ts_recv_us / 1000);

    let Some(state) = market_states.get_mut(market_id) else {
        return Ok(());
    };

    if *idx >= state.legs.len() {
        return Ok(());
    }

    let leg = &mut state.legs[*idx];
    leg.best_bid = best_bid;
    leg.best_ask = best_ask;
    leg.best_bid_size_best = best_bid_size_best;
    leg.best_ask_size_best = best_ask_size_best;
    leg.ask_depth3_usdc = ask_depth3_usdc;
    leg.ts_recv_us = ts_recv_us;
    leg.last_tick_log_ms = ts_recv_us / 1000;
    leg.ready = leg.best_ask.is_finite() && leg.best_ask > 0.0;

    maybe_publish_snapshot(state, snap_tx);
    Ok(())
}

fn handle_ws_price_change(
    obj: serde_json::Map<String, serde_json::Value>,
    token_to_market: &HashMap<String, (String, usize)>,
    market_states: &mut HashMap<String, MarketState>,
    ticks: &mut CsvAppender,
    snap_tx: &watch::Sender<Option<MarketSnapshot>>,
    health: &HealthCounters,
) -> anyhow::Result<()> {
    let Some(changes) = obj.get("price_changes").and_then(|v| v.as_array()) else {
        return Ok(());
    };

    for ch in changes {
        let Some(ch) = ch.as_object() else { continue };
        let Some(token_id) = ch.get("asset_id").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some((market_id, idx)) = token_to_market.get(token_id) else {
            continue;
        };
        let Some(state) = market_states.get_mut(market_id) else {
            continue;
        };
        if *idx >= state.legs.len() {
            continue;
        }

        let best_bid = parse_f64(ch.get("best_bid")).unwrap_or(0.0);
        let best_ask = parse_f64(ch.get("best_ask")).unwrap_or(0.0);

        let leg = &mut state.legs[*idx];
        // Best bid: 0 means missing.
        leg.best_bid = if best_bid.is_finite() && best_bid > 0.0 {
            best_bid
        } else {
            0.0
        };
        // Best ask: 0 means missing -> set to 1.0 (conservative).
        leg.best_ask = if best_ask.is_finite() && best_ask > 0.0 {
            best_ask
        } else {
            1.0
        };
        leg.best_bid_size_best = 0.0;
        leg.best_ask_size_best = 0.0;
        leg.ts_recv_us = now_us();
        leg.ready = leg.best_ask.is_finite() && leg.best_ask > 0.0;

        // Observability hardening:
        // Some markets may not publish full L2 `book` updates frequently. We still want ticks.csv
        // to grow (and health.last_tick_ingest_ms to advance) so it's obvious the WS link is live.
        //
        // Rate-limit per leg to ~1Hz to avoid turning price_change into an unbounded tick log.
        let tick_ms = leg.ts_recv_us / 1000;
        if tick_ms.saturating_sub(leg.last_tick_log_ms) >= 1_000 {
            ticks.write_record([
                leg.ts_recv_us.to_string(),
                market_id.to_string(),
                token_id.to_string(),
                leg.best_bid.to_string(),
                leg.best_ask.to_string(),
                leg.ask_depth3_usdc.to_string(),
            ])?;
            leg.last_tick_log_ms = tick_ms;
            health.inc_ticks_processed(1);
            health.set_last_tick_ingest_ms(tick_ms);
        }

        maybe_publish_snapshot(state, snap_tx);
    }

    Ok(())
}

fn parse_f64(v: Option<&serde_json::Value>) -> Option<f64> {
    let v = v?;
    if let Some(s) = v.as_str() {
        return s.parse::<f64>().ok();
    }
    v.as_f64()
}

fn maybe_publish_snapshot(state: &MarketState, snap_tx: &watch::Sender<Option<MarketSnapshot>>) {
    if !state.legs.iter().all(|l| l.ready) {
        return;
    }
    let snap = MarketSnapshot {
        market_id: state.market_id.clone(),
        legs: state
            .legs
            .iter()
            .map(|l| LegSnapshot {
                token_id: l.token_id.clone(),
                best_ask: l.best_ask,
                best_bid: l.best_bid,
                best_ask_size_best: l.best_ask_size_best,
                best_bid_size_best: l.best_bid_size_best,
                ask_depth3_usdc: l.ask_depth3_usdc,
                ts_recv_us: l.ts_recv_us,
            })
            .collect(),
    };
    let _ = snap_tx.send(Some(snap));
}

#[derive(Clone, Copy)]
enum PriceSide {
    Bid,
    Ask,
}

fn best_level(levels: &[serde_json::Value], side: PriceSide) -> Option<(f64, f64)> {
    let mut best: Option<(f64, f64)> = None;
    for lvl in levels {
        let Some(px) = parse_f64(lvl.get("price")).filter(|v| v.is_finite() && *v > 0.0) else {
            continue;
        };

        let sz = parse_f64(lvl.get("size"))
            .filter(|s| s.is_finite() && *s > 0.0)
            .unwrap_or(0.0);

        best = match (best, side) {
            (None, _) => Some((px, sz)),
            (Some((cur_px, cur_sz)), PriceSide::Bid) => {
                if px > cur_px {
                    Some((px, sz))
                } else {
                    Some((cur_px, cur_sz))
                }
            }
            (Some((cur_px, cur_sz)), PriceSide::Ask) => {
                if px < cur_px {
                    Some((px, sz))
                } else {
                    Some((cur_px, cur_sz))
                }
            }
        };
    }
    best
}

fn ask_depth3_usdc(levels: &[serde_json::Value]) -> f64 {
    let mut best = [(f64::INFINITY, 0.0f64); 3];
    for lvl in levels {
        let Some(px) = parse_f64(lvl.get("price")).filter(|v| v.is_finite() && *v > 0.0) else {
            continue;
        };
        let Some(sz) = parse_f64(lvl.get("size")).filter(|v| v.is_finite() && *v > 0.0) else {
            continue;
        };

        if px >= best[2].0 {
            continue;
        }

        best[2] = (px, sz);
        best.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    }

    best.iter()
        .filter(|(px, _)| px.is_finite())
        .map(|(px, sz)| px * sz)
        .sum()
}

#[derive(Debug, Deserialize)]
struct DataApiTrade {
    #[serde(rename = "asset")]
    asset_id: String,
    #[serde(rename = "conditionId")]
    market_id: String,
    size: f64,
    price: f64,
    timestamp: u64,
    #[serde(rename = "transactionHash")]
    transaction_hash: String,
}

pub async fn run_trades_poller(
    cfg: Config,
    markets: Vec<MarketDef>,
    trade_tx: mpsc::Sender<TradeTick>,
    trades_path: PathBuf,
    health: Arc<HealthCounters>,
    health_tx: mpsc::Sender<HealthLine>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut trades = CsvAppender::open(trades_path, &TRADES_HEADER).context("open trades.csv")?;

    let client = reqwest::Client::builder()
        .user_agent(concat!("razor/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_millis(
            cfg.polymarket.http_connect_timeout_ms,
        ))
        .timeout(Duration::from_millis(cfg.polymarket.http_timeout_ms))
        .build()
        .context("build http client")?;

    let mut allowed_tokens: HashSet<String> = HashSet::new();
    let market_ids: Vec<String> = markets
        .into_iter()
        .inspect(|m| {
            for t in &m.token_ids {
                allowed_tokens.insert(t.clone());
            }
        })
        .map(|m| m.market_id)
        .collect();
    let market_param = market_ids.join(",");

    let url = format!(
        "{}/trades",
        cfg.polymarket.data_api_base.trim_end_matches('/')
    );
    let mut last_ts: u64 = 0;
    let mut seen_at_last_ts: HashSet<String> = HashSet::new();
    let mut recent_ids: HashSet<String> = HashSet::new();
    let mut recent_queue: std::collections::VecDeque<(u64, String)> =
        std::collections::VecDeque::new();
    let mut last_drop_log_ms: u64 = 0;
    let mut dropped_trades: u64 = 0;

    let mut interval =
        tokio::time::interval(Duration::from_millis(cfg.shadow.trade_poll_interval_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
            _ = interval.tick() => {}
        }
        if *shutdown.borrow() {
            break;
        }

        let resp = match client
            .get(&url)
            .query(&[
                ("limit", cfg.shadow.trade_poll_limit.to_string()),
                ("takerOnly", cfg.shadow.trade_poll_taker_only.to_string()),
                ("market", market_param.clone()),
            ])
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "data-api trades request failed");
                continue;
            }
        };

        let list: Vec<DataApiTrade> = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "data-api trades decode failed");
                continue;
            }
        };

        let returned_count = list.len();
        if returned_count >= cfg.shadow.trade_poll_limit {
            health.inc_trade_poll_hit_limit(1);
            let mut earliest = u64::MAX;
            let mut latest = 0u64;
            for t in &list {
                let ts_ms = normalize_ts_ms(t.timestamp);
                earliest = earliest.min(ts_ms);
                latest = latest.max(ts_ms);
            }
            warn!(
                returned_count,
                limit = cfg.shadow.trade_poll_limit,
                earliest_ts_ms = earliest,
                latest_ts_ms = latest,
                "data-api trades poll hit limit; may be missing trades"
            );
            let _ = health_tx
                .try_send(HealthLine::TradePollHitLimit {
                    ts_ms: now_ms(),
                    returned_count,
                    earliest_ts_ms: earliest,
                    latest_ts_ms: latest,
                })
                .map_err(|_| ());
        }

        let mut list = list;
        list.sort_by(|a, b| {
            a.timestamp
                .cmp(&b.timestamp)
                .then_with(|| a.transaction_hash.cmp(&b.transaction_hash))
        });

        let mut max_ts = last_ts;
        let mut ids_at_max_ts: HashSet<String> = HashSet::new();

        for t in list {
            if !t.price.is_finite() || !t.size.is_finite() || t.price < 0.0 || t.size <= 0.0 {
                health.inc_trades_invalid(1);
                continue;
            }
            if t.price > 1.0 {
                health.inc_trades_invalid(1);
                continue;
            }

            if t.asset_id.trim().is_empty() {
                warn!(
                    market_id = %t.market_id,
                    "data-api trade missing token_id/asset; skipping tick to avoid shadow pollution"
                );
                continue;
            }
            if !allowed_tokens.contains(&t.asset_id) {
                warn!(
                    market_id = %t.market_id,
                    token_id = %t.asset_id,
                    "data-api trade token_id not in configured market token set; skipping"
                );
                continue;
            }

            let trade_ts_ms = normalize_ts_ms(t.timestamp);
            let trade_id = dedup_key(
                &t.market_id,
                &t.asset_id,
                trade_ts_ms,
                t.price,
                t.size,
                &t.transaction_hash,
            );

            let is_new = if t.timestamp > last_ts {
                true
            } else if t.timestamp == last_ts {
                !seen_at_last_ts.contains(&trade_id)
            } else {
                false
            };

            if !is_new {
                continue;
            }

            let now = now_ms();
            expire_recent_ids(
                now,
                cfg.shadow.trade_retention_ms,
                &mut recent_queue,
                &mut recent_ids,
            );

            if recent_ids.contains(&trade_id) {
                health.inc_trades_duplicated(1);
                continue;
            }
            recent_ids.insert(trade_id.clone());
            recent_queue.push_back((now, trade_id.clone()));

            // Phase 1 uses local ingest time as the canonical timestamp domain for shadow windows.
            let ingest_ts_ms = now;
            let ts_ms = ingest_ts_ms;
            let tick = TradeTick {
                ts_ms,
                ingest_ts_ms,
                exchange_ts_ms: Some(trade_ts_ms),
                market_id: t.market_id.clone(),
                token_id: t.asset_id.clone(),
                price: t.price,
                size: t.size,
                trade_id: trade_id.clone(),
            };

            trades.write_record([
                tick.ts_ms.to_string(),
                tick.market_id.clone(),
                tick.token_id.clone(),
                tick.price.to_string(),
                tick.size.to_string(),
                tick.trade_id.clone(),
                tick.ingest_ts_ms.to_string(),
                tick.exchange_ts_ms
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
            ])?;
            health.inc_trades_written(1);
            health.set_last_trade_ingest_ms(tick.ingest_ts_ms);

            match trade_tx.try_send(tick) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    health.inc_trades_dropped(1);
                    dropped_trades = dropped_trades.saturating_add(1);
                    if now.saturating_sub(last_drop_log_ms) >= 10_000 {
                        last_drop_log_ms = now;
                        warn!(
                            dropped_trades,
                            "trade channel full; dropping trades (Phase1 allows drop)"
                        );
                    }
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    return Err(anyhow::anyhow!("trade receiver dropped"));
                }
            }

            if t.timestamp > max_ts {
                max_ts = t.timestamp;
                ids_at_max_ts.clear();
            }
            if t.timestamp == max_ts {
                ids_at_max_ts.insert(trade_id);
            }
        }

        if max_ts > last_ts {
            last_ts = max_ts;
            seen_at_last_ts = ids_at_max_ts;
        } else if max_ts == last_ts && !ids_at_max_ts.is_empty() {
            seen_at_last_ts.extend(ids_at_max_ts);
        }
    }

    trades.flush_and_sync().context("flush trades.csv")?;
    Ok(())
}

fn normalize_ts_ms(ts: u64) -> u64 {
    // If the API gives seconds, normalize to ms.
    if ts < 1_000_000_000_000 {
        ts.saturating_mul(1000)
    } else {
        ts
    }
}

fn dedup_key(
    market_id: &str,
    token_id: &str,
    ts_ms: u64,
    price: f64,
    size: f64,
    transaction_hash: &str,
) -> String {
    let tx = transaction_hash.trim();
    if !tx.is_empty() {
        // Do not use bare `transactionHash` as a unique trade ID: a single tx can contain
        // multiple fills. Include token+ts+price+size to avoid false de-duplication.
        format!(
            "tx:{market_id}:{token_id}:{ts_ms}:{tx}:{:016x}:{:016x}",
            price.to_bits(),
            size.to_bits()
        )
    } else {
        format!(
            "weak:{market_id}:{token_id}:{ts_ms}:{:016x}:{:016x}",
            price.to_bits(),
            size.to_bits()
        )
    }
}

fn expire_recent_ids(
    now_ms: u64,
    retention_ms: u64,
    q: &mut std::collections::VecDeque<(u64, String)>,
    set: &mut HashSet<String>,
) {
    if retention_ms == 0 {
        q.clear();
        set.clear();
        return;
    }

    let cutoff = now_ms.saturating_sub(retention_ms);
    while q.front().is_some_and(|(ts, _)| *ts < cutoff) {
        if let Some((_, id)) = q.pop_front() {
            set.remove(&id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_approx_eq::assert_approx_eq;
    use serde_json::json;
    use tokio::sync::watch;

    #[test]
    fn ws_book_level_parses_numeric_and_string_fields() {
        let bids = vec![
            json!({"price": 0.49, "size": 1.0}),
            json!({"price": "0.5", "size": "2"}),
        ];
        let (px, sz) = best_level(&bids, PriceSide::Bid).expect("best bid");
        assert_approx_eq!(px, 0.5);
        assert_approx_eq!(sz, 2.0);

        let asks = vec![
            json!({"price": 0.6, "size": 1.0}),
            json!({"price": "0.55", "size": "2"}),
        ];
        let (px, sz) = best_level(&asks, PriceSide::Ask).expect("best ask");
        assert_approx_eq!(px, 0.55);
        assert_approx_eq!(sz, 2.0);
    }

    #[test]
    fn ws_book_depth3_parses_numeric_and_sums_top3() {
        let asks = vec![
            json!({"price": 0.6, "size": 10.0}),    // 6
            json!({"price": "0.55", "size": 20.0}), // 11
            json!({"price": 0.50, "size": "30"}),   // 15
            json!({"price": 0.65, "size": 40.0}),   // excluded (higher price)
        ];
        let d = ask_depth3_usdc(&asks);
        assert_approx_eq!(d, 32.0);
    }

    #[test]
    fn ws_book_market_id_uses_token_mapping_when_mismatched() {
        let tmp = std::env::temp_dir().join(format!(
            "razor_ticks_test_{}_{}.csv",
            std::process::id(),
            crate::types::now_ms()
        ));
        let mut ticks = CsvAppender::open(&tmp, &TICKS_HEADER).expect("open ticks csv");

        let mut token_to_market: HashMap<String, (String, usize)> = HashMap::new();
        token_to_market.insert("t1".to_string(), ("m1".to_string(), 0));

        let mut market_states: HashMap<String, MarketState> = HashMap::new();
        market_states.insert(
            "m1".to_string(),
            MarketState {
                market_id: "m1".to_string(),
                legs: vec![LegState {
                    token_id: "t1".to_string(),
                    best_ask: 0.0,
                    best_ask_size_best: 0.0,
                    best_bid: 0.0,
                    best_bid_size_best: 0.0,
                    ask_depth3_usdc: 0.0,
                    ts_recv_us: 0,
                    last_tick_log_ms: 0,
                    ready: false,
                }],
            },
        );

        let (snap_tx, snap_rx) = watch::channel::<Option<MarketSnapshot>>(None);
        let health = HealthCounters::default();

        let v = json!({
            "event_type": "book",
            // Mismatched market id from WS.
            "market": "mX",
            "asset_id": "t1",
            "bids": [{"price": 0.49, "size": 1.0}],
            "asks": [{"price": 0.50, "size": 2.0}],
        });
        let obj = v.as_object().expect("obj").clone();

        handle_ws_book(
            obj,
            &token_to_market,
            &mut market_states,
            &mut ticks,
            &snap_tx,
            &health,
        )
        .expect("handle_ws_book");
        ticks.flush_and_sync().expect("flush ticks");

        // Snapshot should publish under the mapped market_id.
        let snap = snap_rx.borrow().clone().expect("snapshot published");
        assert_eq!(snap.market_id, "m1");
        assert_eq!(snap.legs.len(), 1);
        assert_eq!(snap.legs[0].token_id, "t1");

        // Tick row must also use mapped market_id (m1), not ws field (mX).
        let text = std::fs::read_to_string(&tmp).expect("read ticks");
        let mut lines = text.lines();
        let _header = lines.next().expect("header");
        let row = lines.next().expect("row");
        let cols: Vec<&str> = row.split(',').collect();
        assert_eq!(cols[1], "m1");
        assert_eq!(cols[2], "t1");
    }
}
