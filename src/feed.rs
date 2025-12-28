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

#[derive(Debug, Deserialize)]
struct GammaMarket {
    #[serde(rename = "conditionId")]
    condition_id: String,
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: String,
}

pub async fn fetch_markets(cfg: &Config) -> anyhow::Result<Vec<MarketDef>> {
    let client = reqwest::Client::builder()
        .user_agent(concat!("razor/", env!("CARGO_PKG_VERSION")))
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

        out.push(MarketDef {
            market_id: m.condition_id,
            token_ids,
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
    let mut raw = JsonlAppender::open(raw_ws_path).context("open raw_ws.jsonl")?;

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
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    info!(%ws_url, tokens = subscribe_tokens.len(), "connecting ws");
    if *shutdown.borrow() {
        return Ok(());
    }
    let (ws, _) = tokio_tungstenite::connect_async(ws_url)
        .await
        .context("connect ws")?;

    let (mut sink, mut stream) = ws.split();

    let subscribe_msg = serde_json::json!({
        "assets_ids": subscribe_tokens,
        "type": "market",
    });

    sink.send(Message::Text(subscribe_msg.to_string().into()))
        .await
        .context("send subscribe")?;

    let mut ping = tokio::time::interval(Duration::from_secs(10));

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return Ok(());
                }
            }
            _ = ping.tick() => {
                sink.send(Message::Text("PING".to_string().into()))
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

    if event_type != "book" {
        return Ok(());
    }

    let Some(market_id) = obj.get("market").and_then(|v| v.as_str()) else {
        return Ok(());
    };
    let Some(token_id) = obj.get("asset_id").and_then(|v| v.as_str()) else {
        return Ok(());
    };

    if !token_to_market.contains_key(token_id) {
        return Ok(());
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

    let Some((best_bid, best_bid_size_best)) = best_level(bids, PriceSide::Bid) else {
        return Ok(());
    };
    let Some((best_ask, best_ask_size_best)) = best_level(asks, PriceSide::Ask) else {
        return Ok(());
    };

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

    let Some((_, idx)) = token_to_market.get(token_id) else {
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
    leg.ready = true;

    if state.legs.iter().all(|l| l.ready) {
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

    Ok(())
}

#[derive(Clone, Copy)]
enum PriceSide {
    Bid,
    Ask,
}

fn best_level(levels: &[serde_json::Value], side: PriceSide) -> Option<(f64, f64)> {
    let mut best: Option<(f64, f64)> = None;
    for lvl in levels {
        let Some(p) = lvl.get("price").and_then(|v| v.as_str()) else {
            continue;
        };
        let Ok(px) = p.parse::<f64>() else {
            continue;
        };

        let sz = lvl
            .get("size")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
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
        let Some(p) = lvl.get("price").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(s) = lvl.get("size").and_then(|v| v.as_str()) else {
            continue;
        };
        let Ok(px) = p.parse::<f64>() else {
            continue;
        };
        let Ok(sz) = s.parse::<f64>() else {
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
                ("takerOnly", "true".to_string()),
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
        let mut hashes_at_max_ts: HashSet<String> = HashSet::new();

        for t in list {
            let is_new = if t.timestamp > last_ts {
                true
            } else if t.timestamp == last_ts {
                !seen_at_last_ts.contains(&t.transaction_hash)
            } else {
                false
            };

            if !is_new {
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

            let now = now_ms();
            expire_recent_ids(
                now,
                cfg.shadow.trade_retention_ms,
                &mut recent_queue,
                &mut recent_ids,
            );

            let trade_ts_ms = normalize_ts_ms(t.timestamp);
            let trade_id = dedup_key(
                &t.market_id,
                &t.asset_id,
                trade_ts_ms,
                t.price,
                t.size,
                &t.transaction_hash,
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
                hashes_at_max_ts.clear();
            }
            if t.timestamp == max_ts {
                hashes_at_max_ts.insert(t.transaction_hash);
            }
        }

        if max_ts > last_ts {
            last_ts = max_ts;
            seen_at_last_ts = hashes_at_max_ts;
        } else if max_ts == last_ts && !hashes_at_max_ts.is_empty() {
            seen_at_last_ts.extend(hashes_at_max_ts);
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
    if !transaction_hash.trim().is_empty() {
        transaction_hash.trim().to_string()
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
