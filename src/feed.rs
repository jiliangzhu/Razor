use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context as _;
use futures_util::{SinkExt as _, StreamExt as _};
use serde::Deserialize;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::recorder::{CsvAppender, JsonlAppender, TICKS_HEADER, TRADES_HEADER};
use crate::types::{now_us, LegSnapshot, MarketDef, MarketSnapshot, TradeTick};

#[derive(Debug, Deserialize)]
struct GammaMarket {
    #[serde(rename = "conditionId")]
    condition_id: String,
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: String,
}

pub async fn fetch_markets(cfg: &Config) -> anyhow::Result<Vec<MarketDef>> {
    let client = reqwest::Client::builder()
        .user_agent("razor/1.3.2a")
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
    best_bid: f64,
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
                best_bid: 0.0,
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
        match ws_run_once(
            &ws_url,
            &subscribe_tokens,
            &token_to_market,
            &mut market_states,
            &mut ticks,
            &mut raw,
            &snap_tx,
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
}

async fn ws_run_once(
    ws_url: &str,
    subscribe_tokens: &[String],
    token_to_market: &HashMap<String, (String, usize)>,
    market_states: &mut HashMap<String, MarketState>,
    ticks: &mut CsvAppender,
    raw: &mut JsonlAppender,
    snap_tx: &watch::Sender<Option<MarketSnapshot>>,
) -> anyhow::Result<()> {
    info!(%ws_url, tokens = subscribe_tokens.len(), "connecting ws");
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
                        handle_ws_text(&txt, token_to_market, market_states, ticks, raw, snap_tx).await?;
                    }
                    Message::Binary(bin) => {
                        let txt = String::from_utf8_lossy(&bin);
                        handle_ws_text(&txt, token_to_market, market_states, ticks, raw, snap_tx).await?;
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
                    handle_ws_obj(obj, token_to_market, market_states, ticks, snap_tx)?;
                }
            }
        }
        serde_json::Value::Object(obj) => {
            handle_ws_obj(obj, token_to_market, market_states, ticks, snap_tx)?;
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

    let Some(best_bid) = best_price(bids, PriceSide::Bid) else {
        return Ok(());
    };
    let Some(best_ask) = best_price(asks, PriceSide::Ask) else {
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

fn best_price(levels: &[serde_json::Value], side: PriceSide) -> Option<f64> {
    let mut best: Option<f64> = None;
    for lvl in levels {
        let Some(p) = lvl.get("price").and_then(|v| v.as_str()) else {
            continue;
        };
        let Ok(px) = p.parse::<f64>() else {
            continue;
        };
        best = match (best, side) {
            (None, _) => Some(px),
            (Some(cur), PriceSide::Bid) => Some(cur.max(px)),
            (Some(cur), PriceSide::Ask) => Some(cur.min(px)),
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
) -> anyhow::Result<()> {
    let mut trades = CsvAppender::open(trades_path, &TRADES_HEADER).context("open trades.csv")?;

    let client = reqwest::Client::builder()
        .user_agent("razor/1.3.2a")
        .build()
        .context("build http client")?;

    let market_ids: Vec<String> = markets.into_iter().map(|m| m.market_id).collect();
    let market_param = market_ids.join(",");

    let url = format!(
        "{}/trades",
        cfg.polymarket.data_api_base.trim_end_matches('/')
    );
    let mut last_ts: u64 = 0;
    let mut seen_at_last_ts: HashSet<String> = HashSet::new();

    let mut interval =
        tokio::time::interval(Duration::from_millis(cfg.shadow.trade_poll_interval_ms));

    loop {
        interval.tick().await;

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

            let ts_recv_us = now_us();
            let tick = TradeTick {
                ts_recv_us,
                market_id: t.market_id.clone(),
                token_id: t.asset_id.clone(),
                price: t.price,
                size: t.size,
            };

            trades.write_record([
                tick.ts_recv_us.to_string(),
                tick.market_id.clone(),
                tick.token_id.clone(),
                tick.price.to_string(),
                tick.size.to_string(),
            ])?;

            if trade_tx.send(tick).await.is_err() {
                return Err(anyhow::anyhow!("trade receiver dropped"));
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
}
