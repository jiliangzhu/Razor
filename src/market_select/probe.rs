use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Context as _;
use futures_util::{SinkExt as _, StreamExt as _};
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::buckets::classify_bucket;
use crate::config::Config;
use crate::json_util::parse_f64;
use crate::market_select::gamma::GammaMarket;
use crate::market_select::metrics::{self, MarketScoreRowComputed, SnapshotAccum, TradesAccum};
use crate::types::{now_ms, now_us, LegSnapshot, MarketSnapshot};

static SIM_HTTP_429_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
struct LegState {
    token_id: String,
    best_bid: f64,
    best_ask: f64,
    ask_depth3_usdc: f64,
    ready: bool,
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
}

pub async fn probe_market(
    cfg: &Config,
    run_id: &str,
    m: &GammaMarket,
    probe_seconds: u64,
) -> anyhow::Result<MarketScoreRowComputed> {
    let probe_start_ms = now_ms();
    let probe_end_ms = probe_start_ms.saturating_add(probe_seconds.saturating_mul(1000));

    let mut legs: Vec<LegState> = m
        .token_ids
        .iter()
        .map(|t| LegState {
            token_id: t.clone(),
            best_bid: 0.0,
            best_ask: 0.0,
            ask_depth3_usdc: 0.0,
            ready: false,
        })
        .collect();

    let ws_url = format!("{}/ws/market", cfg.polymarket.ws_base.trim_end_matches('/'));
    let subscribe_msg = serde_json::json!({
        "assets_ids": m.token_ids,
        "type": "market",
    });

    let client = reqwest::Client::builder()
        .user_agent(concat!("razor/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_millis(
            cfg.polymarket.http_connect_timeout_ms,
        ))
        .timeout(Duration::from_millis(cfg.polymarket.http_timeout_ms))
        .build()
        .context("build http client")?;

    let trades_url = format!(
        "{}/trades",
        cfg.polymarket.data_api_base.trim_end_matches('/')
    );

    let mut snap_acc = SnapshotAccum::default();
    let mut trades_acc = TradesAccum::default();
    let mut trade_dedup: HashSet<String> = HashSet::new();

    let mut backoff = Duration::from_secs(1);
    let mut ws_connected = false;

    // Trade polling tick is derived from Phase 1 config.
    let mut trade_tick = tokio::time::interval(Duration::from_millis(
        cfg.shadow.trade_poll_interval_ms.max(200),
    ));
    trade_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Snapshot sample tick: fixed interval so samples_total stays bounded and comparable.
    let mut sample_tick =
        tokio::time::interval(Duration::from_millis(metrics::SNAPSHOT_SAMPLE_INTERVAL_MS));
    sample_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    while now_ms() < probe_end_ms {
        // (Re)connect WS as needed.
        if !ws_connected {
            if now_ms() >= probe_end_ms {
                break;
            }
            match tokio::time::timeout(
                Duration::from_millis(cfg.polymarket.ws_connect_timeout_ms),
                tokio_tungstenite::connect_async(&ws_url),
            )
            .await
            {
                Ok(Ok((ws, _))) => {
                    let (mut sink, mut stream) = ws.split();
                    ws_send(
                        &mut sink,
                        Message::Text(subscribe_msg.to_string().into()),
                        Duration::from_millis(cfg.polymarket.ws_write_timeout_ms),
                    )
                    .await
                    .context("send ws subscribe")?;
                    ws_connected = true;
                    backoff = Duration::from_secs(1);
                    info!(
                        gamma_id = %m.gamma_id,
                        condition_id = %m.condition_id,
                        tokens = m.token_ids.len(),
                        "probe ws connected"
                    );

                    let mut ping = tokio::time::interval(Duration::from_secs(10));
                    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                    loop {
                        if now_ms() >= probe_end_ms {
                            break;
                        }
                        tokio::select! {
                            _ = ping.tick() => {
                                let _ = ws_send(
                                    &mut sink,
                                    Message::Text("PING".to_string().into()),
                                    Duration::from_millis(cfg.polymarket.ws_write_timeout_ms),
                                ).await;
                            }
                            _ = trade_tick.tick() => {
                                poll_trades(
                                    &client,
                                    &trades_url,
                                    cfg.shadow.trade_poll_limit,
                                    cfg.shadow.trade_poll_taker_only,
                                    &m.condition_id,
                                    &mut trades_acc,
                                    &mut trade_dedup,
                                )
                                .await;
                            }
                            _ = sample_tick.tick() => {
                                sample_snapshot(cfg, m, run_id, probe_start_ms, probe_end_ms, probe_seconds, &mut legs, &mut snap_acc, &mut trades_acc)?;
                            }
                            msg = stream.next() => {
                                let Some(msg) = msg else {
                                    ws_connected = false;
                                    break;
                                };
                                match msg {
                                    Ok(Message::Text(txt)) => {
                                        handle_ws_text(&txt, &mut legs)?;
                                    }
                                    Ok(Message::Binary(bin)) => {
                                        let txt = String::from_utf8_lossy(&bin);
                                        handle_ws_text(&txt, &mut legs)?;
                                    }
                                    Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
                                    Ok(Message::Close(_)) => {
                                        ws_connected = false;
                                        break;
                                    }
                                    Ok(_) => {}
                                    Err(_) => {
                                        ws_connected = false;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(Err(e)) => {
                    warn!(
                        gamma_id = %m.gamma_id,
                        condition_id = %m.condition_id,
                        error = %e,
                        backoff_ms = backoff.as_millis() as u64,
                        "probe ws connect failed; backing off"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
                Err(_) => {
                    warn!(
                        gamma_id = %m.gamma_id,
                        condition_id = %m.condition_id,
                        timeout_ms = cfg.polymarket.ws_connect_timeout_ms,
                        backoff_ms = backoff.as_millis() as u64,
                        "probe ws connect timeout; backing off"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
        } else {
            // Should not happen; WS loop manages connected state.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // Final sample to include last state.
    sample_snapshot(
        cfg,
        m,
        run_id,
        probe_start_ms,
        probe_end_ms,
        probe_seconds,
        &mut legs,
        &mut snap_acc,
        &mut trades_acc,
    )?;

    // Ensure sorted timestamps for gap metrics.
    trades_acc.trade_ts_ms.sort_unstable();
    trades_acc.poll_ok_ts_ms.sort_unstable();
    snap_acc.passes_ts_ms.sort_unstable();

    Ok(metrics::compute_row(
        run_id,
        probe_start_ms,
        probe_end_ms,
        probe_seconds,
        &m.gamma_id,
        &m.condition_id,
        m.token_ids.len(),
        &m.strategy,
        &m.token_ids,
        m.volume24hr,
        m.liquidity,
        m.market_phase,
        snap_acc,
        trades_acc,
        cfg.brain.min_net_edge_bps,
        cfg.shadow.trade_poll_limit,
    ))
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

#[allow(clippy::too_many_arguments)]
fn sample_snapshot(
    cfg: &Config,
    m: &GammaMarket,
    _run_id: &str,
    _probe_start_ms: u64,
    _probe_end_ms: u64,
    _probe_seconds: u64,
    legs: &mut [LegState],
    snap_acc: &mut SnapshotAccum,
    _trades_acc: &mut TradesAccum,
) -> anyhow::Result<()> {
    // Only sample when we have asks for all legs (as in Phase 1 pipeline).
    if !legs.iter().all(|l| l.ready) {
        return Ok(());
    }

    let ts_recv_us = now_us();
    let ts_ms = ts_recv_us / 1000;

    // Apply depth3 sanity-check (Phase1 selector-specific).
    let mut depth3_degraded = false;
    let mut snap_legs: Vec<LegSnapshot> = Vec::with_capacity(legs.len());
    for l in legs.iter() {
        let mut depth3 = l.ask_depth3_usdc;
        if metrics::depth3_is_degraded(depth3) {
            depth3_degraded = true;
            depth3 = 0.0; // force degrade in bucket classifier
        }
        snap_legs.push(LegSnapshot {
            token_id: l.token_id.clone(),
            best_ask: l.best_ask,
            best_bid: l.best_bid,
            best_ask_size_best: 0.0,
            best_bid_size_best: 0.0,
            ask_depth3_usdc: depth3,
            ts_recv_us,
        });
    }

    let snapshot = MarketSnapshot {
        market_id: m.condition_id.clone(),
        legs: snap_legs,
    };

    let bucket_decision = classify_bucket(&snapshot);
    let bucket = bucket_decision.bucket;

    let best_bids: Vec<f64> = snapshot.legs.iter().map(|l| l.best_bid).collect();
    let best_asks: Vec<f64> = snapshot.legs.iter().map(|l| l.best_ask).collect();
    let depth3_usdc: Vec<f64> = snapshot.legs.iter().map(|l| l.ask_depth3_usdc).collect();

    let sum_ask: f64 = best_asks.iter().copied().sum();
    let expected_net_bps = metrics::compute_expected_net_bps(sum_ask, cfg.brain.risk_premium_bps);
    let passes = expected_net_bps.is_some_and(|v| v >= cfg.brain.min_net_edge_bps);

    snap_acc.push_snapshot(
        ts_ms,
        &best_bids,
        &best_asks,
        &depth3_usdc,
        bucket,
        &bucket_decision,
        depth3_degraded,
        expected_net_bps,
        passes,
    );

    Ok(())
}

async fn poll_trades(
    client: &reqwest::Client,
    url: &str,
    trade_poll_limit: usize,
    trade_poll_taker_only: bool,
    condition_id: &str,
    trades_acc: &mut TradesAccum,
    trade_dedup: &mut HashSet<String>,
) {
    if let Some(every) = env_u64("RAZOR_SIM_HTTP_429_EVERY") {
        if every > 0 {
            let seq = SIM_HTTP_429_SEQ
                .fetch_add(1, Ordering::Relaxed)
                .saturating_add(1);
            if seq % every == 0 {
                warn!(
                    condition_id,
                    seq, every, "SIM injected HTTP 429 (skipping this poll)"
                );
                return;
            }
        }
    }

    let resp = match client
        .get(url)
        .query(&[
            ("limit", trade_poll_limit.to_string()),
            ("takerOnly", trade_poll_taker_only.to_string()),
            ("market", condition_id.to_string()),
        ])
        .send()
        .await
    {
        Ok(r) => r,
        Err(_) => return,
    };

    let list: Vec<DataApiTrade> = match resp.json().await {
        Ok(v) => v,
        Err(_) => return,
    };

    trades_acc.poll_ok_ts_ms.push(now_ms());

    if list.len() == trade_poll_limit {
        trades_acc.trade_poll_hit_limit_count += 1;
    }

    for t in list {
        if t.asset_id.trim().is_empty() {
            continue;
        }
        if t.market_id != condition_id {
            continue;
        }
        if !t.price.is_finite() || !t.size.is_finite() || t.price < 0.0 || t.size <= 0.0 {
            continue;
        }

        let exchange_ts_ms = normalize_ts_ms(t.timestamp);
        let ingest_ts_ms = now_ms();
        // Frozen timestamp domain (Phase 1 / market_select): use **local ingest time** for
        // all window/gap/coverage statistics so results are directly comparable with Phase 1
        // shadow windows.
        let stat_ts_ms = ingest_ts_ms;

        // Required dedup key:
        // - Prefer (condition_id, token_id, exchange_ts_ms, price, size)
        // - If exchange ts is missing/zero: fall back to ingest_ts_ms.
        let key_ts_ms = if exchange_ts_ms > 0 {
            exchange_ts_ms
        } else {
            ingest_ts_ms
        };
        let key = format!(
            "{}:{}:{}:{:016x}:{:016x}",
            condition_id,
            t.asset_id,
            key_ts_ms,
            t.price.to_bits(),
            t.size.to_bits()
        );

        if trade_dedup.contains(&key) {
            trades_acc.trades_duplicated_count += 1;
            continue;
        }
        trade_dedup.insert(key);

        trades_acc.trades_total += 1;
        trades_acc.trade_ts_ms.push(stat_ts_ms);
    }
}

fn env_u64(name: &str) -> Option<u64> {
    let raw = std::env::var(name).ok()?;
    raw.trim().parse::<u64>().ok()
}

fn normalize_ts_ms(ts: u64) -> u64 {
    // Normalize unix timestamps to milliseconds (see `feed::normalize_ts_ms`).
    match ts {
        0..=99_999_999_999 => ts.saturating_mul(1_000),
        100_000_000_000..=99_999_999_999_999 => ts,
        100_000_000_000_000..=99_999_999_999_999_999 => ts / 1_000,
        _ => ts / 1_000_000,
    }
}

fn handle_ws_text(txt: &str, legs: &mut [LegState]) -> anyhow::Result<()> {
    if txt == "PONG" {
        return Ok(());
    }

    let v: serde_json::Value = match serde_json::from_str(txt) {
        Ok(v) => v,
        Err(_) => return Ok(()),
    };

    match v {
        serde_json::Value::Array(items) => {
            for item in items {
                if let serde_json::Value::Object(obj) = item {
                    handle_ws_obj(obj, legs)?;
                }
            }
        }
        serde_json::Value::Object(obj) => {
            handle_ws_obj(obj, legs)?;
        }
        _ => {}
    }

    Ok(())
}

fn handle_ws_obj(
    obj: serde_json::Map<String, serde_json::Value>,
    legs: &mut [LegState],
) -> anyhow::Result<()> {
    let Some(event_type) = obj.get("event_type").and_then(|v| v.as_str()) else {
        return Ok(());
    };

    match event_type {
        "book" => handle_ws_book(obj, legs),
        "price_change" => handle_ws_price_change(obj, legs),
        _ => Ok(()),
    }
}

fn handle_ws_book(
    obj: serde_json::Map<String, serde_json::Value>,
    legs: &mut [LegState],
) -> anyhow::Result<()> {
    let Some(token_id) = obj.get("asset_id").and_then(|v| v.as_str()) else {
        return Ok(());
    };
    let Some(idx) = legs.iter().position(|l| l.token_id == token_id) else {
        return Ok(());
    };

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

    let (best_bid, _best_bid_sz) = best_level(bids, PriceSide::Bid).unwrap_or((0.0, 0.0));
    let (best_ask, _best_ask_sz) = best_level(asks, PriceSide::Ask).unwrap_or((1.0, 0.0));
    let ask_depth3_usdc = ask_depth3_usdc(asks);

    let leg = &mut legs[idx];
    leg.best_bid = best_bid;
    leg.best_ask = best_ask;
    leg.ask_depth3_usdc = ask_depth3_usdc;
    leg.ready = leg.best_ask.is_finite() && leg.best_ask > 0.0;
    Ok(())
}

fn handle_ws_price_change(
    obj: serde_json::Map<String, serde_json::Value>,
    legs: &mut [LegState],
) -> anyhow::Result<()> {
    let Some(changes) = obj.get("price_changes").and_then(|v| v.as_array()) else {
        return Ok(());
    };

    for ch in changes {
        let Some(ch) = ch.as_object() else { continue };
        let Some(token_id) = ch.get("asset_id").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(idx) = legs.iter().position(|l| l.token_id == token_id) else {
            continue;
        };
        let best_bid = parse_f64(ch.get("best_bid")).unwrap_or(0.0);
        let best_ask = parse_f64(ch.get("best_ask")).unwrap_or(0.0);

        let leg = &mut legs[idx];
        leg.best_bid = if best_bid.is_finite() && best_bid > 0.0 {
            best_bid
        } else {
            0.0
        };
        leg.best_ask = if best_ask.is_finite() && best_ask > 0.0 {
            best_ask
        } else {
            1.0
        };
        leg.ready = leg.best_ask.is_finite() && leg.best_ask > 0.0;
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

#[cfg(test)]
mod tests {
    use super::*;
    use assert_approx_eq::assert_approx_eq;
    use serde_json::json;

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
            json!({"price": 0.65, "size": 40.0}),   // excluded
        ];
        let d = ask_depth3_usdc(&asks);
        assert_approx_eq!(d, 32.0);
    }

    #[test]
    fn http_429_every_k_logic_is_stable() {
        fn fires(seq: u64, every: u64) -> bool {
            every > 0 && seq % every == 0
        }
        assert!(!fires(1, 3));
        assert!(!fires(2, 3));
        assert!(fires(3, 3));
        assert!(!fires(4, 3));
        assert!(!fires(5, 3));
        assert!(fires(6, 3));
    }
}
