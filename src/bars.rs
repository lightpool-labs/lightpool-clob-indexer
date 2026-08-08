// Copyright (c) LightPool Labs

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::{broadcast, RwLock};

use crate::chain::{format_price_pieces, format_token_amount};
use crate::persist::{ClosedBarRow, SharedPersist};
use crate::spot_market::normalize_spot_market_key;

pub const INTERVAL_1M: &str = "1m";
pub const BAR_SECONDS_1M: u64 = 60;

#[derive(Debug, Clone, Serialize)]
pub struct Bar {
    pub spot_market: String,
    pub interval: String,
    pub start_ts: u64,
    pub open_raw: u64,
    pub high_raw: u64,
    pub low_raw: u64,
    pub close_raw: u64,
    pub volume_raw: u64,
    pub trade_count: u64,
    pub closed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct BarWsMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub spot_market: String,
    pub interval: String,
    pub start_ts: u64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub trade_count: u64,
    pub closed: bool,
}

impl Bar {
    pub fn to_ws_message(&self, msg_type: &str) -> BarWsMessage {
        BarWsMessage {
            msg_type: msg_type.into(),
            spot_market: self.spot_market.clone(),
            interval: self.interval.clone(),
            start_ts: self.start_ts,
            open: format_price_pieces(self.open_raw),
            high: format_price_pieces(self.high_raw),
            low: format_price_pieces(self.low_raw),
            close: format_price_pieces(self.close_raw),
            volume: format_token_amount(self.volume_raw),
            trade_count: self.trade_count,
            closed: self.closed,
        }
    }

    pub fn to_closed_row(&self) -> ClosedBarRow {
        ClosedBarRow {
            spot_market: self.spot_market.clone(),
            interval: self.interval.clone(),
            start_ts: self.start_ts,
            open_raw: self.open_raw,
            high_raw: self.high_raw,
            low_raw: self.low_raw,
            close_raw: self.close_raw,
            volume_raw: self.volume_raw,
            trade_count: self.trade_count,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BarsSnapshot {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub spot_market: String,
    pub interval: String,
    pub bars: Vec<BarWsMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub forming: Option<BarWsMessage>,
}

struct SpotBars {
    forming: Option<Bar>,
    publisher: Option<broadcast::Sender<BarWsMessage>>,
}

#[derive(Default)]
struct BarStoreInner {
    by_spot: HashMap<String, SpotBars>,
}

pub struct BarStore {
    inner: RwLock<BarStoreInner>,
    persist: Option<SharedPersist>,
}

pub type SharedBarStore = Arc<BarStore>;

impl BarStore {
    pub fn new(persist: Option<SharedPersist>) -> Self {
        Self {
            inner: RwLock::new(BarStoreInner::default()),
            persist,
        }
    }

    pub fn now_ts() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    pub fn minute_start(ts: u64) -> u64 {
        ts / BAR_SECONDS_1M * BAR_SECONDS_1M
    }

    async fn publisher(&self, spot_market: &str) -> broadcast::Sender<BarWsMessage> {
        let key = normalize_spot_market_key(spot_market);
        let mut inner = self.inner.write().await;
        let entry = inner.by_spot.entry(key).or_insert_with(|| SpotBars {
            forming: None,
            publisher: None,
        });
        if let Some(sender) = entry.publisher.clone() {
            return sender;
        }
        let (sender, _) = broadcast::channel(256);
        entry.publisher = Some(sender.clone());
        sender
    }

    pub async fn subscribe(&self, spot_market: &str) -> broadcast::Receiver<BarWsMessage> {
        self.publisher(spot_market).await.subscribe()
    }

    pub async fn forming(&self, spot_market: &str) -> Option<Bar> {
        let key = normalize_spot_market_key(spot_market);
        self.inner
            .read()
            .await
            .by_spot
            .get(&key)
            .and_then(|s| s.forming.clone())
    }

    pub async fn snapshot_ws(
        &self,
        spot_market: &str,
        history_limit: usize,
    ) -> BarsSnapshot {
        let key = normalize_spot_market_key(spot_market);
        let history = self
            .load_history(&key, INTERVAL_1M, None, None, history_limit)
            .await;
        let forming = self.forming(&key).await.map(|b| b.to_ws_message("bar"));
        BarsSnapshot {
            msg_type: "bars_snapshot".into(),
            spot_market: key,
            interval: INTERVAL_1M.into(),
            bars: history
                .into_iter()
                .map(|b| b.to_ws_message("bar_closed"))
                .collect(),
            forming,
        }
    }

    pub async fn load_history(
        &self,
        spot_market: &str,
        interval: &str,
        from_ts: Option<u64>,
        to_ts: Option<u64>,
        limit: usize,
    ) -> Vec<Bar> {
        let key = normalize_spot_market_key(spot_market);
        let Some(persist) = &self.persist else {
            return Vec::new();
        };
        let rows = match persist.load_closed_bars(
            &key,
            INTERVAL_1M,
            from_ts,
            to_ts,
            if interval == INTERVAL_1M || interval.is_empty() {
                limit.max(1)
            } else {
                50_000
            },
        ) {
            Ok(rows) => rows,
            Err(error) => {
                tracing::error!(error = %error, "load closed bars failed");
                return Vec::new();
            }
        };
        let ones: Vec<Bar> = rows
            .into_iter()
            .map(|row| Bar {
                spot_market: row.spot_market,
                interval: row.interval,
                start_ts: row.start_ts,
                open_raw: row.open_raw,
                high_raw: row.high_raw,
                low_raw: row.low_raw,
                close_raw: row.close_raw,
                volume_raw: row.volume_raw,
                trade_count: row.trade_count,
                closed: true,
            })
            .collect();
        let aggregated = aggregate_from_1m(&ones, interval);
        let len = aggregated.len();
        if len > limit {
            aggregated.into_iter().skip(len - limit).collect()
        } else {
            aggregated
        }
    }

    /// Apply a buy-side fill into the 1m forming bar (avoids double-counting taker+maker).
    pub async fn on_trade(&self, spot_market: &str, price_raw: u64, volume_raw: u64, ts: u64) {
        if volume_raw == 0 {
            return;
        }
        let key = normalize_spot_market_key(spot_market);
        let start_ts = Self::minute_start(ts);
        let mut closed_to_persist: Option<Bar> = None;
        let mut ws_msgs: Vec<BarWsMessage> = Vec::new();

        {
            let mut inner = self.inner.write().await;
            let entry = inner.by_spot.entry(key.clone()).or_insert_with(|| SpotBars {
                forming: None,
                publisher: None,
            });

            if let Some(forming) = entry.forming.as_ref() {
                if forming.start_ts != start_ts {
                    let mut closed = forming.clone();
                    closed.closed = true;
                    ws_msgs.push(closed.to_ws_message("bar_closed"));
                    closed_to_persist = Some(closed);
                    entry.forming = None;
                }
            }

            match entry.forming.as_mut() {
                Some(bar) => {
                    bar.high_raw = bar.high_raw.max(price_raw);
                    bar.low_raw = bar.low_raw.min(price_raw);
                    bar.close_raw = price_raw;
                    bar.volume_raw = bar.volume_raw.saturating_add(volume_raw);
                    bar.trade_count = bar.trade_count.saturating_add(1);
                }
                None => {
                    entry.forming = Some(Bar {
                        spot_market: key.clone(),
                        interval: INTERVAL_1M.into(),
                        start_ts,
                        open_raw: price_raw,
                        high_raw: price_raw,
                        low_raw: price_raw,
                        close_raw: price_raw,
                        volume_raw,
                        trade_count: 1,
                        closed: false,
                    });
                }
            }

            if let Some(bar) = entry.forming.as_ref() {
                ws_msgs.push(bar.to_ws_message("bar"));
            }
        }

        if let Some(closed) = closed_to_persist {
            self.persist_closed(&closed);
        }

        if !ws_msgs.is_empty() {
            let sender = self.publisher(&key).await;
            for msg in ws_msgs {
                let _ = sender.send(msg);
            }
        }
    }

    /// Close forming bars whose minute has ended (no trade needed).
    pub async fn close_expired(&self, now_ts: u64) {
        let current_start = Self::minute_start(now_ts);
        let mut closed_bars = Vec::new();
        {
            let mut inner = self.inner.write().await;
            for entry in inner.by_spot.values_mut() {
                let should_close = entry
                    .forming
                    .as_ref()
                    .is_some_and(|b| b.start_ts < current_start);
                if should_close {
                    if let Some(mut bar) = entry.forming.take() {
                        bar.closed = true;
                        closed_bars.push(bar);
                    }
                }
            }
        }

        for closed in closed_bars {
            self.persist_closed(&closed);
            let sender = self.publisher(&closed.spot_market).await;
            let _ = sender.send(closed.to_ws_message("bar_closed"));
        }
    }

    fn persist_closed(&self, bar: &Bar) {
        let Some(persist) = &self.persist else {
            return;
        };
        if let Err(error) = persist.save_closed_bar(&bar.to_closed_row()) {
            tracing::error!(
                spot_market = %bar.spot_market,
                start_ts = bar.start_ts,
                error = %error,
                "failed to persist closed 1m bar"
            );
        }
    }
}

fn aggregate_from_1m(ones: &[Bar], interval: &str) -> Vec<Bar> {
    let bucket = match interval {
        "1m" | "" => return ones.to_vec(),
        "5m" => 300,
        "15m" => 900,
        "1h" => 3600,
        "4h" => 14400,
        "1d" => 86400,
        _ => return ones.to_vec(),
    };
    let mut out: Vec<Bar> = Vec::new();
    for bar in ones {
        let start = bar.start_ts / bucket * bucket;
        match out.last_mut() {
            Some(cur) if cur.start_ts == start => {
                cur.high_raw = cur.high_raw.max(bar.high_raw);
                cur.low_raw = cur.low_raw.min(bar.low_raw);
                cur.close_raw = bar.close_raw;
                cur.volume_raw = cur.volume_raw.saturating_add(bar.volume_raw);
                cur.trade_count = cur.trade_count.saturating_add(bar.trade_count);
            }
            _ => {
                out.push(Bar {
                    spot_market: bar.spot_market.clone(),
                    interval: interval.into(),
                    start_ts: start,
                    open_raw: bar.open_raw,
                    high_raw: bar.high_raw,
                    low_raw: bar.low_raw,
                    close_raw: bar.close_raw,
                    volume_raw: bar.volume_raw,
                    trade_count: bar.trade_count,
                    closed: true,
                });
            }
        }
    }
    out
}
