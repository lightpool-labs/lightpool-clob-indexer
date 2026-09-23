// Copyright (c) LightPool Labs

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::{broadcast, RwLock};

use crate::chain::{format_price_pieces, format_token_amount};
use crate::persist::{ClosedBarRow, SharedPersist, BAR_HISTORY_LIMIT};
use crate::spot_market::normalize_spot_market_key;

pub const INTERVAL_1M: &str = "1m";
pub const BAR_SECONDS_1M: u64 = 60;

/// Hyperliquid-style intervals. Higher TFs are updated from the same trades as 1m in memory.
pub const BAR_INTERVALS: &[&str] = &[
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h", "1d", "3d", "1w", "1M",
];

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
    /// Forming candle per interval (only live buckets stay in memory).
    forming: HashMap<String, Bar>,
    publisher: Option<broadcast::Sender<BarWsMessage>>,
}

#[derive(Default)]
struct BarsInner {
    by_spot: HashMap<String, SpotBars>,
}

pub struct Bars {
    inner: RwLock<BarsInner>,
    persist: Option<SharedPersist>,
}

pub fn interval_secs(interval: &str) -> Option<u64> {
    match interval {
        "1m" => Some(60),
        "3m" => Some(180),
        "5m" => Some(300),
        "15m" => Some(900),
        "30m" => Some(1_800),
        "1h" => Some(3_600),
        "2h" => Some(7_200),
        "4h" => Some(14_400),
        "8h" => Some(28_800),
        "12h" => Some(43_200),
        "1d" => Some(86_400),
        "3d" => Some(259_200),
        "1w" => Some(604_800),
        // Calendar month length varies; use `bucket_start` instead.
        "1M" => None,
        _ => None,
    }
}

pub fn bucket_start(ts: u64, interval: &str) -> Option<u64> {
    if interval == "1M" {
        return calendar_month_start_utc(ts);
    }
    let secs = interval_secs(interval)?;
    Some(ts / secs * secs)
}

/// UTC calendar month: first day 00:00:00 of the month containing `ts`.
fn calendar_month_start_utc(ts: u64) -> Option<u64> {
    let days = (ts / 86_400) as i64;
    let (year, month, _day) = civil_from_days(days);
    let month_start_days = days_from_civil(year, month, 1)?;
    Some((month_start_days as u64).saturating_mul(86_400))
}

/// Howard Hinnant civil calendar algorithms (proleptic Gregorian, Unix epoch day 0 = 1970-01-01).
fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32, d as u32)
}

fn days_from_civil(year: i32, month: u32, day: u32) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let y = if month <= 2 { year - 1 } else { year } as i64;
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let m = month as i64;
    let doy = ((153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5) as u64 + (day as u64) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146_097 + doe as i64 - 719_468)
}

pub fn is_supported_interval(interval: &str) -> bool {
    BAR_INTERVALS.iter().any(|&item| item == interval)
}

impl Bars {
    pub fn new(persist: Option<SharedPersist>) -> Self {
        Self {
            inner: RwLock::new(BarsInner::default()),
            persist,
        }
    }

    pub async fn clear(&self) {
        *self.inner.write().await = BarsInner::default();
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
            forming: HashMap::new(),
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
        self.forming_interval(spot_market, INTERVAL_1M).await
    }

    pub async fn forming_interval(&self, spot_market: &str, interval: &str) -> Option<Bar> {
        let key = normalize_spot_market_key(spot_market);
        self.inner
            .read()
            .await
            .by_spot
            .get(&key)
            .and_then(|s| s.forming.get(interval).cloned())
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
        let interval = if interval.is_empty() {
            INTERVAL_1M
        } else {
            interval
        };
        if !is_supported_interval(interval) {
            return Vec::new();
        }
        let Some(persist) = &self.persist else {
            return Vec::new();
        };
        let limit = limit.max(1).min(BAR_HISTORY_LIMIT);
        let rows = match persist.load_closed_bars(&key, interval, from_ts, to_ts, limit) {
            Ok(rows) => rows,
            Err(error) => {
                tracing::error!(error = %error, "load closed bars failed");
                return Vec::new();
            }
        };
        rows.into_iter()
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
            .collect()
    }

    /// Apply a buy-side fill into forming bars for every interval (from the same 1m trade stream).
    pub async fn on_trade(&self, spot_market: &str, price_raw: u64, volume_raw: u64, ts: u64) {
        if volume_raw == 0 {
            return;
        }
        let key = normalize_spot_market_key(spot_market);
        let mut closed_to_persist: Vec<Bar> = Vec::new();
        let mut ws_msgs: Vec<BarWsMessage> = Vec::new();

        {
            let mut inner = self.inner.write().await;
            let entry = inner.by_spot.entry(key.clone()).or_insert_with(|| SpotBars {
                forming: HashMap::new(),
                publisher: None,
            });

            for &interval in BAR_INTERVALS {
                let Some(start_ts) = bucket_start(ts, interval) else {
                    continue;
                };
                if let Some(forming) = entry.forming.get(interval) {
                    if forming.start_ts != start_ts {
                        let mut closed = forming.clone();
                        closed.closed = true;
                        if interval == INTERVAL_1M {
                            ws_msgs.push(closed.to_ws_message("bar_closed"));
                        }
                        closed_to_persist.push(closed);
                        entry.forming.remove(interval);
                    }
                }

                match entry.forming.get_mut(interval) {
                    Some(bar) => {
                        bar.high_raw = bar.high_raw.max(price_raw);
                        bar.low_raw = bar.low_raw.min(price_raw);
                        bar.close_raw = price_raw;
                        bar.volume_raw = bar.volume_raw.saturating_add(volume_raw);
                        bar.trade_count = bar.trade_count.saturating_add(1);
                    }
                    None => {
                        entry.forming.insert(
                            interval.into(),
                            Bar {
                                spot_market: key.clone(),
                                interval: interval.into(),
                                start_ts,
                                open_raw: price_raw,
                                high_raw: price_raw,
                                low_raw: price_raw,
                                close_raw: price_raw,
                                volume_raw,
                                trade_count: 1,
                                closed: false,
                            },
                        );
                    }
                }

                if interval == INTERVAL_1M {
                    if let Some(bar) = entry.forming.get(interval) {
                        ws_msgs.push(bar.to_ws_message("bar"));
                    }
                }
            }
        }

        for closed in &closed_to_persist {
            self.persist_closed(closed);
        }

        if !ws_msgs.is_empty() {
            let sender = self.publisher(&key).await;
            for msg in ws_msgs {
                let _ = sender.send(msg);
            }
        }
    }

    /// Close forming bars whose bucket has ended (no trade needed).
    pub async fn close_expired(&self, now_ts: u64) {
        let mut closed_bars = Vec::new();
        {
            let mut inner = self.inner.write().await;
            for entry in inner.by_spot.values_mut() {
                let intervals: Vec<String> = entry.forming.keys().cloned().collect();
                for interval in intervals {
                    let Some(current_start) = bucket_start(now_ts, &interval) else {
                        continue;
                    };
                    let should_close = entry
                        .forming
                        .get(&interval)
                        .is_some_and(|b| b.start_ts < current_start);
                    if should_close {
                        if let Some(mut bar) = entry.forming.remove(&interval) {
                            bar.closed = true;
                            closed_bars.push(bar);
                        }
                    }
                }
            }
        }

        for closed in closed_bars {
            self.persist_closed(&closed);
            if closed.interval == INTERVAL_1M {
                let sender = self.publisher(&closed.spot_market).await;
                let _ = sender.send(closed.to_ws_message("bar_closed"));
            }
        }
    }

    fn persist_closed(&self, bar: &Bar) {
        let Some(persist) = &self.persist else {
            return;
        };
        persist.enqueue_closed_bar(bar.to_closed_row());
    }
}
