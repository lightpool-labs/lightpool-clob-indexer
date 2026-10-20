// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::Deserialize;

pub const CHANNEL_ORDERBOOK_DELTA: &str = "orderbook_delta";
pub const CHANNEL_QUOTE: &str = "quote";
pub const CHANNEL_USER: &str = "user";
pub const CHANNEL_BARS: &str = "bars";
pub const CHANNEL_ORACLE: &str = "oracle";
pub const CHANNEL_PAPER_USER: &str = "paper.user";
pub const CHANNEL_PAPER_USER_STATE: &str = "paper.user.state";
pub const CHANNEL_PAPER_POSITIONS: &str = "paper.positions";
pub const CHANNEL_PAPER_HOUSE: &str = "paper.house";
pub const CHANNEL_PAPER_ACTIVITY: &str = "paper.activity";
pub const CHANNEL_PAPER_STAKE: &str = "paper.stake";

#[derive(Debug, Deserialize)]
pub struct WsRequest {
    pub op: String,
    pub channel: Option<String>,
    pub spot_market: Option<String>,
    pub user_address: Option<String>,
    pub market: Option<String>,
    pub depth: Option<u32>,
    pub interval: Option<String>,
}

pub fn ws_error(message: impl Into<String>) -> String {
    serde_json::json!({
        "type": "error",
        "error": message.into(),
    })
    .to_string()
}

pub fn ws_subscribed(channel: &str, key: &str) -> String {
    serde_json::json!({
        "type": "subscribed",
        "channel": channel,
        "key": key,
    })
    .to_string()
}

pub fn ws_unsubscribed(channel: &str, key: &str) -> String {
    serde_json::json!({
        "type": "unsubscribed",
        "channel": channel,
        "key": key,
    })
    .to_string()
}
