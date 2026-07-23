// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::Deserialize;

pub const CHANNEL_ORDERBOOK_DELTA: &str = "orderbook_delta";
pub const CHANNEL_QUOTE: &str = "quote";
pub const CHANNEL_USER: &str = "user";

#[derive(Debug, Deserialize)]
pub struct WsRequest {
    pub op: String,
    pub channel: Option<String>,
    pub spot_market: Option<String>,
    pub user_address: Option<String>,
    pub depth: Option<u32>,
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
