// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::Serialize;
use uuid::Uuid;

use crate::domain::Order;

#[derive(Debug, Clone, Serialize)]
pub struct UserOrderMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub event: String,
    pub user_address: String,
    pub chain_order_id: String,
    pub block_num: u64,
    pub spot_market: String,
    #[serde(flatten)]
    pub order: Order,
}

#[derive(Debug, Clone, Serialize)]
pub struct UserTradeMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub user_address: String,
    pub chain_order_id: String,
    pub order_id: Uuid,
    pub market_slug: String,
    pub outcome: String,
    pub side: String,
    pub price: String,
    pub fill_amount: String,
    pub remaining_amount: String,
    pub is_fully_filled: bool,
    pub spot_market: String,
    pub block_num: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cloid: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum UserWsMessage {
    Order(UserOrderMessage),
    Trade(UserTradeMessage),
}
