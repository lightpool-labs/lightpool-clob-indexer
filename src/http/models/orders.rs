// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::Serialize;

use crate::domain::Order;

#[derive(Debug, Serialize)]
pub struct CancelContextResponse {
    pub order: Order,
    pub chain_order_id: String,
    pub spot_market: String,
}

#[derive(Debug, Serialize)]
pub struct OrderQueryResponse {
    pub order: Order,
    pub chain_order_id: String,
    pub spot_market: String,
    pub user_address: String,
    pub size_raw: u64,
    pub filled_raw: u64,
}
