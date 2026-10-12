// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct OracleMarkWsMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub spot_market: String,
    pub round: u64,
    pub oracle_price: String,
    pub mark_price: String,
    pub oracle_price_raw: u64,
    pub mark_price_raw: u64,
    pub block_num: u64,
    pub ts_ms: u64,
}
