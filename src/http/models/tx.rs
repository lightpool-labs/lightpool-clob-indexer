// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use serde::Deserialize;

use lightpool_sdk::lightpool_types::TransactionReceipt;

#[derive(Debug, Deserialize)]
pub struct SubmitTxRequest {
    10|    pub tx: lightpool_sdk::lightpool_types::SignedTransaction,
}

#[derive(Debug, serde::Serialize)]
pub struct SubmitTxResponse {
    pub digest: String,
    pub block_num: u64,
    pub receipt: TransactionReceipt,
}

#[derive(Debug, serde::Serialize)]
pub struct InjectTxResponse {
    pub digest: String,
}
