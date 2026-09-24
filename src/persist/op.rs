// Copyright (c) LightPool Labs
// Author: xiaoyu1998


use lightpool_sdk::ReceiptBlock;

use crate::error::{AppError, AppResult};
use crate::peer::encode_block_payload;
use crate::spot_market::normalize_spot_market_key;

use super::index_write_set::IndexWriteSet;
use super::types::{ClosedBarRow, PersistOrderRow};

/// Ingress job: blocks are cloned `ReceiptBlock`s; encode workers turn these into
/// [`EncodedPersistOp`] before sqlite write. IndexWrite bypasses encode and goes
/// straight to the RocksDB WriteBatch writer.
#[derive(Debug)]
pub enum PersistOp {
    SaveReceiptBlock(ReceiptBlock),
    UpsertOrderHistory(PersistOrderRow),
    SaveClosedBar(ClosedBarRow),
    /// One block of index-state RocksDB deltas.
    IndexWrite(IndexWriteSet),
}

/// Ready for sqlite: all CPU serialize work done in encode workers.
#[derive(Debug)]
pub enum EncodedPersistOp {
    SaveBlockBytes {
        block_num: u64,
        digest: String,
        payload: Vec<u8>,
    },
    UpsertOrderHistory {
        order_id: String,
        user_address: String,
        chain_order_id: String,
        spot_market: String,
        size_raw: i64,
        filled_raw: i64,
        status: String,
        payload: String,
    },
    SaveClosedBar(ClosedBarRow),
}

pub(crate) fn encode_persist_op(op: PersistOp) -> AppResult<EncodedPersistOp> {
    match op {
        PersistOp::SaveReceiptBlock(block) => {
            let block_num = block.block_num;
            let digest = hex::encode(block.digest.as_bytes());
            let payload = encode_block_payload(&block)?;
            Ok(EncodedPersistOp::SaveBlockBytes {
                block_num,
                digest,
                payload,
            })
        }
        PersistOp::UpsertOrderHistory(row) => {
            let payload = serde_json::to_string(&row.order)
                .map_err(|e| AppError::Internal(format!("serialize order history: {e}")))?;
            Ok(EncodedPersistOp::UpsertOrderHistory {
                order_id: row.order.id.to_string(),
                user_address: row.user_address.trim().to_ascii_lowercase(),
                chain_order_id: row.chain_order_id,
                spot_market: normalize_spot_market_key(&row.spot_market),
                size_raw: row.size_raw as i64,
                filled_raw: row.filled_raw as i64,
                status: row.order.status,
                payload,
            })
        }
        PersistOp::SaveClosedBar(bar) => Ok(EncodedPersistOp::SaveClosedBar(bar)),
        PersistOp::IndexWrite(_) => Err(AppError::Internal(
            "index write must use dedicated index-write pipeline".into(),
        )),
    }
}
