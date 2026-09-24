// Copyright (c) LightPool Labs
// Author: xiaoyu1998


use lightpool_sdk::ReceiptBlock;

use crate::domain::{Market, Vault};
use crate::error::{AppError, AppResult};
use crate::peer::encode_block_payload;
use crate::spot_market::normalize_spot_market_key;

use super::types::{
    ClosedBarRow, PersistBookLevel, PersistBookMeta, PersistOrderRow, PersistVaultPortfolioRow,
};

/// Ingress job: blocks are cloned `ReceiptBlock`s; encode workers turn these into
/// [`EncodedPersistOp`] before sqlite write.
#[derive(Debug)]
pub enum PersistOp {
    SaveReceiptBlock(ReceiptBlock),
    UpsertOrderHistory(PersistOrderRow),
    SaveClosedBar(ClosedBarRow),
    Checkpoint {
        block_num: u64,
        digest: String,
        markets: Vec<Market>,
        orders: Vec<PersistOrderRow>,
        last_trades: Vec<(String, u64)>,
        levels: Vec<PersistBookLevel>,
        metas: Vec<PersistBookMeta>,
        vaults: Vec<Vault>,
        vault_portfolio: Vec<PersistVaultPortfolioRow>,
    },
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

impl EncodedPersistOp {
    pub(crate) fn is_block(&self) -> bool {
        matches!(self, Self::SaveBlockBytes { .. })
    }
}

#[derive(Debug, serde::Serialize)]
pub struct EncodedCheckpoint {
    pub block_num: u64,
    pub digest: String,
    pub markets: Vec<(String, String)>,
    pub orders: Vec<EncodedOrderInsert>,
    pub last_trades: Vec<(String, i64)>,
    pub levels: Vec<(String, String, i64, i64)>,
    pub metas: Vec<(String, i64, Option<i64>)>,
    pub vaults: Vec<(String, String)>,
    pub vault_portfolio: Vec<(String, String, i64)>,
}

#[derive(Debug, serde::Serialize)]
pub struct EncodedOrderInsert {
    pub id: String,
    pub user_address: String,
    pub chain_order_id: String,
    pub spot_market: String,
    pub size_raw: i64,
    pub filled_raw: i64,
    pub status: String,
    pub payload: String,
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
        PersistOp::Checkpoint { .. } => Err(AppError::Internal(
            "checkpoint must use dedicated checkpoint pipeline".into(),
        )),
    }
}

pub(crate) fn encode_checkpoint_op(op: PersistOp) -> AppResult<EncodedCheckpoint> {
    match op {
        PersistOp::Checkpoint {
            block_num,
            digest,
            markets,
            orders,
            last_trades,
            levels,
            metas,
            vaults,
            vault_portfolio,
        } => encode_checkpoint(
            block_num,
            digest,
            markets,
            orders,
            last_trades,
            levels,
            metas,
            vaults,
            vault_portfolio,
        ),
        _ => Err(AppError::Internal("expected checkpoint PersistOp".into())),
    }
}

pub(crate) fn encode_checkpoint(
    block_num: u64,
    digest: String,
    markets: Vec<Market>,
    orders: Vec<PersistOrderRow>,
    last_trades: Vec<(String, u64)>,
    levels: Vec<PersistBookLevel>,
    metas: Vec<PersistBookMeta>,
    vaults: Vec<Vault>,
    vault_portfolio: Vec<PersistVaultPortfolioRow>,
) -> AppResult<EncodedCheckpoint> {
    let mut market_rows = Vec::with_capacity(markets.len());
    for market in markets {
        let payload = serde_json::to_string(&market)
            .map_err(|e| AppError::Internal(format!("serialize market: {e}")))?;
        market_rows.push((market.id().to_string(), payload));
    }

    let mut order_rows = Vec::with_capacity(orders.len());
    for row in orders {
        let payload = serde_json::to_string(&row.order)
            .map_err(|e| AppError::Internal(format!("serialize order: {e}")))?;
        order_rows.push(EncodedOrderInsert {
            id: row.order.id.to_string(),
            user_address: row.user_address,
            chain_order_id: row.chain_order_id,
            spot_market: normalize_spot_market_key(&row.spot_market),
            size_raw: row.size_raw as i64,
            filled_raw: row.filled_raw as i64,
            status: row.order.status,
            payload,
        });
    }

    let last_trades = last_trades
        .into_iter()
        .map(|(spot, price)| (normalize_spot_market_key(&spot), price as i64))
        .collect();

    let levels = levels
        .into_iter()
        .map(|level| {
            (
                normalize_spot_market_key(&level.spot_market),
                level.side,
                level.price_raw as i64,
                level.size_raw as i64,
            )
        })
        .collect();

    let metas = metas
        .into_iter()
        .map(|meta| {
            (
                normalize_spot_market_key(&meta.spot_market),
                meta.sequence as i64,
                meta.last_trade_price.map(|v| v as i64),
            )
        })
        .collect();

    let mut vault_rows = Vec::with_capacity(vaults.len());
    for vault in vaults {
        let payload = serde_json::to_string(&vault)
            .map_err(|e| AppError::Internal(format!("serialize vault: {e}")))?;
        vault_rows.push((vault.id.to_string(), payload));
    }

    let vault_portfolio = vault_portfolio
        .into_iter()
        .map(|row| {
            (
                row.vault_id,
                normalize_spot_market_key(&row.spot_market),
                row.amount_raw as i64,
            )
        })
        .collect();

    Ok(EncodedCheckpoint {
        block_num,
        digest,
        markets: market_rows,
        orders: order_rows,
        last_trades,
        levels,
        metas,
        vaults: vault_rows,
        vault_portfolio,
    })
}
