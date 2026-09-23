// Copyright (c) LightPool Labs
// Author: xiaoyu1998


use lightpool_sdk::ReceiptBlock;

use crate::domain::{Market, Vault};

use super::types::{
    ClosedBarRow, PersistBookLevel, PersistBookMeta, PersistOrderRow, PersistVaultPortfolioRow,
};

/// Unified persist job: all sqlite writes go through one queue / N workers.
#[derive(Debug)]
pub enum PersistOp {
    SaveReceiptBlock(ReceiptBlock),
    SaveBlockBytes {
        block_num: u64,
        digest: String,
        payload: Vec<u8>,
    },
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
