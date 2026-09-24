// Copyright (c) LightPool Labs
// Author: xiaoyu1998


//! Per-block index-state deltas for RocksDB (like node `BlockWriteSet`).

use std::collections::{HashMap, HashSet};
use crate::domain::{Market, Order, Vault};
use crate::spot_market::normalize_spot_market_key;

use super::index_state_tables::{BookMetaRow, OrderRow};

/// One applied receipt block → index-state RocksDB put/delete sets.
/// Collected directly during `process_prepared_block` (no separate dirty tracker).
#[derive(Debug, Clone, Default)]
pub struct IndexWriteSet {
    pub block_num: u64,
    pub digest: String,
    /// When true, wipe all index CFs before applying stores (peer / force sync).
    pub full_replace: bool,

    pub markets_to_store: HashMap<String, Market>,
    pub markets_to_delete: HashSet<String>,

    pub orders_to_store: HashMap<String, OrderRow>,
    pub orders_to_delete: HashSet<String>,

    pub book_levels_to_store: HashMap<(String, String, i64), i64>,
    pub book_levels_to_delete: HashSet<(String, String, i64)>,

    pub book_meta_to_store: HashMap<String, BookMetaRow>,
    pub book_meta_to_delete: HashSet<String>,

    pub last_trades_to_store: HashMap<String, i64>,
    pub last_trades_to_delete: HashSet<String>,

    pub vaults_to_store: HashMap<String, Vault>,
    pub vaults_to_delete: HashSet<String>,

    pub vault_portfolio_to_store: HashMap<(String, String), i64>,
    pub vault_portfolio_to_delete: HashSet<(String, String)>,
}

impl IndexWriteSet {
    pub fn merge(&mut self, other: IndexWriteSet) {
        for (k, v) in other.markets_to_store {
            self.markets_to_delete.remove(&k);
            self.markets_to_store.insert(k, v);
        }
        for k in other.markets_to_delete {
            self.markets_to_store.remove(&k);
            self.markets_to_delete.insert(k);
        }
        for (k, v) in other.orders_to_store {
            self.orders_to_delete.remove(&k);
            self.orders_to_store.insert(k, v);
        }
        for k in other.orders_to_delete {
            self.orders_to_store.remove(&k);
            self.orders_to_delete.insert(k);
        }
        for (k, v) in other.book_levels_to_store {
            self.book_levels_to_delete.remove(&k);
            self.book_levels_to_store.insert(k, v);
        }
        for k in other.book_levels_to_delete {
            self.book_levels_to_store.remove(&k);
            self.book_levels_to_delete.insert(k);
        }
        for (k, v) in other.book_meta_to_store {
            self.book_meta_to_delete.remove(&k);
            self.book_meta_to_store.insert(k, v);
        }
        for k in other.book_meta_to_delete {
            self.book_meta_to_store.remove(&k);
            self.book_meta_to_delete.insert(k);
        }
        for (k, v) in other.last_trades_to_store {
            self.last_trades_to_delete.remove(&k);
            self.last_trades_to_store.insert(k, v);
        }
        for k in other.last_trades_to_delete {
            self.last_trades_to_store.remove(&k);
            self.last_trades_to_delete.insert(k);
        }
        for (k, v) in other.vaults_to_store {
            self.vaults_to_delete.remove(&k);
            self.vaults_to_store.insert(k, v);
        }
        for k in other.vaults_to_delete {
            self.vaults_to_store.remove(&k);
            self.vaults_to_delete.insert(k);
        }
        for (k, v) in other.vault_portfolio_to_store {
            self.vault_portfolio_to_delete.remove(&k);
            self.vault_portfolio_to_store.insert(k, v);
        }
        for k in other.vault_portfolio_to_delete {
            self.vault_portfolio_to_store.remove(&k);
            self.vault_portfolio_to_delete.insert(k);
        }
        if other.full_replace {
            self.full_replace = true;
        }
    }

    pub fn is_empty(&self) -> bool {
        !self.full_replace
            && self.markets_to_store.is_empty()
            && self.markets_to_delete.is_empty()
            && self.orders_to_store.is_empty()
            && self.orders_to_delete.is_empty()
            && self.book_levels_to_store.is_empty()
            && self.book_levels_to_delete.is_empty()
            && self.book_meta_to_store.is_empty()
            && self.book_meta_to_delete.is_empty()
            && self.last_trades_to_store.is_empty()
            && self.last_trades_to_delete.is_empty()
            && self.vaults_to_store.is_empty()
            && self.vaults_to_delete.is_empty()
            && self.vault_portfolio_to_store.is_empty()
            && self.vault_portfolio_to_delete.is_empty()
    }

    pub fn store_market(&mut self, market: Market) {
        let id = market.id().to_string();
        self.markets_to_delete.remove(&id);
        self.markets_to_store.insert(id, market);
    }

    pub fn delete_market(&mut self, market_id: &str) {
        self.markets_to_store.remove(market_id);
        self.markets_to_delete.insert(market_id.to_string());
    }

    pub fn store_order(&mut self, row: OrderRow) {
        let id = row.id.clone();
        self.orders_to_delete.remove(&id);
        self.orders_to_store.insert(id, row);
    }

    pub fn delete_order(&mut self, order_id: &str) {
        self.orders_to_store.remove(order_id);
        self.orders_to_delete.insert(order_id.to_string());
    }

    pub fn store_book_level(&mut self, spot: &str, side: &str, price_raw: i64, size_raw: i64) {
        let spot = normalize_spot_market_key(spot);
        let key = (spot, side.to_string(), price_raw);
        if size_raw <= 0 {
            self.book_levels_to_store.remove(&key);
            self.book_levels_to_delete.insert(key);
        } else {
            self.book_levels_to_delete.remove(&key);
            self.book_levels_to_store.insert(key, size_raw);
        }
    }

    pub fn delete_book_level(&mut self, spot: &str, side: &str, price_raw: i64) {
        let spot = normalize_spot_market_key(spot);
        let key = (spot, side.to_string(), price_raw);
        self.book_levels_to_store.remove(&key);
        self.book_levels_to_delete.insert(key);
    }

    pub fn store_book_meta(&mut self, spot: &str, meta: BookMetaRow) {
        let spot = normalize_spot_market_key(spot);
        self.book_meta_to_delete.remove(&spot);
        self.book_meta_to_store.insert(spot, meta);
    }

    pub fn store_last_trade(&mut self, spot: &str, price: i64) {
        let spot = normalize_spot_market_key(spot);
        self.last_trades_to_delete.remove(&spot);
        self.last_trades_to_store.insert(spot, price);
    }

    pub fn store_vault(&mut self, mut vault: Vault) {
        vault.portfolio.clear();
        let id = vault.id.to_string();
        self.vaults_to_delete.remove(&id);
        self.vaults_to_store.insert(id, vault);
    }

    pub fn store_vault_portfolio(&mut self, vault_id: &str, spot: &str, amount: i64) {
        let spot = normalize_spot_market_key(spot);
        let key = (vault_id.to_string(), spot);
        if amount <= 0 {
            self.vault_portfolio_to_store.remove(&key);
            self.vault_portfolio_to_delete.insert(key);
        } else {
            self.vault_portfolio_to_delete.remove(&key);
            self.vault_portfolio_to_store.insert(key, amount);
        }
    }
}

pub fn order_row_from_parts(
    order: Order,
    user_address: String,
    chain_order_id: String,
    spot_market: String,
    size_raw: u64,
    filled_raw: u64,
) -> OrderRow {
    OrderRow {
        id: order.id.to_string(),
        user_address,
        chain_order_id,
        spot_market: normalize_spot_market_key(&spot_market),
        size_raw: size_raw as i64,
        filled_raw: filled_raw as i64,
        status: order.status.clone(),
        order,
    }
}
