// Copyright (c) LightPool Labs
// Author: xiaoyu1998


//! RocksDB tables for live index state (markets / orders / books / vaults), via typed_store.
//! Blocks, order_history, and bars remain in sqlite.

use std::path::Path;

use serde::{Deserialize, Serialize};
use typed_store::rocks::{default_db_options, DBMap, DBOptions, MetricConf};
use typed_store::traits::{Map, TableSummary, TypedStoreDebug};
use typed_store::DBMapUtils;

use crate::domain::{Market, Order, Vault};
use crate::error::{AppError, AppResult};
use crate::spot_market::normalize_spot_market_key;

use super::index_write_set::IndexWriteSet;
use super::types::{
    PersistBookLevel, PersistBookMeta, PersistMeta, PersistOrderRow, PersistVaultPortfolioRow,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRow {
    pub id: String,
    pub user_address: String,
    pub chain_order_id: String,
    pub spot_market: String,
    pub size_raw: i64,
    pub filled_raw: i64,
    pub status: String,
    pub order: Order,
}

fn write_throughput_config() -> DBOptions {
    default_db_options()
        .optimize_for_write_throughput()
        .optimize_for_read(256)
}

fn meta_table_config() -> DBOptions {
    let mut opt = default_db_options();
    opt.options.set_write_buffer_size(8 * 1024 * 1024);
    opt.options.set_max_write_buffer_number(2);
    opt
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BookMetaRow {
    pub sequence: i64,
    pub last_trade_price: Option<i64>,
}

/// Column families for indexer live state.
#[derive(DBMapUtils)]
pub struct IndexStateTables {
    #[default_options_override_fn = "meta_table_config"]
    pub meta: DBMap<String, String>,

    #[default_options_override_fn = "write_throughput_config"]
    pub markets: DBMap<String, Market>,

    #[default_options_override_fn = "write_throughput_config"]
    pub orders: DBMap<String, OrderRow>,

    /// (spot_market, side, price_raw) → size_raw
    #[default_options_override_fn = "write_throughput_config"]
    pub book_levels: DBMap<(String, String, i64), i64>,

    #[default_options_override_fn = "write_throughput_config"]
    pub book_meta: DBMap<String, BookMetaRow>,

    #[default_options_override_fn = "write_throughput_config"]
    pub last_trades: DBMap<String, i64>,

    #[default_options_override_fn = "write_throughput_config"]
    pub vaults: DBMap<String, Vault>,

    /// (vault_id, spot_market) → amount_raw
    #[default_options_override_fn = "write_throughput_config"]
    pub vault_portfolio: DBMap<(String, String), i64>,
}

impl IndexStateTables {
    pub fn open(path: impl AsRef<Path>) -> AppResult<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    AppError::Internal(format!("create index-state dir {}: {e}", parent.display()))
                })?;
            }
        }
        Ok(Self::open_tables_read_write(
            path.to_path_buf(),
            MetricConf::new("clob-index-state"),
            None,
            None,
        ))
    }

    pub fn map_err(e: impl std::fmt::Display) -> AppError {
        AppError::Internal(format!("index-state rocksdb: {e}"))
    }
}

/// Mutex-friendly wrapper used by index writer / recover / epoch checkpoint.
pub struct IndexStateStore {
    pub(crate) tables: IndexStateTables,
}

impl IndexStateStore {
    pub fn open(path: impl AsRef<Path>) -> AppResult<Self> {
        Ok(Self {
            tables: IndexStateTables::open(path)?,
        })
    }

    pub fn read_meta(&self) -> AppResult<Option<PersistMeta>> {
        let block_num = self
            .tables
            .meta
            .get(&"last_block_num".to_string())
            .map_err(IndexStateTables::map_err)?;
        let digest = self
            .tables
            .meta
            .get(&"last_digest".to_string())
            .map_err(IndexStateTables::map_err)?;
        match (block_num, digest) {
            (Some(b), Some(d)) => {
                let block_num = b
                    .parse::<u64>()
                    .map_err(|e| AppError::Internal(format!("bad last_block_num: {e}")))?;
                Ok(Some(PersistMeta {
                    block_num,
                    digest: d,
                }))
            }
            _ => Ok(None),
        }
    }

    pub fn write_meta(&self, block_num: u64, digest: &str) -> AppResult<()> {
        self.tables
            .meta
            .insert(&"last_block_num".to_string(), &block_num.to_string())
            .map_err(IndexStateTables::map_err)?;
        self.tables
            .meta
            .insert(&"last_digest".to_string(), &digest.to_string())
            .map_err(IndexStateTables::map_err)?;
        Ok(())
    }

    /// Build one RocksDB [`DBBatch`] from [`IndexWriteSet`] (does not commit).
    pub fn build_index_write_batch(
        &self,
        ws: &IndexWriteSet,
    ) -> AppResult<typed_store::rocks::DBBatch> {
        let mut batch = self.tables.markets.batch();

        if ws.full_replace {
            clear_map(&mut batch, &self.tables.markets)?;
            clear_map(&mut batch, &self.tables.orders)?;
            clear_map(&mut batch, &self.tables.book_levels)?;
            clear_map(&mut batch, &self.tables.book_meta)?;
            clear_map(&mut batch, &self.tables.last_trades)?;
            clear_map(&mut batch, &self.tables.vaults)?;
            clear_map(&mut batch, &self.tables.vault_portfolio)?;
        }

        if !ws.markets_to_delete.is_empty() {
            batch
                .delete_batch(&self.tables.markets, ws.markets_to_delete.iter())
                .map_err(IndexStateTables::map_err)?;
        }
        if !ws.orders_to_delete.is_empty() {
            batch
                .delete_batch(&self.tables.orders, ws.orders_to_delete.iter())
                .map_err(IndexStateTables::map_err)?;
        }
        if !ws.book_levels_to_delete.is_empty() {
            batch
                .delete_batch(&self.tables.book_levels, ws.book_levels_to_delete.iter())
                .map_err(IndexStateTables::map_err)?;
        }
        if !ws.book_meta_to_delete.is_empty() {
            batch
                .delete_batch(&self.tables.book_meta, ws.book_meta_to_delete.iter())
                .map_err(IndexStateTables::map_err)?;
        }
        if !ws.last_trades_to_delete.is_empty() {
            batch
                .delete_batch(&self.tables.last_trades, ws.last_trades_to_delete.iter())
                .map_err(IndexStateTables::map_err)?;
        }
        if !ws.vaults_to_delete.is_empty() {
            batch
                .delete_batch(&self.tables.vaults, ws.vaults_to_delete.iter())
                .map_err(IndexStateTables::map_err)?;
        }
        if !ws.vault_portfolio_to_delete.is_empty() {
            batch
                .delete_batch(
                    &self.tables.vault_portfolio,
                    ws.vault_portfolio_to_delete.iter(),
                )
                .map_err(IndexStateTables::map_err)?;
        }

        if !ws.markets_to_store.is_empty() {
            batch
                .insert_batch(&self.tables.markets, ws.markets_to_store.iter())
                .map_err(IndexStateTables::map_err)?;
        }
        if !ws.orders_to_store.is_empty() {
            batch
                .insert_batch(&self.tables.orders, ws.orders_to_store.iter())
                .map_err(IndexStateTables::map_err)?;
        }
        if !ws.book_levels_to_store.is_empty() {
            batch
                .insert_batch(&self.tables.book_levels, ws.book_levels_to_store.iter())
                .map_err(IndexStateTables::map_err)?;
        }
        if !ws.book_meta_to_store.is_empty() {
            batch
                .insert_batch(&self.tables.book_meta, ws.book_meta_to_store.iter())
                .map_err(IndexStateTables::map_err)?;
        }
        if !ws.last_trades_to_store.is_empty() {
            batch
                .insert_batch(&self.tables.last_trades, ws.last_trades_to_store.iter())
                .map_err(IndexStateTables::map_err)?;
        }
        if !ws.vaults_to_store.is_empty() {
            batch
                .insert_batch(&self.tables.vaults, ws.vaults_to_store.iter())
                .map_err(IndexStateTables::map_err)?;
        }
        if !ws.vault_portfolio_to_store.is_empty() {
            batch
                .insert_batch(
                    &self.tables.vault_portfolio,
                    ws.vault_portfolio_to_store.iter(),
                )
                .map_err(IndexStateTables::map_err)?;
        }

        batch
            .insert_batch(
                &self.tables.meta,
                [
                    ("last_block_num".to_string(), ws.block_num.to_string()),
                    ("last_digest".to_string(), ws.digest.clone()),
                ]
                .into_iter(),
            )
            .map_err(IndexStateTables::map_err)?;

        Ok(batch)
    }

    /// Build + commit one [`IndexWriteSet`] (used by checkpoint / sync paths).
    pub fn apply_index_write_set(&self, ws: &IndexWriteSet) -> AppResult<()> {
        self.build_index_write_batch(ws)?
            .write()
            .map_err(IndexStateTables::map_err)
    }

    /// Full wipe+write used by peer `checkpoint_exported`.
    pub fn checkpoint(
        &self,
        block_num: u64,
        digest: &str,
        markets: &[Market],
        orders: &[PersistOrderRow],
        last_trades: &[(String, u64)],
        levels: &[PersistBookLevel],
        metas: &[PersistBookMeta],
        vaults: &[Vault],
        vault_portfolio: &[PersistVaultPortfolioRow],
    ) -> AppResult<()> {
        let mut ws = IndexWriteSet {
            block_num,
            digest: digest.to_string(),
            full_replace: true,
            ..Default::default()
        };
        for market in markets {
            ws.markets_to_store
                .insert(market.id().to_string(), market.clone());
        }
        for row in orders {
            let id = row.order.id.to_string();
            ws.orders_to_store.insert(
                id.clone(),
                OrderRow {
                    id,
                    user_address: row.user_address.clone(),
                    chain_order_id: row.chain_order_id.clone(),
                    spot_market: normalize_spot_market_key(&row.spot_market),
                    size_raw: row.size_raw as i64,
                    filled_raw: row.filled_raw as i64,
                    status: row.order.status.clone(),
                    order: row.order.clone(),
                },
            );
        }
        for level in levels {
            if level.size_raw == 0 {
                continue;
            }
            ws.book_levels_to_store.insert(
                (
                    normalize_spot_market_key(&level.spot_market),
                    level.side.clone(),
                    level.price_raw as i64,
                ),
                level.size_raw as i64,
            );
        }
        for meta in metas {
            ws.book_meta_to_store.insert(
                normalize_spot_market_key(&meta.spot_market),
                BookMetaRow {
                    sequence: meta.sequence as i64,
                    last_trade_price: meta.last_trade_price.map(|v| v as i64),
                },
            );
        }
        for (spot, price) in last_trades {
            ws.last_trades_to_store
                .insert(normalize_spot_market_key(spot), *price as i64);
        }
        for vault in vaults {
            let mut v = vault.clone();
            v.portfolio.clear();
            ws.vaults_to_store.insert(v.id.to_string(), v);
        }
        for row in vault_portfolio {
            if row.amount_raw == 0 {
                continue;
            }
            ws.vault_portfolio_to_store.insert(
                (
                    row.vault_id.clone(),
                    normalize_spot_market_key(&row.spot_market),
                ),
                row.amount_raw as i64,
            );
        }
        self.apply_index_write_set(&ws)
    }

    pub fn checkpoint_db(&self, dest: &Path) -> AppResult<()> {
        if dest.exists() {
            std::fs::remove_dir_all(dest).map_err(|e| {
                AppError::Internal(format!("remove existing rocks ckpt {}: {e}", dest.display()))
            })?;
        }
        if let Some(parent) = dest.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    AppError::Internal(format!("create ckpt dir {}: {e}", parent.display()))
                })?;
            }
        }
        self.tables
            .meta
            .checkpoint_db(dest)
            .map_err(IndexStateTables::map_err)
    }

    pub fn load_markets(&self) -> AppResult<Vec<Market>> {
        let mut out = Vec::new();
        for item in self.tables.markets.safe_iter() {
            let (_id, market) = item.map_err(IndexStateTables::map_err)?;
            out.push(market);
        }
        Ok(out)
    }

    pub fn load_orders(&self) -> AppResult<Vec<PersistOrderRow>> {
        let mut out = Vec::new();
        for item in self.tables.orders.safe_iter() {
            let (_id, row) = item.map_err(IndexStateTables::map_err)?;
            out.push(PersistOrderRow {
                order: row.order,
                user_address: row.user_address,
                chain_order_id: row.chain_order_id,
                spot_market: row.spot_market,
                size_raw: row.size_raw as u64,
                filled_raw: row.filled_raw as u64,
            });
        }
        Ok(out)
    }

    pub fn load_last_trades(&self) -> AppResult<Vec<(String, u64)>> {
        let mut out = Vec::new();
        for item in self.tables.last_trades.safe_iter() {
            let (spot, price) = item.map_err(IndexStateTables::map_err)?;
            out.push((spot, price as u64));
        }
        Ok(out)
    }

    pub fn load_book_levels(&self) -> AppResult<Vec<PersistBookLevel>> {
        let mut out = Vec::new();
        for item in self.tables.book_levels.safe_iter() {
            let ((spot, side, price), size) = item.map_err(IndexStateTables::map_err)?;
            if size <= 0 {
                continue;
            }
            out.push(PersistBookLevel {
                spot_market: spot,
                side,
                price_raw: price as u64,
                size_raw: size as u64,
            });
        }
        Ok(out)
    }

    pub fn load_book_meta(&self) -> AppResult<Vec<PersistBookMeta>> {
        let mut out = Vec::new();
        for item in self.tables.book_meta.safe_iter() {
            let (spot, meta) = item.map_err(IndexStateTables::map_err)?;
            out.push(PersistBookMeta {
                spot_market: spot,
                sequence: meta.sequence as u64,
                last_trade_price: meta.last_trade_price.map(|v| v as u64),
            });
        }
        Ok(out)
    }

    pub fn load_vaults(&self) -> AppResult<Vec<Vault>> {
        let mut out = Vec::new();
        for item in self.tables.vaults.safe_iter() {
            let (_id, vault) = item.map_err(IndexStateTables::map_err)?;
            out.push(vault);
        }
        Ok(out)
    }

    pub fn load_vault_portfolio(&self) -> AppResult<Vec<PersistVaultPortfolioRow>> {
        let mut out = Vec::new();
        for item in self.tables.vault_portfolio.safe_iter() {
            let ((vault_id, spot), amount) = item.map_err(IndexStateTables::map_err)?;
            if amount <= 0 {
                continue;
            }
            out.push(PersistVaultPortfolioRow {
                vault_id,
                spot_market: normalize_spot_market_key(&spot),
                amount_raw: amount as u64,
            });
        }
        Ok(out)
    }
}

fn clear_map<K, V>(
    batch: &mut typed_store::rocks::DBBatch,
    map: &DBMap<K, V>,
) -> AppResult<()>
where
    K: Serialize + for<'de> Deserialize<'de> + std::fmt::Debug,
    V: Serialize + for<'de> Deserialize<'de> + std::fmt::Debug,
{
    let keys: Vec<K> = map
        .safe_iter()
        .map(|item| item.map(|(k, _)| k).map_err(IndexStateTables::map_err))
        .collect::<AppResult<Vec<_>>>()?;
    if !keys.is_empty() {
        batch
            .delete_batch(map, keys.iter())
            .map_err(IndexStateTables::map_err)?;
    }
    Ok(())
}
