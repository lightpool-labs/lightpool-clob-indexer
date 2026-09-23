// Copyright (c) LightPool Labs
// Author: xiaoyu1998


use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use lightpool_sdk::ReceiptBlock;
use rusqlite::Connection;
use tokio::sync::mpsc;

use crate::domain::{Market, Vault};
use crate::error::{AppError, AppResult};
use crate::indexer::SharedIndexState;

use super::op::PersistOp;
use super::store::PersistStore;
use super::types::{
    CheckpointSnapshot, ClosedBarRow, PersistBookLevel, PersistBookMeta, PersistMeta,
    PersistOrderRow, PersistVaultPortfolioRow, BAR_HISTORY_LIMIT,
};
use super::workers::PersistWorkers;

#[derive(Clone)]
pub struct SharedPersist {
    inner: Arc<Mutex<PersistStore>>,
    op_tx: mpsc::UnboundedSender<PersistOp>,
    pending: Arc<AtomicU64>,
}

impl SharedPersist {
    /// Open sqlite and return deferred persist workers (not started yet).
    pub fn open(
        path: impl AsRef<Path>,
        worker_count: usize,
    ) -> AppResult<(Self, PersistWorkers)> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    AppError::Internal(format!("create sqlite dir {}: {e}", parent.display()))
                })?;
            }
        }

        let conn = Connection::open(path)
            .map_err(|e| AppError::Internal(format!("open sqlite {}: {e}", path.display())))?;
        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA foreign_keys = ON;
            ",
        )
        .map_err(|e| AppError::Internal(format!("sqlite pragma: {e}")))?;

        let store = PersistStore { conn };
        store.migrate()?;
        let inner = Arc::new(Mutex::new(store));
        let pending = Arc::new(AtomicU64::new(0));
        let (op_tx, op_rx) = mpsc::unbounded_channel::<PersistOp>();
        let worker_count = worker_count.max(1);
        tracing::info!(
            path = %path.display(),
            workers = worker_count,
            "sqlite persist opened"
        );
        Ok((
            Self {
                inner: inner.clone(),
                op_tx,
                pending: pending.clone(),
            },
            PersistWorkers {
                inner,
                pending,
                rx: op_rx,
                worker_count,
            },
        ))
    }

    fn submit(&self, op: PersistOp) {
        self.pending.fetch_add(1, Ordering::Relaxed);
        if self.op_tx.send(op).is_err() {
            self.pending.fetch_sub(1, Ordering::Relaxed);
            tracing::error!("persist workers stopped; dropping persist op");
        }
    }

    /// Queue a live block for background bincode serialize + sqlite write.
    pub fn enqueue_receipt_block(&self, block: ReceiptBlock) {
        self.submit(PersistOp::SaveReceiptBlock(block));
    }

    pub fn enqueue_order_history(&self, row: PersistOrderRow) {
        self.submit(PersistOp::UpsertOrderHistory(row));
    }

    pub fn enqueue_closed_bar(&self, bar: ClosedBarRow) {
        self.submit(PersistOp::SaveClosedBar(bar));
    }

    pub fn enqueue_checkpoint(
        &self,
        block_num: u64,
        digest: String,
        markets: Vec<Market>,
        orders: Vec<PersistOrderRow>,
        last_trades: Vec<(String, u64)>,
        levels: Vec<PersistBookLevel>,
        metas: Vec<PersistBookMeta>,
        vaults: Vec<Vault>,
        vault_portfolio: Vec<PersistVaultPortfolioRow>,
    ) {
        self.submit(PersistOp::Checkpoint {
            block_num,
            digest,
            markets,
            orders,
            last_trades,
            levels,
            metas,
            vaults,
            vault_portfolio,
        });
    }

    pub fn persist_pending(&self) -> u64 {
        self.pending.load(Ordering::Relaxed)
    }

    /// Wait until the async persist queue drains (or timeout).
    pub async fn wait_idle(&self, timeout: Duration) -> bool {
        let start = Instant::now();
        while self.pending.load(Ordering::Relaxed) > 0 {
            if start.elapsed() >= timeout {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        true
    }

    pub fn meta(&self) -> AppResult<Option<PersistMeta>> {
        self.with(|store| store.read_meta())
    }

    pub fn export_checkpoint_snapshot(&self) -> AppResult<Option<CheckpointSnapshot>> {
        self.with(|store| {
            let Some(meta) = store.read_meta()? else {
                return Ok(None);
            };
            Ok(Some(CheckpointSnapshot {
                block_num: meta.block_num,
                digest: meta.digest,
                markets: store.load_markets()?,
                orders: store.load_orders()?,
                last_trades: store.load_last_trades()?,
                book_levels: store.load_book_levels()?,
                book_metas: store.load_book_meta()?,
                vaults: store.load_vaults()?,
                vault_portfolio: store.load_vault_portfolio()?,
            }))
        })
    }

    pub fn save_block(&self, block_num: u64, digest: &str, payload: &[u8]) -> AppResult<()> {
        self.with(|store| store.save_block(block_num, digest, payload))
    }

    pub fn load_blocks_after(
        &self,
        after_block_num: Option<u64>,
    ) -> AppResult<Vec<(u64, String, Vec<u8>)>> {
        self.with(|store| store.load_blocks_after(after_block_num, None))
    }

    pub fn load_blocks_after_limited(
        &self,
        after_block_num: Option<u64>,
        limit: usize,
    ) -> AppResult<Vec<(u64, String, Vec<u8>)>> {
        self.with(|store| store.load_blocks_after(after_block_num, Some(limit)))
    }

    pub fn delete_blocks_after(&self, after_block_num: u64) -> AppResult<usize> {
        self.with(|store| store.delete_blocks_after(after_block_num))
    }

    pub fn save_closed_bar(&self, bar: &ClosedBarRow) -> AppResult<()> {
        self.with(|store| {
            store.save_closed_bar(bar)?;
            store.trim_bars(&bar.spot_market, &bar.interval, BAR_HISTORY_LIMIT)
        })
    }

    pub fn load_closed_bars(
        &self,
        spot_market: &str,
        interval: &str,
        from_ts: Option<u64>,
        to_ts: Option<u64>,
        limit: usize,
    ) -> AppResult<Vec<ClosedBarRow>> {
        self.with(|store| {
            store.load_closed_bars(spot_market, interval, from_ts, to_ts, limit)
        })
    }

    pub fn upsert_order_history(&self, row: &PersistOrderRow) -> AppResult<()> {
        self.with(|store| store.upsert_order_history(row))
    }

    pub fn list_order_history(
        &self,
        user_address: &str,
        limit: usize,
    ) -> AppResult<Vec<PersistOrderRow>> {
        self.with(|store| store.list_order_history(user_address, limit))
    }

    pub fn checkpoint_exported(
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
        self.with(|store| {
            store.checkpoint(
                block_num,
                digest,
                markets,
                orders,
                last_trades,
                levels,
                metas,
                vaults,
                vault_portfolio,
            )
        })
    }

    pub async fn load_into(
        &self,
        index: &SharedIndexState,
    ) -> AppResult<Option<PersistMeta>> {
        let (meta, markets, orders, last_trades, levels, metas, vaults, vault_portfolio) =
            self.with(|store| {
                let meta = store.read_meta()?;
                let markets = store.load_markets()?;
                let orders = store.load_orders()?;
                let last_trades = store.load_last_trades()?;
                let levels = store.load_book_levels()?;
                let metas = store.load_book_meta()?;
                let vaults = store.load_vaults()?;
                let vault_portfolio = store.load_vault_portfolio()?;
                Ok((
                    meta,
                    markets,
                    orders,
                    last_trades,
                    levels,
                    metas,
                    vaults,
                    vault_portfolio,
                ))
            })?;

        apply_snapshot_to_memory(
            index,
            markets,
            orders,
            last_trades,
            levels,
            metas,
            vaults,
            vault_portfolio,
        )
        .await;

        if let Some(ref meta) = meta {
            tracing::info!(
                block_num = meta.block_num,
                digest = %meta.digest,
                "recovered indexer state from sqlite"
            );
        } else {
            tracing::info!("sqlite has no persisted indexer head yet");
        }

        Ok(meta)
    }

    pub async fn apply_checkpoint_snapshot(
        &self,
        snapshot: &CheckpointSnapshot,
        index: &SharedIndexState,
    ) -> AppResult<()> {
        index.clear_all().await;
        apply_snapshot_to_memory(
            index,
            snapshot.markets.clone(),
            snapshot.orders.clone(),
            snapshot.last_trades.clone(),
            snapshot.book_levels.clone(),
            snapshot.book_metas.clone(),
            snapshot.vaults.clone(),
            snapshot.vault_portfolio.clone(),
        )
        .await;

        self.checkpoint_exported(
            snapshot.block_num,
            &snapshot.digest,
            &snapshot.markets,
            &snapshot.orders,
            &snapshot.last_trades,
            &snapshot.book_levels,
            &snapshot.book_metas,
            &snapshot.vaults,
            &snapshot.vault_portfolio,
        )?;
        self.delete_blocks_after(snapshot.block_num)?;
        Ok(())
    }

    fn with<T>(&self, f: impl FnOnce(&PersistStore) -> AppResult<T>) -> AppResult<T> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| AppError::Internal("sqlite mutex poisoned".into()))?;
        f(&guard)
    }
}

async fn apply_snapshot_to_memory(
    index: &SharedIndexState,
    markets: Vec<Market>,
    orders: Vec<PersistOrderRow>,
    last_trades: Vec<(String, u64)>,
    levels: Vec<PersistBookLevel>,
    metas: Vec<PersistBookMeta>,
    vaults: Vec<Vault>,
    vault_portfolio: Vec<PersistVaultPortfolioRow>,
) {
    for market in markets {
        index.upsert_market(market).await;
    }
    for row in orders {
        index
            .insert_order(
                row.order,
                row.user_address,
                &row.spot_market,
                row.chain_order_id,
                row.size_raw,
                row.filled_raw,
            )
            .await;
    }
    for (spot, price) in last_trades {
        index.record_last_trade_price(&spot, price).await;
    }
    index.books.import_from_persist(levels, metas).await;
    for vault in vaults {
        index.upsert_vault(vault).await;
    }
    index
        .import_vault_portfolio_for_persist(vault_portfolio)
        .await;
}

pub fn default_sqlite_path() -> PathBuf {
    PathBuf::from("data/clob-index.sqlite3")
}
