// Copyright (c) LightPool Labs
// Author: xiaoyu1998


use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use lightpool_sdk::ReceiptBlock;
use rusqlite::Connection;
use tokio::sync::mpsc;

use crate::domain::{Market, Vault};
use crate::error::{AppError, AppResult};
use crate::indexer::SharedIndexState;

use super::op::PersistOp;
use super::store::{BlockStore, StateStore};
use super::timing::{block_num_of, persist_timing, PersistOpKind, TimedPersistOp};
use super::types::{
    CheckpointSnapshot, ClosedBarRow, PersistBookLevel, PersistBookMeta, PersistMeta,
    PersistOrderRow, PersistVaultPortfolioRow, BAR_HISTORY_LIMIT, DEFAULT_PERSIST_QUEUE_CAPACITY,
    SECONDARY_PENDING_LIMIT,
};
use super::workers::PersistWorkers;

#[derive(Clone)]
pub struct SharedPersist {
    blocks: Arc<Mutex<BlockStore>>,
    state: Arc<Mutex<StateStore>>,
    op_tx: mpsc::Sender<TimedPersistOp>,
    ckpt_tx: mpsc::Sender<TimedPersistOp>,
    pending: Arc<AtomicU64>,
    /// When false, order-history / bars enqueues are skipped (catch-up / quiet).
    secondary_enabled: Arc<AtomicBool>,
}

fn open_sqlite_conn(path: &Path) -> AppResult<Connection> {
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
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -65536;
        PRAGMA foreign_keys = ON;
        ",
    )
    .map_err(|e| AppError::Internal(format!("sqlite pragma: {e}")))?;
    Ok(conn)
}

/// Derive `*-blocks.sqlite3` / `*-state.sqlite3` from `SQLITE_PATH` stem.
pub fn split_sqlite_paths(base: &Path) -> (PathBuf, PathBuf) {
    let parent = base.parent().unwrap_or_else(|| Path::new("."));
    let stem = base
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("clob-index");
    let ext = base
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("sqlite3");
    (
        parent.join(format!("{stem}-blocks.{ext}")),
        parent.join(format!("{stem}-state.{ext}")),
    )
}

impl SharedPersist {
    /// Open blocks + state sqlite files and return deferred persist pipeline.
    pub fn open(path: impl AsRef<Path>) -> AppResult<(Self, PersistWorkers)> {
        let base = path.as_ref();
        let (blocks_path, state_path) = split_sqlite_paths(base);

        let blocks_conn = open_sqlite_conn(&blocks_path)?;
        let state_conn = open_sqlite_conn(&state_path)?;
        let blocks = BlockStore { conn: blocks_conn };
        let state = StateStore { conn: state_conn };
        blocks.migrate()?;
        state.migrate()?;

        let blocks = Arc::new(Mutex::new(blocks));
        let state = Arc::new(Mutex::new(state));
        let pending = Arc::new(AtomicU64::new(0));
        let secondary_enabled = Arc::new(AtomicBool::new(true));
        let (op_tx, op_rx) = mpsc::channel::<TimedPersistOp>(DEFAULT_PERSIST_QUEUE_CAPACITY);
        let (ckpt_tx, ckpt_rx) = mpsc::channel::<TimedPersistOp>(8);
        tracing::info!(
            blocks = %blocks_path.display(),
            state = %state_path.display(),
            queue_capacity = DEFAULT_PERSIST_QUEUE_CAPACITY,
            "sqlite persist opened (blocks + state + checkpoint lane)"
        );
        Ok((
            Self {
                blocks: blocks.clone(),
                state: state.clone(),
                op_tx,
                ckpt_tx,
                pending: pending.clone(),
                secondary_enabled,
            },
            PersistWorkers {
                blocks,
                state,
                pending,
                rx: op_rx,
                ckpt_rx,
            },
        ))
    }

    pub fn set_secondary_persist(&self, enabled: bool) {
        self.secondary_enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn secondary_persist_enabled(&self) -> bool {
        self.secondary_enabled.load(Ordering::Relaxed)
    }

    fn secondary_ok(&self) -> bool {
        self.secondary_enabled.load(Ordering::Relaxed)
            && self.pending.load(Ordering::Relaxed) <= SECONDARY_PENDING_LIMIT
    }

    fn submit_nowait(&self, op: PersistOp) {
        let kind = PersistOpKind::from_op(&op);
        let block_num = block_num_of(&op);
        let op_id = persist_timing().mark_enqueued(kind, block_num);
        self.pending.fetch_add(1, Ordering::Relaxed);
        let timed = TimedPersistOp { op_id, op };
        match self.op_tx.try_send(timed) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.pending.fetch_sub(1, Ordering::Relaxed);
                persist_timing().cancel(op_id);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.pending.fetch_sub(1, Ordering::Relaxed);
                persist_timing().cancel(op_id);
                tracing::error!("persist pipeline stopped; dropping persist op");
            }
        }
    }

    fn submit_blocking(&self, op: PersistOp) {
        let kind = PersistOpKind::from_op(&op);
        let block_num = block_num_of(&op);
        let op_id = persist_timing().mark_enqueued(kind, block_num);
        self.pending.fetch_add(1, Ordering::Relaxed);
        let timed = TimedPersistOp { op_id, op };
        let tx = self.op_tx.clone();
        let result = if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| tx.blocking_send(timed))
        } else {
            tx.blocking_send(timed)
        };
        if result.is_err() {
            self.pending.fetch_sub(1, Ordering::Relaxed);
            persist_timing().cancel(op_id);
            tracing::error!("persist pipeline stopped; dropping persist op");
        }
    }

    async fn submit_async(&self, op: PersistOp) {
        let kind = PersistOpKind::from_op(&op);
        let block_num = block_num_of(&op);
        let op_id = persist_timing().mark_enqueued(kind, block_num);
        self.pending.fetch_add(1, Ordering::Relaxed);
        let timed = TimedPersistOp { op_id, op };
        if self.op_tx.send(timed).await.is_err() {
            self.pending.fetch_sub(1, Ordering::Relaxed);
            persist_timing().cancel(op_id);
            tracing::error!("persist pipeline stopped; dropping persist op");
        }
    }

    /// Clone the block into the ingress queue; encode workers bincode it later.
    pub async fn enqueue_receipt_block(&self, block: &ReceiptBlock) {
        self.submit_async(PersistOp::SaveReceiptBlock(block.clone()))
            .await;
    }

    pub fn enqueue_order_history(&self, row: PersistOrderRow) {
        if !self.secondary_ok() {
            return;
        }
        self.submit_nowait(PersistOp::UpsertOrderHistory(row));
    }

    pub fn enqueue_closed_bar(&self, bar: ClosedBarRow) {
        if !self.secondary_ok() {
            return;
        }
        self.submit_nowait(PersistOp::SaveClosedBar(bar));
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
        let op = PersistOp::Checkpoint {
            block_num,
            digest,
            markets,
            orders,
            last_trades,
            levels,
            metas,
            vaults,
            vault_portfolio,
        };
        let kind = PersistOpKind::from_op(&op);
        let block_num = block_num_of(&op);
        let op_id = persist_timing().mark_enqueued(kind, block_num);
        self.pending.fetch_add(1, Ordering::Relaxed);
        let timed = TimedPersistOp { op_id, op };
        let tx = self.ckpt_tx.clone();
        let result = if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| tx.blocking_send(timed))
        } else {
            tx.blocking_send(timed)
        };
        if result.is_err() {
            self.pending.fetch_sub(1, Ordering::Relaxed);
            persist_timing().cancel(op_id);
            tracing::error!("checkpoint pipeline stopped; dropping checkpoint");
        }
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
        self.with_state(|store| store.read_meta())
    }

    pub fn export_checkpoint_snapshot(&self) -> AppResult<Option<CheckpointSnapshot>> {
        self.with_state(|store| {
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
        self.with_blocks(|store| store.save_block(block_num, digest, payload))
    }

    pub fn load_blocks_after(
        &self,
        after_block_num: Option<u64>,
    ) -> AppResult<Vec<(u64, String, Vec<u8>)>> {
        self.with_blocks(|store| store.load_blocks_after(after_block_num, None))
    }

    pub fn load_blocks_after_limited(
        &self,
        after_block_num: Option<u64>,
        limit: usize,
    ) -> AppResult<Vec<(u64, String, Vec<u8>)>> {
        self.with_blocks(|store| store.load_blocks_after(after_block_num, Some(limit)))
    }

    pub fn delete_blocks_after(&self, after_block_num: u64) -> AppResult<usize> {
        self.with_blocks(|store| store.delete_blocks_after(after_block_num))
    }

    pub fn save_closed_bar(&self, bar: &ClosedBarRow) -> AppResult<()> {
        self.with_state(|store| {
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
        self.with_state(|store| {
            store.load_closed_bars(spot_market, interval, from_ts, to_ts, limit)
        })
    }

    pub fn upsert_order_history(&self, row: &PersistOrderRow) -> AppResult<()> {
        self.with_state(|store| store.upsert_order_history(row))
    }

    pub fn list_order_history(
        &self,
        user_address: &str,
        limit: usize,
    ) -> AppResult<Vec<PersistOrderRow>> {
        self.with_state(|store| store.list_order_history(user_address, limit))
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
        self.with_state(|store| {
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
        })?;
        self.with_blocks(|store| {
            store.delete_blocks_through(block_num)?;
            Ok(())
        })
    }

    pub async fn load_into(
        &self,
        index: &SharedIndexState,
    ) -> AppResult<Option<PersistMeta>> {
        let (meta, markets, orders, last_trades, levels, metas, vaults, vault_portfolio) =
            self.with_state(|store| {
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
        // Drop any local blocks ahead of the peer snapshot before applying peer blocks.
        self.delete_blocks_after(snapshot.block_num)?;
        Ok(())
    }

    fn with_blocks<T>(&self, f: impl FnOnce(&BlockStore) -> AppResult<T>) -> AppResult<T> {
        let guard = self
            .blocks
            .lock()
            .map_err(|_| AppError::Internal("blocks sqlite mutex poisoned".into()))?;
        f(&guard)
    }

    fn with_state<T>(&self, f: impl FnOnce(&StateStore) -> AppResult<T>) -> AppResult<T> {
        let guard = self
            .state
            .lock()
            .map_err(|_| AppError::Internal("state sqlite mutex poisoned".into()))?;
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
