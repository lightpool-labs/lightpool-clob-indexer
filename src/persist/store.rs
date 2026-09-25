// Copyright (c) LightPool Labs
// Author: xiaoyu1998


use rusqlite::{params, Connection, OptionalExtension};

use crate::domain::{Market, Order, Vault};
use crate::error::{AppError, AppResult};
use crate::spot_market::normalize_spot_market_key;

use super::types::{
    ClosedBarRow, PersistBookLevel, PersistBookMeta, PersistMeta, PersistOrderRow,
    PersistVaultPortfolioRow, BAR_HISTORY_LIMIT, ORDER_HISTORY_LIMIT,
};

pub(crate) struct BlockStore {
    pub(crate) conn: Connection,
}

pub(crate) struct StateStore {
    pub(crate) conn: Connection,
}

impl BlockStore {
    pub(crate) fn migrate(&self) -> AppResult<()> {
        self.conn
            .execute_batch(
                "
                CREATE TABLE IF NOT EXISTS blocks (
                    block_num INTEGER PRIMARY KEY NOT NULL,
                    digest TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    saved_at_ms INTEGER NOT NULL
                );
                ",
            )
            .map_err(|e| AppError::Internal(format!("sqlite migrate blocks: {e}")))?;
        Ok(())
    }

    pub(crate) fn delete_blocks_through(&self, block_num: u64) -> AppResult<usize> {
        let deleted = self
            .conn
            .execute(
                "DELETE FROM blocks WHERE block_num <= ?1",
                params![block_num as i64],
            )
            .map_err(|e| AppError::Internal(format!("sqlite truncate blocks: {e}")))?;
        Ok(deleted)
    }

    pub(crate) fn save_block(&self, block_num: u64, digest: &str, payload: &[u8]) -> AppResult<()> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        self.conn
            .execute(
                "INSERT OR REPLACE INTO blocks (block_num, digest, payload, saved_at_ms)
                 VALUES (?1, ?2, ?3, ?4)",
                params![block_num as i64, digest, payload, now_ms],
            )
            .map_err(|e| AppError::Internal(format!("sqlite save_block: {e}")))?;
        Ok(())
    }

    pub(crate) fn load_blocks_after(
        &self,
        after_block_num: Option<u64>,
        limit: Option<usize>,
    ) -> AppResult<Vec<(u64, String, Vec<u8>)>> {
        let map_row = |row: &rusqlite::Row<'_>| -> rusqlite::Result<(u64, String, Vec<u8>)> {
            Ok((
                row.get::<_, i64>(0)? as u64,
                row.get::<_, String>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        };

        let mut out = Vec::new();
        if let Some(n) = after_block_num {
            let mut stmt = self
                .conn
                .prepare(
                    "SELECT block_num, digest, payload FROM blocks
                     WHERE block_num > ?1
                     ORDER BY block_num ASC, saved_at_ms ASC",
                )
                .map_err(|e| AppError::Internal(format!("sqlite prepare blocks: {e}")))?;
            let rows = stmt
                .query_map(params![n as i64], map_row)
                .map_err(|e| AppError::Internal(format!("sqlite query blocks: {e}")))?;
            for row in rows {
                out.push(row.map_err(|e| AppError::Internal(format!("sqlite blocks row: {e}")))?);
                if limit.is_some_and(|lim| out.len() >= lim) {
                    break;
                }
            }
        } else {
            let mut stmt = self
                .conn
                .prepare(
                    "SELECT block_num, digest, payload FROM blocks
                     ORDER BY block_num ASC, saved_at_ms ASC",
                )
                .map_err(|e| AppError::Internal(format!("sqlite prepare blocks: {e}")))?;
            let rows = stmt
                .query_map([], map_row)
                .map_err(|e| AppError::Internal(format!("sqlite query blocks: {e}")))?;
            for row in rows {
                out.push(row.map_err(|e| AppError::Internal(format!("sqlite blocks row: {e}")))?);
                if limit.is_some_and(|lim| out.len() >= lim) {
                    break;
                }
            }
        }
        Ok(out)
    }

    pub(crate) fn delete_blocks_after(&self, after_block_num: u64) -> AppResult<usize> {
        let deleted = self
            .conn
            .execute(
                "DELETE FROM blocks WHERE block_num > ?1",
                params![after_block_num as i64],
            )
            .map_err(|e| AppError::Internal(format!("sqlite delete blocks after: {e}")))?;
        Ok(deleted)
    }

    pub(crate) fn apply_encoded_blocks_batch(
        &self,
        ops: &[super::op::EncodedPersistOp],
    ) -> AppResult<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let tx = self
            .conn
            .unchecked_transaction()
            .map_err(|e| AppError::Internal(format!("sqlite begin blocks batch: {e}")))?;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        {
            let mut stmt = tx
                .prepare(
                    "INSERT OR REPLACE INTO blocks (block_num, digest, payload, saved_at_ms)
                     VALUES (?1, ?2, ?3, ?4)",
                )
                .map_err(|e| AppError::Internal(format!("sqlite prepare blocks batch: {e}")))?;
            for op in ops {
                match op {
                    super::op::EncodedPersistOp::SaveBlockBytes {
                        block_num,
                        digest,
                        payload,
                    } => {
                        stmt.execute(params![*block_num as i64, digest, payload, now_ms])
                            .map_err(|e| {
                                AppError::Internal(format!("sqlite batch save_block: {e}"))
                            })?;
                    }
                    _ => {
                        return Err(AppError::Internal(
                            "non-block op in blocks batch".into(),
                        ));
                    }
                }
            }
        }
        tx.commit()
            .map_err(|e| AppError::Internal(format!("sqlite commit blocks batch: {e}")))?;
        Ok(())
    }
}

impl StateStore {
    pub(crate) fn migrate(&self) -> AppResult<()> {
        self.conn
            .execute_batch(
                "
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY NOT NULL,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS markets (
                    id TEXT PRIMARY KEY NOT NULL,
                    payload TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS orders (
                    id TEXT PRIMARY KEY NOT NULL,
                    user_address TEXT NOT NULL,
                    chain_order_id TEXT NOT NULL,
                    spot_market TEXT NOT NULL,
                    size_raw INTEGER NOT NULL,
                    filled_raw INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_address);
                CREATE INDEX IF NOT EXISTS idx_orders_spot_chain
                    ON orders(spot_market, chain_order_id);

                CREATE TABLE IF NOT EXISTS last_trades (
                    spot_market TEXT PRIMARY KEY NOT NULL,
                    price_raw INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS book_levels (
                    spot_market TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price_raw INTEGER NOT NULL,
                    size_raw INTEGER NOT NULL,
                    PRIMARY KEY (spot_market, side, price_raw)
                );

                CREATE TABLE IF NOT EXISTS book_meta (
                    spot_market TEXT PRIMARY KEY NOT NULL,
                    sequence INTEGER NOT NULL,
                    last_trade_price INTEGER
                );

                CREATE TABLE IF NOT EXISTS vaults (
                    id TEXT PRIMARY KEY NOT NULL,
                    payload TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS vault_portfolio (
                    vault_id TEXT NOT NULL,
                    spot_market TEXT NOT NULL,
                    amount_raw INTEGER NOT NULL,
                    PRIMARY KEY (vault_id, spot_market)
                );

                CREATE TABLE IF NOT EXISTS bars (
                    spot_market TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    start_ts INTEGER NOT NULL,
                    open_raw INTEGER NOT NULL,
                    high_raw INTEGER NOT NULL,
                    low_raw INTEGER NOT NULL,
                    close_raw INTEGER NOT NULL,
                    volume_raw INTEGER NOT NULL,
                    trade_count INTEGER NOT NULL,
                    PRIMARY KEY (spot_market, interval, start_ts)
                );
                CREATE INDEX IF NOT EXISTS idx_bars_spot_interval_ts
                    ON bars(spot_market, interval, start_ts);

                CREATE TABLE IF NOT EXISTS order_history (
                    id TEXT PRIMARY KEY NOT NULL,
                    user_address TEXT NOT NULL,
                    chain_order_id TEXT NOT NULL,
                    spot_market TEXT NOT NULL,
                    size_raw INTEGER NOT NULL,
                    filled_raw INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status_ts_ms INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_order_history_user_ts
                    ON order_history(user_address, status_ts_ms DESC);
                ",
            )
            .map_err(|e| AppError::Internal(format!("sqlite migrate state: {e}")))?;
        Ok(())
    }

    pub(crate) fn read_meta(&self) -> AppResult<Option<PersistMeta>> {
        let block_num: Option<String> = self
            .conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'last_block_num'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| AppError::Internal(format!("sqlite read meta block: {e}")))?;
        let Some(block_num) = block_num else {
            return Ok(None);
        };
        let digest: String = self
            .conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'last_digest'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| AppError::Internal(format!("sqlite read meta digest: {e}")))?;
        let block_num: u64 = block_num
            .parse()
            .map_err(|e| AppError::Internal(format!("sqlite meta block_num: {e}")))?;
        Ok(Some(PersistMeta { block_num, digest }))
    }

    pub(crate) fn checkpoint(
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
        let tx = self
            .conn
            .unchecked_transaction()
            .map_err(|e| AppError::Internal(format!("sqlite begin: {e}")))?;

        tx.execute("DELETE FROM markets", [])
            .map_err(|e| AppError::Internal(format!("sqlite clear markets: {e}")))?;
        tx.execute("DELETE FROM orders", [])
            .map_err(|e| AppError::Internal(format!("sqlite clear orders: {e}")))?;
        tx.execute("DELETE FROM last_trades", [])
            .map_err(|e| AppError::Internal(format!("sqlite clear last_trades: {e}")))?;
        tx.execute("DELETE FROM book_levels", [])
            .map_err(|e| AppError::Internal(format!("sqlite clear book_levels: {e}")))?;
        tx.execute("DELETE FROM book_meta", [])
            .map_err(|e| AppError::Internal(format!("sqlite clear book_meta: {e}")))?;
        tx.execute("DELETE FROM vaults", [])
            .map_err(|e| AppError::Internal(format!("sqlite clear vaults: {e}")))?;
        tx.execute("DELETE FROM vault_portfolio", [])
            .map_err(|e| AppError::Internal(format!("sqlite clear vault_portfolio: {e}")))?;

        {
            let mut stmt = tx
                .prepare("INSERT INTO markets (id, payload) VALUES (?1, ?2)")
                .map_err(|e| AppError::Internal(format!("sqlite prepare markets: {e}")))?;
            for market in markets {
                let payload = serde_json::to_string(market)
                    .map_err(|e| AppError::Internal(format!("serialize market: {e}")))?;
                stmt.execute(params![market.id().to_string(), payload])
                    .map_err(|e| AppError::Internal(format!("sqlite insert market: {e}")))?;
            }
        }

        {
            let mut stmt = tx
                .prepare(
                    "INSERT INTO orders
                     (id, user_address, chain_order_id, spot_market, size_raw, filled_raw, status, payload)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                )
                .map_err(|e| AppError::Internal(format!("sqlite prepare orders: {e}")))?;
            for row in orders {
                let payload = serde_json::to_string(&row.order)
                    .map_err(|e| AppError::Internal(format!("serialize order: {e}")))?;
                stmt.execute(params![
                    row.order.id.to_string(),
                    row.user_address,
                    row.chain_order_id,
                    normalize_spot_market_key(&row.spot_market),
                    row.size_raw as i64,
                    row.filled_raw as i64,
                    row.order.status,
                    payload,
                ])
                .map_err(|e| AppError::Internal(format!("sqlite insert order: {e}")))?;
            }
        }

        {
            let mut stmt = tx
                .prepare("INSERT INTO last_trades (spot_market, price_raw) VALUES (?1, ?2)")
                .map_err(|e| AppError::Internal(format!("sqlite prepare last_trades: {e}")))?;
            for (spot, price) in last_trades {
                stmt.execute(params![normalize_spot_market_key(spot), *price as i64])
                    .map_err(|e| AppError::Internal(format!("sqlite insert last_trade: {e}")))?;
            }
        }

        {
            let mut stmt = tx
                .prepare(
                    "INSERT INTO book_levels (spot_market, side, price_raw, size_raw)
                     VALUES (?1, ?2, ?3, ?4)",
                )
                .map_err(|e| AppError::Internal(format!("sqlite prepare book_levels: {e}")))?;
            for level in levels {
                stmt.execute(params![
                    normalize_spot_market_key(&level.spot_market),
                    level.side,
                    level.price_raw as i64,
                    level.size_raw as i64,
                ])
                .map_err(|e| AppError::Internal(format!("sqlite insert book_level: {e}")))?;
            }
        }

        {
            let mut stmt = tx
                .prepare(
                    "INSERT INTO book_meta (spot_market, sequence, last_trade_price)
                     VALUES (?1, ?2, ?3)",
                )
                .map_err(|e| AppError::Internal(format!("sqlite prepare book_meta: {e}")))?;
            for meta in metas {
                stmt.execute(params![
                    normalize_spot_market_key(&meta.spot_market),
                    meta.sequence as i64,
                    meta.last_trade_price.map(|v| v as i64),
                ])
                .map_err(|e| AppError::Internal(format!("sqlite insert book_meta: {e}")))?;
            }
        }

        {
            let mut stmt = tx
                .prepare("INSERT INTO vaults (id, payload) VALUES (?1, ?2)")
                .map_err(|e| AppError::Internal(format!("sqlite prepare vaults: {e}")))?;
            for vault in vaults {
                let payload = serde_json::to_string(vault)
                    .map_err(|e| AppError::Internal(format!("serialize vault: {e}")))?;
                stmt.execute(params![vault.id.to_string(), payload])
                    .map_err(|e| AppError::Internal(format!("sqlite insert vault: {e}")))?;
            }
        }

        {
            let mut stmt = tx
                .prepare(
                    "INSERT INTO vault_portfolio (vault_id, spot_market, amount_raw)
                     VALUES (?1, ?2, ?3)",
                )
                .map_err(|e| AppError::Internal(format!("sqlite prepare vault_portfolio: {e}")))?;
            for row in vault_portfolio {
                stmt.execute(params![
                    row.vault_id,
                    normalize_spot_market_key(&row.spot_market),
                    row.amount_raw as i64,
                ])
                .map_err(|e| AppError::Internal(format!("sqlite insert vault_portfolio: {e}")))?;
            }
        }

        tx.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('last_block_num', ?1)",
            params![block_num.to_string()],
        )
        .map_err(|e| AppError::Internal(format!("sqlite meta block: {e}")))?;
        tx.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('last_digest', ?1)",
            params![digest],
        )
        .map_err(|e| AppError::Internal(format!("sqlite meta digest: {e}")))?;

        tx.commit()
            .map_err(|e| AppError::Internal(format!("sqlite commit checkpoint: {e}")))?;
        Ok(())
    }

    pub(crate) fn write_meta(&self, block_num: u64, digest: &str) -> AppResult<()> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('last_block_num', ?1)",
                params![block_num.to_string()],
            )
            .map_err(|e| AppError::Internal(format!("sqlite meta block: {e}")))?;
        self.conn
            .execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('last_digest', ?1)",
                params![digest],
            )
            .map_err(|e| AppError::Internal(format!("sqlite meta digest: {e}")))?;
        Ok(())
    }

    /// Consistent file snapshot of live state DB (epoch checkpoint).
    pub(crate) fn vacuum_into(&self, dest: &std::path::Path) -> AppResult<()> {
        if let Some(parent) = dest.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    AppError::Internal(format!("create ckpt dir {}: {e}", parent.display()))
                })?;
            }
        }
        if dest.exists() {
            std::fs::remove_file(dest).map_err(|e| {
                AppError::Internal(format!("remove existing ckpt {}: {e}", dest.display()))
            })?;
        }
        let dest_str = dest
            .to_str()
            .ok_or_else(|| AppError::Internal(format!("non-utf8 ckpt path {}", dest.display())))?;
        let escaped = dest_str.replace('\'', "''");
        self.conn
            .execute_batch(&format!("VACUUM INTO '{escaped}';"))
            .map_err(|e| {
                AppError::Internal(format!("VACUUM INTO {}: {e}", dest.display()))
            })?;
        Ok(())
    }

    pub(crate) fn apply_encoded_state_batch(
        &self,
        ops: &[super::op::EncodedPersistOp],
    ) -> AppResult<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let refs: Vec<&super::op::EncodedPersistOp> = ops.iter().collect();
        self.apply_simple_state_batch(&refs)
    }

    fn apply_simple_state_batch(
        &self,
        ops: &[&super::op::EncodedPersistOp],
    ) -> AppResult<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let tx = self
            .conn
            .unchecked_transaction()
            .map_err(|e| AppError::Internal(format!("sqlite begin state batch: {e}")))?;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        {
            let mut history_stmt = tx
                .prepare(
                    "INSERT INTO order_history
                     (id, user_address, chain_order_id, spot_market, size_raw, filled_raw, status, payload, status_ts_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                     ON CONFLICT(id) DO UPDATE SET
                       user_address = excluded.user_address,
                       chain_order_id = excluded.chain_order_id,
                       spot_market = excluded.spot_market,
                       size_raw = excluded.size_raw,
                       filled_raw = excluded.filled_raw,
                       status = excluded.status,
                       payload = excluded.payload,
                       status_ts_ms = excluded.status_ts_ms",
                )
                .map_err(|e| AppError::Internal(format!("sqlite prepare history batch: {e}")))?;
            let mut bar_stmt = tx
                .prepare(
                    "INSERT OR REPLACE INTO bars
                     (spot_market, interval, start_ts, open_raw, high_raw, low_raw, close_raw, volume_raw, trade_count)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                )
                .map_err(|e| AppError::Internal(format!("sqlite prepare bars batch: {e}")))?;

            for op in ops {
                match op {
                    super::op::EncodedPersistOp::UpsertOrderHistory {
                        order_id,
                        user_address,
                        chain_order_id,
                        spot_market,
                        size_raw,
                        filled_raw,
                        status,
                        payload,
                    } => {
                        history_stmt
                            .execute(params![
                                order_id,
                                user_address,
                                chain_order_id,
                                spot_market,
                                size_raw,
                                filled_raw,
                                status,
                                payload,
                                now_ms,
                            ])
                            .map_err(|e| {
                                AppError::Internal(format!("sqlite batch order_history: {e}"))
                            })?;
                    }
                    super::op::EncodedPersistOp::SaveClosedBar(bar) => {
                        bar_stmt
                            .execute(params![
                                normalize_spot_market_key(&bar.spot_market),
                                bar.interval,
                                bar.start_ts as i64,
                                bar.open_raw as i64,
                                bar.high_raw as i64,
                                bar.low_raw as i64,
                                bar.close_raw as i64,
                                bar.volume_raw as i64,
                                bar.trade_count as i64,
                            ])
                            .map_err(|e| {
                                AppError::Internal(format!("sqlite batch save_bar: {e}"))
                            })?;
                    }
                    super::op::EncodedPersistOp::SaveBlockBytes { .. } => {
                        return Err(AppError::Internal(
                            "block op in secondary state batch".into(),
                        ));
                    }
                }
            }
        }
        tx.commit()
            .map_err(|e| AppError::Internal(format!("sqlite commit state batch: {e}")))?;

        for op in ops {
            if let super::op::EncodedPersistOp::SaveClosedBar(bar) = op {
                self.trim_bars(&bar.spot_market, &bar.interval, BAR_HISTORY_LIMIT)?;
            }
        }
        Ok(())
    }

    pub(crate) fn load_markets(&self) -> AppResult<Vec<Market>> {
        let mut stmt = self
            .conn
            .prepare("SELECT payload FROM markets")
            .map_err(|e| AppError::Internal(format!("sqlite load markets: {e}")))?;
        let rows = stmt
            .query_map([], |row| {
                let payload: String = row.get(0)?;
                Ok(payload)
            })
            .map_err(|e| AppError::Internal(format!("sqlite query markets: {e}")))?;
        let mut out = Vec::new();
        for row in rows {
            let payload =
                row.map_err(|e| AppError::Internal(format!("sqlite market row: {e}")))?;
            let value: serde_json::Value = serde_json::from_str(&payload)
                .map_err(|e| AppError::Internal(format!("deserialize market json: {e}")))?;
            let market = match serde_json::from_value::<Market>(value.clone()) {
                Ok(market) => market,
                Err(_) => Market::from_legacy_flat(value).ok_or_else(|| {
                    AppError::Internal("deserialize market: unsupported payload".into())
                })?,
            };
            out.push(market);
        }
        Ok(out)
    }

    pub(crate) fn load_orders(&self) -> AppResult<Vec<PersistOrderRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT user_address, chain_order_id, spot_market, size_raw, filled_raw, payload
                 FROM orders",
            )
            .map_err(|e| AppError::Internal(format!("sqlite load orders: {e}")))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })
            .map_err(|e| AppError::Internal(format!("sqlite query orders: {e}")))?;
        let mut out = Vec::new();
        for row in rows {
            let (user_address, chain_order_id, spot_market, size_raw, filled_raw, payload) =
                row.map_err(|e| AppError::Internal(format!("sqlite order row: {e}")))?;
            let order: Order = serde_json::from_str(&payload)
                .map_err(|e| AppError::Internal(format!("deserialize order: {e}")))?;
            out.push(PersistOrderRow {
                order,
                user_address,
                chain_order_id,
                spot_market,
                size_raw: size_raw as u64,
                filled_raw: filled_raw as u64,
                status_ts_ms: 0,
            });
        }
        Ok(out)
    }

    pub(crate) fn load_last_trades(&self) -> AppResult<Vec<(String, u64)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT spot_market, price_raw FROM last_trades")
            .map_err(|e| AppError::Internal(format!("sqlite load last_trades: {e}")))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(|e| AppError::Internal(format!("sqlite query last_trades: {e}")))?;
        let mut out = Vec::new();
        for row in rows {
            let (spot, price) =
                row.map_err(|e| AppError::Internal(format!("sqlite last_trade row: {e}")))?;
            out.push((spot, price as u64));
        }
        Ok(out)
    }

    pub(crate) fn load_book_levels(&self) -> AppResult<Vec<PersistBookLevel>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT spot_market, side, price_raw, size_raw FROM book_levels",
            )
            .map_err(|e| AppError::Internal(format!("sqlite load book_levels: {e}")))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .map_err(|e| AppError::Internal(format!("sqlite query book_levels: {e}")))?;
        let mut out = Vec::new();
        for row in rows {
            let (spot_market, side, price_raw, size_raw) =
                row.map_err(|e| AppError::Internal(format!("sqlite book_level row: {e}")))?;
            out.push(PersistBookLevel {
                spot_market,
                side,
                price_raw: price_raw as u64,
                size_raw: size_raw as u64,
            });
        }
        Ok(out)
    }

    pub(crate) fn load_book_meta(&self) -> AppResult<Vec<PersistBookMeta>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT spot_market, sequence, last_trade_price FROM book_meta",
            )
            .map_err(|e| AppError::Internal(format!("sqlite load book_meta: {e}")))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                ))
            })
            .map_err(|e| AppError::Internal(format!("sqlite query book_meta: {e}")))?;
        let mut out = Vec::new();
        for row in rows {
            let (spot_market, sequence, last_trade_price) =
                row.map_err(|e| AppError::Internal(format!("sqlite book_meta row: {e}")))?;
            out.push(PersistBookMeta {
                spot_market,
                sequence: sequence as u64,
                last_trade_price: last_trade_price.map(|v| v as u64),
            });
        }
        Ok(out)
    }

    pub(crate) fn load_vaults(&self) -> AppResult<Vec<Vault>> {
        let mut stmt = self
            .conn
            .prepare("SELECT payload FROM vaults")
            .map_err(|e| AppError::Internal(format!("sqlite load vaults: {e}")))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| AppError::Internal(format!("sqlite query vaults: {e}")))?;
        let mut out = Vec::new();
        for row in rows {
            let payload =
                row.map_err(|e| AppError::Internal(format!("sqlite vault row: {e}")))?;
            let vault: Vault = serde_json::from_str(&payload)
                .map_err(|e| AppError::Internal(format!("deserialize vault: {e}")))?;
            out.push(vault);
        }
        Ok(out)
    }

    pub(crate) fn load_vault_portfolio(&self) -> AppResult<Vec<PersistVaultPortfolioRow>> {
        let mut stmt = self
            .conn
            .prepare("SELECT vault_id, spot_market, amount_raw FROM vault_portfolio")
            .map_err(|e| AppError::Internal(format!("sqlite load vault_portfolio: {e}")))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .map_err(|e| AppError::Internal(format!("sqlite query vault_portfolio: {e}")))?;
        let mut out = Vec::new();
        for row in rows {
            let (vault_id, spot_market, amount_raw) =
                row.map_err(|e| AppError::Internal(format!("sqlite vault_portfolio row: {e}")))?;
            out.push(PersistVaultPortfolioRow {
                vault_id,
                spot_market,
                amount_raw: amount_raw as u64,
            });
        }
        Ok(out)
    }

    pub(crate) fn save_closed_bar(&self, bar: &ClosedBarRow) -> AppResult<()> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO bars
                 (spot_market, interval, start_ts, open_raw, high_raw, low_raw, close_raw, volume_raw, trade_count)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    normalize_spot_market_key(&bar.spot_market),
                    bar.interval,
                    bar.start_ts as i64,
                    bar.open_raw as i64,
                    bar.high_raw as i64,
                    bar.low_raw as i64,
                    bar.close_raw as i64,
                    bar.volume_raw as i64,
                    bar.trade_count as i64,
                ],
            )
            .map_err(|e| AppError::Internal(format!("sqlite save bar: {e}")))?;
        Ok(())
    }

    pub(crate) fn trim_bars(&self, spot_market: &str, interval: &str, keep: usize) -> AppResult<()> {
        let spot = normalize_spot_market_key(spot_market);
        let keep = keep.max(1) as i64;
        self.conn
            .execute(
                "DELETE FROM bars
                 WHERE spot_market = ?1 AND interval = ?2
                   AND start_ts < (
                     SELECT MIN(start_ts) FROM (
                       SELECT start_ts FROM bars
                       WHERE spot_market = ?1 AND interval = ?2
                       ORDER BY start_ts DESC
                       LIMIT ?3
                     )
                   )",
                params![spot, interval, keep],
            )
            .map_err(|e| AppError::Internal(format!("sqlite trim bars: {e}")))?;
        Ok(())
    }

    pub(crate) fn load_closed_bars(
        &self,
        spot_market: &str,
        interval: &str,
        from_ts: Option<u64>,
        to_ts: Option<u64>,
        limit: usize,
    ) -> AppResult<Vec<ClosedBarRow>> {
        let spot = normalize_spot_market_key(spot_market);
        let from = from_ts.unwrap_or(0) as i64;
        let to = to_ts.unwrap_or(i64::MAX as u64) as i64;
        let limit = limit.max(1).min(BAR_HISTORY_LIMIT) as i64;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT spot_market, interval, start_ts, open_raw, high_raw, low_raw,
                        close_raw, volume_raw, trade_count
                 FROM bars
                 WHERE spot_market = ?1 AND interval = ?2
                   AND start_ts >= ?3 AND start_ts <= ?4
                 ORDER BY start_ts DESC
                 LIMIT ?5",
            )
            .map_err(|e| AppError::Internal(format!("sqlite prepare bars: {e}")))?;
        let rows = stmt
            .query_map(params![spot, interval, from, to, limit], |row| {
                Ok(ClosedBarRow {
                    spot_market: row.get(0)?,
                    interval: row.get(1)?,
                    start_ts: row.get::<_, i64>(2)? as u64,
                    open_raw: row.get::<_, i64>(3)? as u64,
                    high_raw: row.get::<_, i64>(4)? as u64,
                    low_raw: row.get::<_, i64>(5)? as u64,
                    close_raw: row.get::<_, i64>(6)? as u64,
                    volume_raw: row.get::<_, i64>(7)? as u64,
                    trade_count: row.get::<_, i64>(8)? as u64,
                })
            })
            .map_err(|e| AppError::Internal(format!("sqlite query bars: {e}")))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(|e| AppError::Internal(format!("sqlite bar row: {e}")))?);
        }
        out.reverse();
        Ok(out)
    }

    pub(crate) fn upsert_order_history(&self, row: &PersistOrderRow) -> AppResult<()> {
        let payload = serde_json::to_string(&row.order)
            .map_err(|e| AppError::Internal(format!("serialize order history: {e}")))?;
        let user = row.user_address.trim().to_ascii_lowercase();
        let spot = normalize_spot_market_key(&row.spot_market);
        let status_ts_ms = if row.status_ts_ms > 0 {
            row.status_ts_ms as i64
        } else {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        };
        self.conn
            .execute(
                "INSERT INTO order_history
                 (id, user_address, chain_order_id, spot_market, size_raw, filled_raw, status, payload, status_ts_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(id) DO UPDATE SET
                   user_address = excluded.user_address,
                   chain_order_id = excluded.chain_order_id,
                   spot_market = excluded.spot_market,
                   size_raw = excluded.size_raw,
                   filled_raw = excluded.filled_raw,
                   status = excluded.status,
                   payload = excluded.payload,
                   status_ts_ms = excluded.status_ts_ms",
                params![
                    row.order.id.to_string(),
                    user,
                    row.chain_order_id,
                    spot,
                    row.size_raw as i64,
                    row.filled_raw as i64,
                    row.order.status,
                    payload,
                    status_ts_ms,
                ],
            )
            .map_err(|e| AppError::Internal(format!("sqlite upsert order_history: {e}")))?;
        Ok(())
    }

    pub(crate) fn list_order_history(
        &self,
        user_address: &str,
        limit: usize,
    ) -> AppResult<Vec<PersistOrderRow>> {
        let user = user_address.trim().to_ascii_lowercase();
        let limit = limit.max(1).min(ORDER_HISTORY_LIMIT) as i64;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT user_address, chain_order_id, spot_market, size_raw, filled_raw, payload, status_ts_ms
                 FROM order_history
                 WHERE user_address = ?1
                 ORDER BY status_ts_ms DESC
                 LIMIT ?2",
            )
            .map_err(|e| AppError::Internal(format!("sqlite prepare order_history: {e}")))?;
        let rows = stmt
            .query_map(params![user, limit], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            })
            .map_err(|e| AppError::Internal(format!("sqlite query order_history: {e}")))?;
        let mut out = Vec::new();
        for row in rows {
            let (user_address, chain_order_id, spot_market, size_raw, filled_raw, payload, status_ts_ms) =
                row.map_err(|e| AppError::Internal(format!("sqlite order_history row: {e}")))?;
            let order: Order = serde_json::from_str(&payload)
                .map_err(|e| AppError::Internal(format!("deserialize order history: {e}")))?;
            out.push(PersistOrderRow {
                order,
                user_address,
                chain_order_id,
                spot_market,
                size_raw: size_raw as u64,
                filled_raw: filled_raw as u64,
                status_ts_ms: status_ts_ms.max(0) as u64,
            });
        }
        Ok(out)
    }
}
