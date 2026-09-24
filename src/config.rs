// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::env;
use std::path::PathBuf;

fn env_flag(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .map(|v| {
            let v = v.trim();
            !(v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("no"))
        })
        .unwrap_or(default)
}

#[derive(Clone, Debug)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub lightpool_rpc_url: String,
    pub lightpool_ws_url: String,
    pub lightpool_mempool_addr: String,
    pub enable_indexer: bool,
    pub query_account: String,
    pub submit_queue_capacity: usize,
    pub submit_wait_timeout_ms: u64,
    pub sqlite_path: PathBuf,
    /// When false, skip sqlite open/recover/history writes.
    pub enable_sqlite: bool,
    /// When true, persist receipt blocks to sqlite. Off by default.
    pub enable_blocks_persist: bool,
    /// When true, run RocksDB index-state WriteBatch pipeline. Off by default.
    pub enable_index_state_persist: bool,
    /// When true, clone index-state RocksDB at epoch ends (`ckpt--999`, …). Off by default.
    pub enable_epoch_checkpoint: bool,
    /// Epoch length in receipt blocks; used only when `enable_epoch_checkpoint` is true.
    pub checkpoint_every_blocks: u64,
    /// Peer clob-index base URLs (e.g. http://127.0.0.1:3003) for historic catch-up.
    pub peer_index_urls: Vec<String>,
    /// Catch up from a peer when peer tip is at least this many block_nums ahead.
    pub peer_catchup_threshold: u64,
}

impl Config {
    pub fn from_env() -> Self {
        Self::from_env_with_overrides(false, false, false)
    }

    /// CLI overrides for persist toggles.
    pub fn from_env_with_overrides(
        no_persist: bool,
        persist_blocks: bool,
        persist_checkpoint: bool,
    ) -> Self {
        let enable_sqlite = env_flag("ENABLE_SQLITE", true)
            && !env_flag("DISABLE_PERSIST", false)
            && !no_persist;
        let want_blocks = persist_blocks || env_flag("PERSIST_BLOCKS", false);
        let want_checkpoint = persist_checkpoint
            || env_flag("PERSIST_CHECKPOINT", false)
            || env_flag("ENABLE_EPOCH_CHECKPOINT", false);

        Self {
            host: env::var("HOST").unwrap_or_else(|_| "0.0.0.0".into()),
            port: env::var("PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3002),
            lightpool_rpc_url: env::var("LIGHTPOOL_RPC_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:26300".into()),
            lightpool_ws_url: env::var("LIGHTPOOL_WS_URL")
                .unwrap_or_else(|_| "ws://127.0.0.1:26400".into()),
            lightpool_mempool_addr: env::var("LIGHTPOOL_MEMPOOL_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:26000".into()),
            enable_indexer: env_flag("ENABLE_INDEXER", true),
            query_account: env::var("QUERY_ACCOUNT")
                .unwrap_or_else(|_| "0x0000000000000000000000000000000000000000".into()),
            submit_queue_capacity: env::var("SUBMIT_QUEUE_CAPACITY")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1024),
            submit_wait_timeout_ms: env::var("SUBMIT_WAIT_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(60_000),
            sqlite_path: env::var("SQLITE_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|_| crate::persist::default_sqlite_path()),
            enable_sqlite,
            enable_blocks_persist: enable_sqlite && want_blocks,
            enable_index_state_persist: enable_sqlite && want_checkpoint,
            enable_epoch_checkpoint: enable_sqlite && want_checkpoint,
            checkpoint_every_blocks: env::var("CHECKPOINT_EVERY_BLOCKS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1_000),
            peer_index_urls: env::var("PEER_INDEX_URLS")
                .ok()
                .map(|raw| {
                    raw.split(',')
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(|s| s.trim_end_matches('/').to_string())
                        .collect()
                })
                .unwrap_or_default(),
            peer_catchup_threshold: env::var("PEER_CATCHUP_THRESHOLD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(50),
        }
    }
}
