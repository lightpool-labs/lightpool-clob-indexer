// Copyright (c) LightPool Labs
// Author: xiaoyu1998


//! Epoch checkpoint = RocksDB Checkpoint of live index-state DB (`ckpt--{block_num}`).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::error::{AppError, AppResult};

use super::index_state_tables::IndexStateStore;

pub fn ckpt_path_for(index_path: &Path, block_num: u64) -> PathBuf {
    let parent = index_path.parent().unwrap_or_else(|| Path::new("."));
    parent.join(format!("ckpt--{block_num}"))
}

/// True for epoch-end heights: 999, 1999, … when `epoch_length == 1000`.
pub fn is_epoch_end(block_num: u64, epoch_length: u64) -> bool {
    epoch_length > 0 && (block_num + 1) % epoch_length == 0
}

/// Clone live index-state RocksDB only (no sqlite / block trim).
/// Takes `write_gate` so no concurrent RocksDB writes land in the checkpoint.
pub fn clone_index_state(
    index: &Arc<IndexStateStore>,
    write_gate: &Mutex<()>,
    index_path: &Path,
    block_num: u64,
    digest: &str,
) -> AppResult<PathBuf> {
    let dest = ckpt_path_for(index_path, block_num);
    let _gate = write_gate
        .lock()
        .map_err(|_| AppError::Internal("index-state write gate poisoned".into()))?;
    index.checkpoint_db(&dest)?;
    tracing::info!(
        block_num,
        digest = %digest,
        ckpt = %dest.display(),
        "epoch checkpoint cloned from index-state rocksdb"
    );
    Ok(dest)
}
