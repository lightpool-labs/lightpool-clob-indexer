// Copyright (c) LightPool Labs
// Author: xiaoyu1998


use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use dashmap::DashMap;

use super::op::{EncodedPersistOp, PersistOp};

const MAX_ENTRIES: usize = 4096;
pub const SLOW_PERSIST_THRESHOLD: Duration = Duration::from_millis(50);

pub fn format_duration_2(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs >= 1.0 {
        format!("{:.2}s", secs)
    } else if secs >= 0.001 {
        format!("{:.2}ms", secs * 1_000.0)
    } else {
        format!("{:.2}µs", secs * 1_000_000.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistOpKind {
    Block,
    OrderHistory,
    ClosedBar,
    Checkpoint,
}

impl fmt::Display for PersistOpKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            PersistOpKind::Block => "block",
            PersistOpKind::OrderHistory => "history",
            PersistOpKind::ClosedBar => "bar",
            PersistOpKind::Checkpoint => "ckpt",
        };
        write!(f, "{name}")
    }
}

impl PersistOpKind {
    pub fn from_op(op: &PersistOp) -> Self {
        match op {
            PersistOp::SaveReceiptBlock(_) => PersistOpKind::Block,
            PersistOp::UpsertOrderHistory(_) => PersistOpKind::OrderHistory,
            PersistOp::SaveClosedBar(_) => PersistOpKind::ClosedBar,
            PersistOp::Checkpoint { .. } => PersistOpKind::Checkpoint,
        }
    }
}

pub fn block_num_of(op: &PersistOp) -> Option<u64> {
    match op {
        PersistOp::SaveReceiptBlock(block) => Some(block.block_num),
        PersistOp::Checkpoint { block_num, .. } => Some(*block_num),
        _ => None,
    }
}

#[derive(Debug, Clone)]
struct PersistOpTiming {
    kind: PersistOpKind,
    block_num: Option<u64>,
    encode_worker: Option<usize>,
    t0: Instant,
    t1: Option<Instant>,
    t2: Option<Instant>,
    t3: Option<Instant>,
    t4: Option<Instant>,
    t5: Option<Instant>,
}

impl PersistOpTiming {
    fn format_line(
        &self,
        op_id: u64,
        batch_size: usize,
        write: Duration,
        batch_wait: Duration,
    ) -> String {
        let opt_d = |from: Option<Instant>, to: Option<Instant>| -> String {
            match (from, to) {
                (Some(a), Some(b)) => format_duration_2(b.saturating_duration_since(a)),
                _ => "-".into(),
            }
        };
        let queue_wait = self
            .t1
            .map(|t1| format_duration_2(t1.saturating_duration_since(self.t0)))
            .unwrap_or_else(|| "-".into());
        let encode = opt_d(self.t1, self.t2);
        let encoded_wait = opt_d(self.t3.or(self.t2), self.t4);
        let coalesce_hold = opt_d(self.t4, self.t5);
        let total = self
            .t5
            .map(|t5| {
                // total ≈ enqueue → write end ≈ (t5 - t0) + batch_wait + write
                format_duration_2(
                    t5.saturating_duration_since(self.t0) + batch_wait + write,
                )
            })
            .unwrap_or_else(|| "-".into());

        format!(
            "PERSIST op_id={op_id} kind={} b={} worker={} \
             q={queue_wait} enc={encode} ewait={encoded_wait} coal={coalesce_hold} \
             bwait={} wr={} total={total} n={batch_size}",
            self.kind,
            self.block_num
                .map(|b| b.to_string())
                .unwrap_or_else(|| "-".into()),
            self.encode_worker
                .map(|w| w.to_string())
                .unwrap_or_else(|| "-".into()),
            format_duration_2(batch_wait),
            format_duration_2(write),
        )
    }
}

pub struct PersistTimingCollector {
    next_id: AtomicU64,
    entries: DashMap<u64, PersistOpTiming>,
}

impl PersistTimingCollector {
    fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            entries: DashMap::new(),
        }
    }

    fn maybe_trim(&self) {
        if self.entries.len() < MAX_ENTRIES {
            return;
        }
        let mut keys: Vec<u64> = self.entries.iter().map(|e| *e.key()).collect();
        keys.sort_unstable();
        let remove_count = keys.len().saturating_sub(MAX_ENTRIES / 2);
        for key in keys.into_iter().take(remove_count) {
            self.entries.remove(&key);
        }
    }

    pub fn mark_enqueued(&self, kind: PersistOpKind, block_num: Option<u64>) -> u64 {
        self.maybe_trim();
        let op_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.entries.insert(
            op_id,
            PersistOpTiming {
                kind,
                block_num,
                encode_worker: None,
                t0: Instant::now(),
                t1: None,
                t2: None,
                t3: None,
                t4: None,
                t5: None,
            },
        );
        op_id
    }

    pub fn cancel(&self, op_id: u64) {
        self.entries.remove(&op_id);
    }

    pub fn mark_encode_start(&self, op_id: u64, worker_id: usize) {
        if let Some(mut entry) = self.entries.get_mut(&op_id) {
            entry.t1 = Some(Instant::now());
            entry.encode_worker = Some(worker_id);
        }
    }

    pub fn mark_encode_done(&self, op_id: u64) {
        if let Some(mut entry) = self.entries.get_mut(&op_id) {
            entry.t2 = Some(Instant::now());
        }
    }

    pub fn mark_encoded_sent(&self, op_id: u64) {
        if let Some(mut entry) = self.entries.get_mut(&op_id) {
            entry.t3 = Some(Instant::now());
        }
    }

    pub fn mark_coalesce_recv(&self, op_id: u64) {
        if let Some(mut entry) = self.entries.get_mut(&op_id) {
            entry.t4 = Some(Instant::now());
        }
    }

    pub fn mark_batch_sent(&self, op_ids: &[u64]) {
        let now = Instant::now();
        for op_id in op_ids {
            if let Some(mut entry) = self.entries.get_mut(op_id) {
                entry.t5 = Some(now);
            }
        }
    }

    /// Write finished for a batch: log slow ops and drop entries.
    pub fn finish_write(&self, op_ids: &[u64], write: Duration, batch_wait: Duration) {
        let batch_size = op_ids.len();
        for &op_id in op_ids {
            let Some((_, timing)) = self.entries.remove(&op_id) else {
                continue;
            };
            let total = timing
                .t5
                .map(|t5| t5.saturating_duration_since(timing.t0) + batch_wait + write)
                .unwrap_or(write);
            if total >= SLOW_PERSIST_THRESHOLD
                || write >= SLOW_PERSIST_THRESHOLD
                || tracing::enabled!(tracing::Level::DEBUG)
            {
                let line = timing.format_line(op_id, batch_size, write, batch_wait);
                if total >= SLOW_PERSIST_THRESHOLD || write >= SLOW_PERSIST_THRESHOLD {
                    tracing::warn!("{line}");
                } else {
                    tracing::debug!("{line}");
                }
            }
        }
    }
}

pub fn persist_timing() -> &'static PersistTimingCollector {
    static COLLECTOR: OnceLock<PersistTimingCollector> = OnceLock::new();
    COLLECTOR.get_or_init(PersistTimingCollector::new)
}

/// Ingress item carrying timing id.
#[derive(Debug)]
pub struct TimedPersistOp {
    pub op_id: u64,
    pub op: PersistOp,
}

/// Encoded item carrying timing id.
#[derive(Debug)]
pub struct TimedEncodedOp {
    pub op_id: u64,
    pub op: EncodedPersistOp,
}
