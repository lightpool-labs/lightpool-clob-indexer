// Copyright (c) LightPool Labs

use std::collections::HashSet;

use base64::{engine::general_purpose::STANDARD as B64, Engine};
use lightpool_sdk::ReceiptBlock;
use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};
use crate::persist::CheckpointSnapshot;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerTip {
    pub block_num: u64,
    pub digest: String,
    pub connected: bool,
    pub checkpoint_block_num: Option<u64>,
    pub checkpoint_digest: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerBlockRow {
    pub block_num: u64,
    pub digest: String,
    pub payload_b64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerBlocksResponse {
    pub blocks: Vec<PeerBlockRow>,
}

#[derive(Clone)]
pub struct PeerClient {
    http: reqwest::Client,
}

impl PeerClient {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(60))
                .build()
                .expect("reqwest client"),
        }
    }

    pub async fn tip(&self, base: &str) -> AppResult<PeerTip> {
        let url = format!("{}/api/peer/tip", base.trim_end_matches('/'));
        self.http
            .get(url)
            .send()
            .await
            .map_err(|e| AppError::Internal(format!("peer tip request: {e}")))?
            .error_for_status()
            .map_err(|e| AppError::Internal(format!("peer tip status: {e}")))?
            .json()
            .await
            .map_err(|e| AppError::Internal(format!("peer tip decode: {e}")))
    }

    pub async fn checkpoint(&self, base: &str) -> AppResult<CheckpointSnapshot> {
        let url = format!("{}/api/peer/checkpoint", base.trim_end_matches('/'));
        self.http
            .get(url)
            .send()
            .await
            .map_err(|e| AppError::Internal(format!("peer checkpoint request: {e}")))?
            .error_for_status()
            .map_err(|e| AppError::Internal(format!("peer checkpoint status: {e}")))?
            .json()
            .await
            .map_err(|e| AppError::Internal(format!("peer checkpoint decode: {e}")))
    }

    pub async fn blocks_after(
        &self,
        base: &str,
        after_block_num: u64,
        limit: usize,
    ) -> AppResult<Vec<(u64, String, Vec<u8>)>> {
        let url = format!(
            "{}/api/peer/blocks?after={after_block_num}&limit={limit}",
            base.trim_end_matches('/')
        );
        let resp: PeerBlocksResponse = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| AppError::Internal(format!("peer blocks request: {e}")))?
            .error_for_status()
            .map_err(|e| AppError::Internal(format!("peer blocks status: {e}")))?
            .json()
            .await
            .map_err(|e| AppError::Internal(format!("peer blocks decode: {e}")))?;

        let mut out = Vec::with_capacity(resp.blocks.len());
        for row in resp.blocks {
            let payload = B64
                .decode(row.payload_b64.as_bytes())
                .map_err(|e| AppError::Internal(format!("peer block b64: {e}")))?;
            out.push((row.block_num, row.digest, payload));
        }
        Ok(out)
    }

    pub async fn download_blocks_until_empty(
        &self,
        base: &str,
        mut after_block_num: u64,
    ) -> AppResult<Vec<(u64, String, Vec<u8>)>> {
        const PAGE: usize = 200;
        let mut all = Vec::new();
        loop {
            let page = self.blocks_after(base, after_block_num, PAGE).await?;
            if page.is_empty() {
                break;
            }
            let page_len = page.len();
            after_block_num = page.last().map(|(n, _, _)| *n).unwrap_or(after_block_num);
            all.extend(page);
            if page_len < PAGE {
                break;
            }
        }
        Ok(all)
    }
}

pub fn decode_block_payload(block_num: u64, payload: &[u8]) -> AppResult<ReceiptBlock> {
    match bincode::deserialize(payload) {
        Ok(block) => Ok(block),
        Err(bincode_error) => serde_json::from_slice(payload).map_err(|json_error| {
            AppError::Internal(format!(
                "decode peer block {block_num}: bincode={bincode_error}; json={json_error}"
            ))
        }),
    }
}

pub fn encode_block_payload(block: &ReceiptBlock) -> AppResult<Vec<u8>> {
    bincode::serialize(block)
        .map_err(|e| AppError::Internal(format!("encode block payload: {e}")))
}

pub fn select_catchup_peer<'a>(
    tips: &'a [(String, PeerTip)],
    local_block_num: u64,
    threshold: u64,
) -> Option<&'a (String, PeerTip)> {
    tips.iter()
        .filter(|(_, tip)| tip.block_num > local_block_num.saturating_add(threshold))
        .filter(|(_, tip)| tip.checkpoint_block_num.is_some())
        .max_by_key(|(_, tip)| tip.block_num)
}

pub fn digest_set_from_blocks(blocks: &[(u64, String, Vec<u8>)]) -> HashSet<String> {
    blocks.iter().map(|(_, d, _)| d.clone()).collect()
}
