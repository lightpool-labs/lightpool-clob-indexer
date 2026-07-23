// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::env;

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
}

impl Config {
    pub fn from_env() -> Self {
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
            enable_indexer: env::var("ENABLE_INDEXER")
                .ok()
                .map(|v| v != "0" && v.to_lowercase() != "false")
                .unwrap_or(true),
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
        }
    }
}
