//! Bootstrap a spot market via indexer `/api/tx/submit` (signed, wait receipt),
//! then **burst** unsigned maker-style place txs like `burst_transfer`:
//! fire-and-forget to mempool TCP (default) or indexer `/api/tx/inject`.
//! Does **not** wait for receipt / 504 on the flood path.
//!
//! ```bash
//! cargo run --example submit_repro -- \
//!   --depth 20 --mid 100.00 --move-ticks 1 \
//!   --tasks 1 --rate 10000
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use futures_util::SinkExt;
use lightpool_sdk::lightpool_types::SignedTransaction;
use lightpool_sdk::{
    extract_market_address_from_events, extract_token_address_from_events, ActionBuilder,
    ContractAddress, CreateMarketParams, CreateTokenParams, MarketState, OrderParamsType,
    OrderSide, PlaceOrderParams, SegmentSize, Signer, TimeInForce, TransactionBuilder,
    TransactionReceipt, TOKEN_SCALE,
};
use serde::Deserialize;
use serde_json::json;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

const TICK_SIZE: u64 = 10_000;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum BurstTarget {
    /// Raw mempool TCP (same as burst_transfer)
    Mempool,
    /// Indexer `/api/tx/inject` (mempool wire ack only, no receipt wait)
    Index,
}

#[derive(Debug, Parser)]
#[command(about = "Bootstrap via indexer submit, then burst unsigned places (no receipt wait)")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:3002", env = "INDEX_HTTP")]
    index_http: String,

    #[arg(long, default_value = "127.0.0.1:26000", env = "LIGHTPOOL_MEMPOOL_ADDR")]
    mempool: String,

    #[arg(long, value_enum, default_value_t = BurstTarget::Mempool)]
    target: BurstTarget,

    #[arg(
        long,
        env = "LIGHTPOOL_PRIVATE_KEY",
        default_value = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
    )]
    private_key: String,

    #[arg(long, default_value = "AAPL")]
    symbol: String,

    #[arg(long, default_value = "100.00")]
    mid: String,

    #[arg(long, default_value_t = 20)]
    depth: usize,

    #[arg(long, default_value_t = 1)]
    move_ticks: u64,

    #[arg(long, default_value = "0.1")]
    amount: String,

    /// Parallel burst workers (each mempool target gets its own TCP)
    #[arg(long, default_value_t = 1)]
    tasks: usize,

    /// Send rate per task (txs/s). 0 = unlimited.
    #[arg(long, default_value_t = 10_000)]
    rate: u64,

    #[arg(long, default_value_t = false)]
    verbose: bool,
}

#[derive(Default)]
struct Stats {
    sent: AtomicU64,
    errors: AtomicU64,
}

#[derive(Debug, Deserialize)]
struct SubmitOkBody {
    digest: String,
    #[allow(dead_code)]
    block_num: u64,
    receipt: TransactionReceipt,
}

fn parse_private_key(raw: &str) -> anyhow::Result<Signer> {
    let trimmed = raw.trim();
    let hex_body = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    let bytes = hex::decode(hex_body)?;
    if bytes.len() != 32 {
        anyhow::bail!("private key must be 32 bytes, got {}", bytes.len());
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(&bytes);
    Signer::from_secret_key_bytes(&key).map_err(|e| anyhow::anyhow!("{e}"))
}

fn dollars_to_price_raw(dollars: &str) -> anyhow::Result<u64> {
    let value: f64 = dollars.trim().parse()?;
    if !value.is_finite() || value <= 0.0 {
        anyhow::bail!("invalid mid dollars: {dollars}");
    }
    let raw = (value * TOKEN_SCALE as f64 / 100.0).round() as u64;
    let aligned = (raw / TICK_SIZE) * TICK_SIZE;
    if aligned == 0 {
        anyhow::bail!("mid too small after tick align");
    }
    Ok(aligned)
}

fn shares_to_amount_raw(shares: &str) -> anyhow::Result<u64> {
    let value: f64 = shares.trim().parse()?;
    if !value.is_finite() || value <= 0.0 {
        anyhow::bail!("invalid amount shares: {shares}");
    }
    let raw = (value * TOKEN_SCALE as f64).round() as u64;
    if raw < 100_000 {
        anyhow::bail!("amount raw {raw} is below min_order_size 100000 (0.1 share)");
    }
    Ok(raw)
}

fn next_expiration(seq: u64) -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .saturating_add(3_600)
        .saturating_add(seq)
}

fn sign_actions(
    signer: &Signer,
    actions: Vec<lightpool_sdk::lightpool_types::Action>,
    seq: u64,
) -> anyhow::Result<SignedTransaction> {
    let mut builder = TransactionBuilder::new()
        .sender(signer.address())
        .expiration(next_expiration(seq));
    for action in actions {
        builder = builder.add_action(action);
    }
    builder
        .build_and_sign_only(signer)
        .map_err(|e| anyhow::anyhow!("sign tx: {e}"))
}

/// Unsigned tx for burst path (same as burst_transfer).
fn build_place_without_sign(
    sender: lightpool_sdk::Address,
    spot_market: ContractAddress,
    lock_token: ContractAddress,
    side: OrderSide,
    price_raw: u64,
    amount_raw: u64,
    seq: u64,
) -> anyhow::Result<SignedTransaction> {
    let params = PlaceOrderParams {
        side,
        amount: amount_raw,
        order_type: OrderParamsType::Limit {
            tif: TimeInForce::GTC,
        },
        limit_price: price_raw,
        token_address: lock_token,
        cloid: Some(format!("elm-burst-{seq}")),
    };
    let action = ActionBuilder::place_order(spot_market, params)
        .map_err(|e| anyhow::anyhow!("place_order action: {e}"))?;
    TransactionBuilder::new()
        .sender(sender)
        .expiration(u64::MAX.saturating_sub(seq))
        .add_action(action)
        .build_and_without_sign()
        .map_err(|e| anyhow::anyhow!("build without sign: {e}"))
}

fn desired_place_levels(
    mid_raw: u64,
    depth: usize,
    usdt: ContractAddress,
    stock: ContractAddress,
) -> Vec<(OrderSide, u64, ContractAddress)> {
    let mut levels = Vec::with_capacity(depth.saturating_mul(2));
    for i in 1..=depth {
        let offset = (i as u64).saturating_mul(TICK_SIZE);
        if mid_raw > offset {
            levels.push((OrderSide::Buy, mid_raw - offset, usdt));
        }
        levels.push((
            OrderSide::Sell,
            mid_raw.saturating_add(offset),
            stock,
        ));
    }
    levels
}

async fn post_submit_raw(
    client: &reqwest::Client,
    index_http: &str,
    signed: &SignedTransaction,
) -> (u16, String, u128) {
    let url = format!("{}/api/tx/submit", index_http.trim_end_matches('/'));
    let started = Instant::now();
    match client
        .post(&url)
        .json(&json!({ "tx": signed }))
        .send()
        .await
    {
        Ok(response) => {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            (status, body, started.elapsed().as_millis())
        }
        Err(error) => (0, error.to_string(), started.elapsed().as_millis()),
    }
}

fn digest_hint(body: &str) -> String {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(body) {
        if let Some(digest) = value.get("digest").and_then(|v| v.as_str()) {
            return digest.to_string();
        }
        if let Some(error) = value.get("error").and_then(|v| v.as_str()) {
            return error.to_string();
        }
    }
    body.chars().take(200).collect()
}

async fn submit_expect_ok(
    client: &reqwest::Client,
    index_http: &str,
    signed: &SignedTransaction,
    label: &str,
) -> anyhow::Result<SubmitOkBody> {
    let digest_local = hex::encode(signed.digest().as_bytes());
    let (status, body, latency_ms) = post_submit_raw(client, index_http, signed).await;
    println!("{label} status={status} latency_ms={latency_ms} digest={digest_local}");
    match status {
        200 => {
            let parsed: SubmitOkBody = serde_json::from_str(&body)
                .map_err(|e| anyhow::anyhow!("{label} bad 200 body: {e}; body={body}"))?;
            if !parsed.receipt.is_success() {
                anyhow::bail!(
                    "{label} receipt failed: {:?} digest={}",
                    parsed.receipt.status,
                    parsed.digest
                );
            }
            Ok(parsed)
        }
        504 => anyhow::bail!(
            "{label} HTTP 504 (submit wait timeout) digest={digest_local} body={}",
            digest_hint(&body)
        ),
        other => anyhow::bail!(
            "{label} HTTP {other} digest={digest_local} body={}",
            digest_hint(&body)
        ),
    }
}

async fn bootstrap_market(
    client: &reqwest::Client,
    index_http: &str,
    signer: &Signer,
    symbol: &str,
) -> anyhow::Result<(ContractAddress, ContractAddress, ContractAddress)> {
    let sender = signer.address();
    let symbol = symbol.trim().to_ascii_uppercase();

    println!("bootstrap: create USDT");
    let usdt_action = ActionBuilder::create_token(CreateTokenParams {
        name: "USD Tether".into(),
        symbol: "USDT".into(),
        total_supply: 1_000_000_000 * TOKEN_SCALE,
        mintable: true,
        to: sender,
    })
    .map_err(|e| anyhow::anyhow!("create USDT action: {e}"))?;
    let usdt_tx = sign_actions(signer, vec![usdt_action], 1)?;
    let usdt_resp = submit_expect_ok(client, index_http, &usdt_tx, "create_usdt").await?;
    let usdt = extract_token_address_from_events(&usdt_resp.receipt)
        .ok_or_else(|| anyhow::anyhow!("create_usdt missing token_created"))?;
    println!("bootstrap: USDT={usdt}");

    println!("bootstrap: create {symbol}");
    let stock_action = ActionBuilder::create_token(CreateTokenParams {
        name: symbol.clone().into(),
        symbol: symbol.clone().into(),
        total_supply: 1_000_000_000 * TOKEN_SCALE,
        mintable: true,
        to: sender,
    })
    .map_err(|e| anyhow::anyhow!("create stock action: {e}"))?;
    let stock_tx = sign_actions(signer, vec![stock_action], 2)?;
    let stock_resp = submit_expect_ok(client, index_http, &stock_tx, "create_stock").await?;
    let stock = extract_token_address_from_events(&stock_resp.receipt)
        .ok_or_else(|| anyhow::anyhow!("create_stock missing token_created"))?;
    println!("bootstrap: {symbol}={stock}");

    println!("bootstrap: create {symbol}/USDT market");
    let market_action = ActionBuilder::create_market(CreateMarketParams {
        name: format!("{symbol}/USDT").into(),
        base_token: stock,
        quote_token: usdt,
        min_order_size: 100_000,
        tick_size: TICK_SIZE,
        maker_fee_bps: 0,
        taker_fee_bps: 0,
        allow_market_orders: true,
        state: MarketState::Active,
        limit_order: true,
        side_book_size: SegmentSize::Large,
        creator: sender,
        access: Default::default(),
    })
    .map_err(|e| anyhow::anyhow!("create market action: {e}"))?;
    let market_tx = sign_actions(signer, vec![market_action], 3)?;
    let market_resp = submit_expect_ok(client, index_http, &market_tx, "create_market").await?;
    let market = extract_market_address_from_events(&market_resp.receipt)
        .ok_or_else(|| anyhow::anyhow!("create_market missing market_created"))?;
    println!("bootstrap: market={market}");

    Ok((market, usdt, stock))
}

async fn wait_rate_token(
    rate_per_second: u64,
    rate_tokens: &mut f64,
    rate_last_refill: &mut Instant,
    stop: &AtomicBool,
) {
    if rate_per_second == 0 {
        return;
    }
    let effective_rate = rate_per_second as f64;
    let max_burst = effective_rate.max(1.0);
    loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let now = Instant::now();
        let elapsed = now.duration_since(*rate_last_refill).as_secs_f64();
        *rate_tokens = (*rate_tokens + elapsed * effective_rate).min(max_burst);
        *rate_last_refill = now;
        if *rate_tokens >= 1.0 {
            *rate_tokens -= 1.0;
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

async fn burst_mempool_task(
    task_id: usize,
    mempool_addr: String,
    sender: lightpool_sdk::Address,
    spot: ContractAddress,
    usdt: ContractAddress,
    stock: ContractAddress,
    depth: usize,
    move_ticks: u64,
    amount_raw: u64,
    mut mid_raw: u64,
    rate: u64,
    stats: Arc<Stats>,
    stop: Arc<AtomicBool>,
    seq_base: u64,
    verbose: bool,
) -> anyhow::Result<()> {
    let stream = TcpStream::connect(&mempool_addr).await?;
    let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
    println!("task={task_id} mempool connected {mempool_addr}");

    let mut rate_tokens = 0.0f64;
    let mut rate_last_refill = Instant::now();
    let mut seq = seq_base;
    let mut level_index = 0usize;

    while !stop.load(Ordering::Relaxed) {
        let levels = desired_place_levels(mid_raw, depth, usdt, stock);
        if levels.is_empty() {
            break;
        }
        if level_index >= levels.len() {
            level_index = 0;
            mid_raw = mid_raw.saturating_add(move_ticks.saturating_mul(TICK_SIZE));
            continue;
        }
        let (side, price_raw, lock_token) = levels[level_index];
        level_index += 1;

        wait_rate_token(rate, &mut rate_tokens, &mut rate_last_refill, &stop).await;
        if stop.load(Ordering::Relaxed) {
            break;
        }

        let tx = match build_place_without_sign(
            sender,
            spot,
            lock_token,
            side,
            price_raw,
            amount_raw,
            seq,
        ) {
            Ok(tx) => tx,
            Err(error) => {
                eprintln!("task={task_id} build failed: {error}");
                stats.errors.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };
        seq += 1;

        let tx_bytes = match bincode::serialize(&tx) {
            Ok(bytes) => bytes,
            Err(error) => {
                eprintln!("task={task_id} serialize failed: {error}");
                stats.errors.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };

        if let Err(error) = transport.send(Bytes::from(tx_bytes)).await {
            eprintln!("task={task_id} mempool send failed: {error}");
            stats.errors.fetch_add(1, Ordering::Relaxed);
            break;
        }
        stats.sent.fetch_add(1, Ordering::Relaxed);
        if verbose {
            println!(
                "task={task_id} sent side={side:?} price={price_raw} seq={}",
                seq - 1
            );
        }
    }
    Ok(())
}

async fn burst_index_task(
    task_id: usize,
    index_http: String,
    sender: lightpool_sdk::Address,
    spot: ContractAddress,
    usdt: ContractAddress,
    stock: ContractAddress,
    depth: usize,
    move_ticks: u64,
    amount_raw: u64,
    mut mid_raw: u64,
    rate: u64,
    stats: Arc<Stats>,
    stop: Arc<AtomicBool>,
    seq_base: u64,
    verbose: bool,
) -> anyhow::Result<()> {
    let http = reqwest::Client::new();
    let url = format!("{}/api/tx/inject", index_http.trim_end_matches('/'));
    println!("task={task_id} inject url={url}");

    let mut rate_tokens = 0.0f64;
    let mut rate_last_refill = Instant::now();
    let mut seq = seq_base;
    let mut level_index = 0usize;

    while !stop.load(Ordering::Relaxed) {
        let levels = desired_place_levels(mid_raw, depth, usdt, stock);
        if levels.is_empty() {
            break;
        }
        if level_index >= levels.len() {
            level_index = 0;
            mid_raw = mid_raw.saturating_add(move_ticks.saturating_mul(TICK_SIZE));
            continue;
        }
        let (side, price_raw, lock_token) = levels[level_index];
        level_index += 1;

        wait_rate_token(rate, &mut rate_tokens, &mut rate_last_refill, &stop).await;
        if stop.load(Ordering::Relaxed) {
            break;
        }

        let tx = match build_place_without_sign(
            sender,
            spot,
            lock_token,
            side,
            price_raw,
            amount_raw,
            seq,
        ) {
            Ok(tx) => tx,
            Err(error) => {
                eprintln!("task={task_id} build failed: {error}");
                stats.errors.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };
        seq += 1;

        match http.post(&url).json(&json!({ "tx": tx })).send().await {
            Ok(response) => {
                let status = response.status().as_u16();
                if status == 200 {
                    stats.sent.fetch_add(1, Ordering::Relaxed);
                    if verbose {
                        let body = response.text().await.unwrap_or_default();
                        println!(
                            "task={task_id} inject ok side={side:?} price={price_raw} body={}",
                            body.chars().take(80).collect::<String>()
                        );
                    }
                } else {
                    let body = response.text().await.unwrap_or_default();
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    eprintln!("task={task_id} inject status={status} body={}", digest_hint(&body));
                }
            }
            Err(error) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("task={task_id} inject error: {error}");
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let tasks = args.tasks.max(1);
    let depth = args.depth.max(1);
    let move_ticks = args.move_ticks.max(1);

    let signer = parse_private_key(&args.private_key)?;
    let mid_raw = dollars_to_price_raw(&args.mid)?;
    let amount_raw = shares_to_amount_raw(&args.amount)?;
    let http = reqwest::Client::new();

    println!(
        "submit_repro bootstrap=index submit; burst target={:?} mempool={} tasks={} rate_per_task={} depth={} move_ticks={} (Ctrl+C to stop)",
        args.target, args.mempool, tasks, args.rate, depth, move_ticks
    );

    let (spot, usdt, stock) =
        bootstrap_market(&http, &args.index_http, &signer, &args.symbol).await?;
    let sender = signer.address();
    println!("burst start spot={spot} usdt={usdt} stock={stock} (unsigned, no receipt wait)");

    let stop = Arc::new(AtomicBool::new(false));
    let stop_signal = stop.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        println!("Ctrl+C received; stopping...");
        stop_signal.store(true, Ordering::Relaxed);
    });

    let stats = Arc::new(Stats::default());
    {
        let stats = stats.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut last = 0u64;
            let mut last_at = Instant::now();
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let sent = stats.sent.load(Ordering::Relaxed);
                let errors = stats.errors.load(Ordering::Relaxed);
                let elapsed = last_at.elapsed().as_secs_f64().max(1e-6);
                let tps = (sent.saturating_sub(last)) as f64 / elapsed;
                println!("tps={tps:.1} sent={sent} errors={errors}");
                last = sent;
                last_at = Instant::now();
            }
        });
    }

    let mut handles = Vec::new();
    for task_id in 0..tasks {
        let stop = stop.clone();
        let stats = stats.clone();
        let seq_base = (task_id as u64).saturating_mul(1_000_000_000);
        match args.target {
            BurstTarget::Mempool => {
                handles.push(tokio::spawn(burst_mempool_task(
                    task_id,
                    args.mempool.clone(),
                    sender,
                    spot,
                    usdt,
                    stock,
                    depth,
                    move_ticks,
                    amount_raw,
                    mid_raw,
                    args.rate,
                    stats,
                    stop,
                    seq_base,
                    args.verbose,
                )));
            }
            BurstTarget::Index => {
                handles.push(tokio::spawn(burst_index_task(
                    task_id,
                    args.index_http.clone(),
                    sender,
                    spot,
                    usdt,
                    stock,
                    depth,
                    move_ticks,
                    amount_raw,
                    mid_raw,
                    args.rate,
                    stats,
                    stop,
                    seq_base,
                    args.verbose,
                )));
            }
        }
    }

    for handle in handles {
        let _ = handle.await?;
    }

    println!(
        "summary sent={} errors={}",
        stats.sent.load(Ordering::Relaxed),
        stats.errors.load(Ordering::Relaxed)
    );
    Ok(())
}
