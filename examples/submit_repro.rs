//! Bootstrap spot markets via indexer `/api/tx/submit` (signed, wait receipt),
//! then **burst** unsigned place txs through indexer `/api/tx/inject`
//! (no receipt wait / 504 on the flood path). Uses concurrent in-flight HTTP
//! so one task is not limited to ~1/RTT sequential posts.
//!
//! ```bash
//! cargo run --example submit_repro -- \
//!   --num-markets 500 --senders 1024 \
//!   --depth 20 --mid 100.00 --move-ticks 1 \
//!   --tasks 1 --inflight 512 --rate 100000
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use lightpool_sdk::lightpool_types::SignedTransaction;
use lightpool_sdk::spot_events::MarketCreatedEvent;
use lightpool_sdk::token_events::TokenCreatedEvent;
use lightpool_sdk::{
    ActionBuilder, ContractAddress, CreateMarketParams, CreateTokenParams, EventData, EventType,
    MarketState, OrderParamsType, OrderSide, PlaceOrderParams, SegmentSize, Signer, TimeInForce,
    TransactionBuilder, TransactionReceipt, TransferParams, TOKEN_SCALE,
};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

const TICK_SIZE: u64 = 10_000;
const SETUP_ACTIONS_BATCH: usize = 64;
const FUND_PER_SENDER: u64 = 10_000_000 * TOKEN_SCALE;

#[derive(Debug, Parser)]
#[command(about = "Bootstrap via indexer submit, then burst inject (no receipt wait)")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:3002", env = "INDEX_HTTP")]
    index_http: String,

    #[arg(
        long,
        env = "LIGHTPOOL_PRIVATE_KEY",
        default_value = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
    )]
    private_key: String,

    /// Number of spot markets to create
    #[arg(long, default_value_t = 500)]
    num_markets: usize,

    /// Number of sender accounts to fund for parallel burst orders
    #[arg(long, default_value_t = 1024)]
    senders: usize,

    #[arg(long, default_value = "100.00")]
    mid: String,

    #[arg(long, default_value_t = 20)]
    depth: usize,

    #[arg(long, default_value_t = 1)]
    move_ticks: u64,

    #[arg(long, default_value = "0.1")]
    amount: String,

    /// Parallel burst workers
    #[arg(long, default_value_t = 1)]
    tasks: usize,

    /// Max in-flight HTTP injects per task (pipeline; 1 = sequential ~8k TPS)
    #[arg(long, default_value_t = 512)]
    inflight: usize,

    /// Send rate per task (txs/s). 0 = unlimited.
    #[arg(long, default_value_t = 500)]
    rate: u64,

    #[arg(long, default_value_t = false)]
    verbose: bool,
}

#[derive(Debug, Clone)]
struct SpotMarketInfo {
    market: ContractAddress,
    quote: ContractAddress,
    base: ContractAddress,
}

struct BurstSender {
    address: lightpool_sdk::Address,
    market_index: usize,
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

fn senders_for_market(senders: usize, num_markets: usize, market_index: usize) -> usize {
    senders / num_markets + if market_index < senders % num_markets { 1 } else { 0 }
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
    quote: ContractAddress,
    base: ContractAddress,
) -> Vec<(OrderSide, u64, ContractAddress)> {
    let mut levels = Vec::with_capacity(depth.saturating_mul(2));
    for i in 1..=depth {
        let offset = (i as u64).saturating_mul(TICK_SIZE);
        if mid_raw > offset {
            levels.push((OrderSide::Buy, mid_raw - offset, quote));
        }
        levels.push((
            OrderSide::Sell,
            mid_raw.saturating_add(offset),
            base,
        ));
    }
    levels
}

fn extract_token_addresses_from_events(receipt: &TransactionReceipt) -> Vec<ContractAddress> {
    let mut tokens = Vec::new();
    for event in &receipt.events {
        if let EventType::Call(action_name) = &event.event_type {
            if action_name == "token_created" {
                if let EventData::Bytes(data) = &event.data {
                    if let Ok(ev) = bincode::deserialize::<TokenCreatedEvent>(data) {
                        tokens.push(ev.token_address);
                    }
                }
            }
        }
    }
    tokens
}

fn extract_markets_from_events(
    receipt: &TransactionReceipt,
    tokens: &[ContractAddress],
    markets_so_far: usize,
) -> anyhow::Result<Vec<SpotMarketInfo>> {
    let mut markets = Vec::new();
    for event in &receipt.events {
        if let EventType::Call(action_name) = &event.event_type {
            if action_name == "market_created" {
                if let EventData::Bytes(data) = &event.data {
                    if let Ok(ev) = bincode::deserialize::<MarketCreatedEvent>(data) {
                        let market_index = markets_so_far + markets.len();
                        let base_index = market_index * 2;
                        let quote_index = base_index + 1;
                        if quote_index >= tokens.len() {
                            anyhow::bail!(
                                "not enough tokens for market {market_index} (need {}, have {})",
                                quote_index + 1,
                                tokens.len()
                            );
                        }
                        markets.push(SpotMarketInfo {
                            market: ev.market_address,
                            base: tokens[base_index],
                            quote: tokens[quote_index],
                        });
                    }
                }
            }
        }
    }
    Ok(markets)
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

async fn submit_signed_actions(
    client: &reqwest::Client,
    index_http: &str,
    signer: &Signer,
    actions: Vec<lightpool_sdk::lightpool_types::Action>,
    seq: u64,
    label: &str,
) -> anyhow::Result<TransactionReceipt> {
    let signed = sign_actions(signer, actions, seq)?;
    let resp = submit_expect_ok(client, index_http, &signed, label).await?;
    Ok(resp.receipt)
}

async fn create_tokens(
    client: &reqwest::Client,
    index_http: &str,
    signer: &Signer,
    num_markets: usize,
    senders: usize,
) -> anyhow::Result<Vec<ContractAddress>> {
    let num_tokens = num_markets * 2;
    let creator = signer.address();
    let mut all_tokens = Vec::with_capacity(num_tokens);
    let mut seq = 1u64;

    println!("bootstrap: creating {num_tokens} tokens for {num_markets} markets...");
    for batch_start in (0..num_tokens).step_by(SETUP_ACTIONS_BATCH) {
        let batch_end = (batch_start + SETUP_ACTIONS_BATCH).min(num_tokens);
        let mut actions = Vec::with_capacity(batch_end - batch_start);
        for token_index in batch_start..batch_end {
            let market_index = token_index / 2;
            let market_senders = senders_for_market(senders, num_markets, market_index);
            let total_supply = FUND_PER_SENDER
                .saturating_mul(market_senders as u64)
                .saturating_add(FUND_PER_SENDER);
            let create_params = CreateTokenParams {
                name: format!("BurstToken{}", token_index + 1).into(),
                symbol: format!("BT{}", token_index + 1).into(),
                total_supply,
                mintable: false,
                to: creator,
            };
            let action = ActionBuilder::create_token(create_params)
                .map_err(|e| anyhow::anyhow!("create_token action: {e}"))?;
            actions.push(action);
        }
        let receipt = submit_signed_actions(
            client,
            index_http,
            signer,
            actions,
            seq,
            &format!("create_tokens_{batch_start}_{}", batch_end - 1),
        )
        .await?;
        seq += 1;
        all_tokens.extend(extract_token_addresses_from_events(&receipt));
        println!(
            "bootstrap: tokens batch {batch_start}-{} ({} total)",
            batch_end - 1,
            all_tokens.len()
        );
    }

    if all_tokens.len() != num_tokens {
        anyhow::bail!(
            "expected {num_tokens} tokens from creation events, got {}",
            all_tokens.len()
        );
    }
    Ok(all_tokens)
}

async fn create_markets(
    client: &reqwest::Client,
    index_http: &str,
    signer: &Signer,
    tokens: &[ContractAddress],
    num_markets: usize,
) -> anyhow::Result<Vec<SpotMarketInfo>> {
    let creator = signer.address();
    let mut all_markets = Vec::with_capacity(num_markets);
    let mut seq = 1_000u64;

    println!("bootstrap: creating {num_markets} markets...");
    for batch_start in (0..num_markets).step_by(SETUP_ACTIONS_BATCH) {
        let batch_end = (batch_start + SETUP_ACTIONS_BATCH).min(num_markets);
        let mut actions = Vec::with_capacity(batch_end - batch_start);
        for market_index in batch_start..batch_end {
            let base_index = market_index * 2;
            let quote_index = base_index + 1;
            let market_action = ActionBuilder::create_market(CreateMarketParams {
                name: format!("BurstMarket{}", market_index + 1).into(),
                base_token: tokens[base_index],
                quote_token: tokens[quote_index],
                min_order_size: 100_000,
                tick_size: TICK_SIZE,
                maker_fee_bps: 0,
                taker_fee_bps: 0,
                allow_market_orders: true,
                state: MarketState::Active,
                limit_order: true,
                side_book_size: SegmentSize::Large,
                creator,
                access: Default::default(),
            })
            .map_err(|e| anyhow::anyhow!("create_market action: {e}"))?;
            actions.push(market_action);
        }
        let receipt = submit_signed_actions(
            client,
            index_http,
            signer,
            actions,
            seq,
            &format!("create_markets_{batch_start}_{}", batch_end - 1),
        )
        .await?;
        seq += 1;
        let batch = extract_markets_from_events(&receipt, tokens, all_markets.len())?;
        all_markets.extend(batch);
        println!(
            "bootstrap: markets batch {batch_start}-{} ({} total)",
            batch_end - 1,
            all_markets.len()
        );
    }

    if all_markets.len() != num_markets {
        anyhow::bail!(
            "expected {num_markets} markets from creation events, got {}",
            all_markets.len()
        );
    }
    Ok(all_markets)
}

fn build_burst_senders(senders: usize, num_markets: usize) -> Vec<BurstSender> {
    (0..senders)
        .map(|index| {
            let signer = Signer::new();
            BurstSender {
                address: signer.address(),
                market_index: index % num_markets,
            }
        })
        .collect()
}

async fn fund_burst_senders(
    client: &reqwest::Client,
    index_http: &str,
    signer: &Signer,
    senders: &[BurstSender],
    markets: &[SpotMarketInfo],
) -> anyhow::Result<()> {
    if senders.is_empty() {
        return Ok(());
    }

    let mut actions = Vec::with_capacity(senders.len() * 2);
    for sender in senders {
        let market = &markets[sender.market_index];
        let base_transfer = ActionBuilder::transfer_token(
            market.base,
            TransferParams {
                to: sender.address,
                amount: FUND_PER_SENDER,
            },
        )
        .map_err(|e| anyhow::anyhow!("transfer base: {e}"))?;
        let quote_transfer = ActionBuilder::transfer_token(
            market.quote,
            TransferParams {
                to: sender.address,
                amount: FUND_PER_SENDER,
            },
        )
        .map_err(|e| anyhow::anyhow!("transfer quote: {e}"))?;
        actions.push(base_transfer);
        actions.push(quote_transfer);
    }

    println!(
        "bootstrap: funding {} senders with base+quote ({FUND_PER_SENDER} each)...",
        senders.len()
    );
    let mut seq = 10_000u64;
    for (batch_id, chunk) in actions.chunks(SETUP_ACTIONS_BATCH).enumerate() {
        submit_signed_actions(
            client,
            index_http,
            signer,
            chunk.to_vec(),
            seq,
            &format!("fund_senders_{batch_id}"),
        )
        .await?;
        seq += 1;
    }
    println!("bootstrap: funded {} senders", senders.len());
    Ok(())
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

async fn burst_inject_task(
    task_id: usize,
    index_http: String,
    senders: Arc<Vec<BurstSender>>,
    markets: Arc<Vec<SpotMarketInfo>>,
    start_index: usize,
    end_index: usize,
    depth: usize,
    move_ticks: u64,
    amount_raw: u64,
    mut mid_raw: u64,
    rate: u64,
    inflight: usize,
    stats: Arc<Stats>,
    stop: Arc<AtomicBool>,
    seq_base: u64,
    verbose: bool,
) -> anyhow::Result<()> {
    let range_size = end_index - start_index;
    if range_size == 0 {
        anyhow::bail!("task {task_id}: empty sender range");
    }
    let inflight = inflight.max(1);

    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(inflight)
        .tcp_nodelay(true)
        .build()?;
    let url = format!("{}/api/tx/inject", index_http.trim_end_matches('/'));
    println!(
        "task={task_id} inject url={url} senders {start_index}-{end_index} inflight={inflight}"
    );

    let sem = Arc::new(Semaphore::new(inflight));
    let mut joins = JoinSet::new();
    let mut rate_tokens = 0.0f64;
    let mut rate_last_refill = Instant::now();
    let mut seq = seq_base;
    let mut level_index = 0usize;
    let mut tx_count = 0u64;

    while !stop.load(Ordering::Relaxed) {
        let sender = &senders[start_index + (tx_count as usize % range_size)];
        let market = &markets[sender.market_index];
        let levels = desired_place_levels(mid_raw, depth, market.quote, market.base);
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
            sender.address,
            market.market,
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
        tx_count += 1;

        let permit = sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow::anyhow!("inflight semaphore closed"))?;
        let http = http.clone();
        let url = url.clone();
        let stats = stats.clone();
        let body = json!({ "tx": tx });
        joins.spawn(async move {
            let _permit = permit;
            match http.post(&url).json(&body).send().await {
                Ok(response) => {
                    let status = response.status().as_u16();
                    let body = response.text().await.unwrap_or_default();
                    if status == 200 {
                        stats.sent.fetch_add(1, Ordering::Relaxed);
                        if verbose {
                            println!(
                                "task={task_id} inject ok side={side:?} price={price_raw} body={}",
                                body.chars().take(80).collect::<String>()
                            );
                        }
                    } else {
                        stats.errors.fetch_add(1, Ordering::Relaxed);
                        eprintln!(
                            "task={task_id} inject status={status} body={}",
                            digest_hint(&body)
                        );
                    }
                }
                Err(error) => {
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    eprintln!("task={task_id} inject error: {error}");
                }
            }
        });

        while joins.try_join_next().is_some() {}
    }

    while joins.join_next().await.is_some() {}
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.num_markets == 0 {
        anyhow::bail!("--num-markets must be at least 1");
    }
    if args.senders == 0 {
        anyhow::bail!("--senders must be at least 1");
    }
    let tasks = args.tasks.max(1);
    if tasks > args.senders {
        anyhow::bail!(
            "--tasks ({tasks}) cannot exceed --senders ({})",
            args.senders
        );
    }
    let depth = args.depth.max(1);
    let move_ticks = args.move_ticks.max(1);

    let signer = parse_private_key(&args.private_key)?;
    let mid_raw = dollars_to_price_raw(&args.mid)?;
    let amount_raw = shares_to_amount_raw(&args.amount)?;
    let http = reqwest::Client::new();

    println!(
        "submit_repro bootstrap=/api/tx/submit burst=/api/tx/inject markets={} senders={} tasks={} inflight={} rate_per_task={} depth={} move_ticks={} (Ctrl+C to stop)",
        args.num_markets,
        args.senders,
        tasks,
        args.inflight.max(1),
        args.rate,
        depth,
        move_ticks
    );

    let tokens = create_tokens(
        &http,
        &args.index_http,
        &signer,
        args.num_markets,
        args.senders,
    )
    .await?;
    let markets = create_markets(
        &http,
        &args.index_http,
        &signer,
        &tokens,
        args.num_markets,
    )
    .await?;
    let senders = build_burst_senders(args.senders, args.num_markets);
    fund_burst_senders(&http, &args.index_http, &signer, &senders, &markets).await?;

    if let Some(first) = markets.first() {
        println!(
            "burst start first_market={} markets={} senders={} (unsigned, no receipt wait)",
            first.market,
            markets.len(),
            senders.len()
        );
    }

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

    let senders = Arc::new(senders);
    let markets = Arc::new(markets);
    let senders_per_task = args.senders / tasks;
    let remaining_senders = args.senders % tasks;

    let mut handles = Vec::new();
    for task_id in 0..tasks {
        let start_index = task_id * senders_per_task + std::cmp::min(task_id, remaining_senders);
        let end_index = start_index
            + senders_per_task
            + if task_id < remaining_senders { 1 } else { 0 };
        let stop = stop.clone();
        let stats = stats.clone();
        let seq_base = (task_id as u64).saturating_mul(1_000_000_000);
        handles.push(tokio::spawn(burst_inject_task(
            task_id,
            args.index_http.clone(),
            Arc::clone(&senders),
            Arc::clone(&markets),
            start_index,
            end_index,
            depth,
            move_ticks,
            amount_raw,
            mid_raw,
            args.rate,
            args.inflight,
            stats,
            stop,
            seq_base,
            args.verbose,
        )));
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
