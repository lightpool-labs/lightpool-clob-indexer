// Copyright (c) LightPool Labs
// Author: xiaoyu1998

//! Build live `PaperStakeView` for HTTP `/paper/stake` and WS `paper.stake`.

use std::str::FromStr;

use lightpool_sdk::{parse_token_contract, Address};

use crate::book_hydrate::SharedChainClient;
use crate::chain::format_token_amount;
use crate::domain::{
    PaperAccountView, PaperPositionQuery, PaperPositionStatus, PaperStakeView,
};
use crate::indexer::IndexState;
use crate::ws::models::PaperUserStateWsMessage;

fn stake_share_pct(staked: u64, total_staked: u64) -> String {
    if total_staked == 0 || staked == 0 {
        return "0".into();
    }
    let bps = (staked as u128)
        .saturating_mul(10_000)
        .checked_div(total_staked as u128)
        .unwrap_or(0);
    let whole = bps / 100;
    let frac = bps % 100;
    if frac == 0 {
        return whole.to_string();
    }
    format!("{whole}.{frac:02}")
}

fn stake_ratio_pct(staked_supply: u64, total_supply: u64) -> String {
    if total_supply == 0 || staked_supply == 0 {
        return "0".into();
    }
    let bps = (staked_supply as u128)
        .saturating_mul(10_000)
        .checked_div(total_supply as u128)
        .unwrap_or(0);
    let whole = bps / 100;
    let frac = bps % 100;
    if frac == 0 {
        return whole.to_string();
    }
    format!("{whole}.{frac:02}")
}

fn mint_rate_ppm(
    tracked_lp: i128,
    tail_hwm: u64,
    flat_threshold: u64,
    flat_rate_ppm: u64,
    tail_decay_scale: u64,
) -> u64 {
    if tracked_lp < flat_threshold as i128 {
        return flat_rate_ppm;
    }
    let s = tail_decay_scale as u128;
    let h = tail_hwm as u128;
    if s == 0 {
        return 0;
    }
    ((flat_rate_ppm as u128) * s * s / ((s + h) * (s + h))) as u64
}

fn reward_rate_label(
    tracked_lp: i128,
    tail_hwm: u64,
    flat_threshold: u64,
    flat_rate_ppm: u64,
    tail_decay_scale: u64,
) -> String {
    if flat_rate_ppm == 0 {
        return "0% / 0% / 99%".into();
    }
    let rate = mint_rate_ppm(
        tracked_lp,
        tail_hwm,
        flat_threshold,
        flat_rate_ppm,
        tail_decay_scale,
    );
    let pct = ((rate as u128) * 100 / flat_rate_ppm as u128).min(99) as u64;
    format!("0% / {pct}% / 99%")
}

fn format_i128_amount(raw: i128) -> String {
    use lightpool_sdk::TOKEN_SCALE;
    let sign = if raw < 0 { "-" } else { "" };
    let abs = raw.unsigned_abs();
    let whole = abs / TOKEN_SCALE as u128;
    let frac = abs % TOKEN_SCALE as u128;
    if frac == 0 {
        return format!("{sign}{whole}");
    }
    format!("{sign}{whole}.{frac:06}")
}

impl IndexState {
    /// Live stake + mint-curve snapshot (chain reads + indexed claim/reward totals).
    pub async fn paper_stake_view(
        &self,
        chain: &SharedChainClient,
        market_raw: &str,
        address_raw: &str,
    ) -> Option<PaperStakeView> {
        let market_raw = market_raw.trim();
        let address_raw = address_raw.trim();
        if market_raw.is_empty() || address_raw.is_empty() {
            return None;
        }
        let paper_market = parse_token_contract(market_raw).ok()?;
        let user = Address::from_str(address_raw).ok()?;

        let (paper_token, flat_threshold, flat_rate_ppm, tail_decay_scale) =
            match chain.get_paper_market(paper_market).await {
                Ok(mkt) => (
                    mkt.paper_token,
                    mkt.flat_threshold,
                    mkt.flat_rate_ppm,
                    mkt.tail_decay_scale,
                ),
                Err(err) => {
                    tracing::debug!(error = %err, %market_raw, "paper_stake_view get_mkt failed");
                    (None, 0, 0, 0)
                }
            };
        let paper_token_str = paper_token.map(|t| t.to_string()).unwrap_or_default();

        let paper_raw = if let Some(token) = paper_token {
            match chain.get_balance(user, token).await {
                Ok(bal) => bal.available,
                Err(_) => 0,
            }
        } else {
            0
        };

        let total_supply_raw = if let Some(token) = paper_token {
            match chain.get_token_info(user, token).await {
                Ok(info) => info.total_supply,
                Err(_) => 0,
            }
        } else {
            0
        };

        let (tracked_lp, tail_hwm, buyback_cash) = match chain.get_paper_lp(paper_market).await {
            Ok(lp) => (lp.tracked_lp, lp.tail_hwm, lp.buyback_cash),
            Err(_) => (0, 0, 0),
        };

        let (staked, claimable, pending_reward, reward_pool, total_staked) =
            match chain.get_paper_stake(user, paper_market).await {
                Ok(s) => (
                    s.staked,
                    s.claimable,
                    s.pending_reward,
                    s.reward_pool,
                    s.total_staked,
                ),
                Err(_) => (0, 0, 0, 0, 0),
            };

        let rewards_claimed_raw = self.paper_rewards_claimed(market_raw, address_raw);
        let total_staker_rewards_raw = self.paper_total_staker_rewards(market_raw);
        let mint_rate_raw = mint_rate_ppm(
            tracked_lp,
            tail_hwm,
            flat_threshold,
            flat_rate_ppm,
            tail_decay_scale,
        );

        Some(PaperStakeView {
            market: market_raw.to_string(),
            address: address_raw.to_string(),
            paper_token: paper_token_str,
            paper: format_token_amount(paper_raw),
            paper_raw,
            staked: format_token_amount(staked),
            staked_raw: staked,
            claimable: format_token_amount(claimable),
            claimable_raw: claimable,
            pending_reward: format_token_amount(pending_reward),
            pending_reward_raw: pending_reward,
            reward_pool: format_token_amount(reward_pool),
            reward_pool_raw: reward_pool,
            buyback_cash: format_token_amount(buyback_cash),
            buyback_cash_raw: buyback_cash,
            total_staked: format_token_amount(total_staked),
            total_staked_raw: total_staked,
            stake_share_pct: stake_share_pct(staked, total_staked),
            rewards_claimed: format_token_amount(rewards_claimed_raw),
            rewards_claimed_raw,
            total_staker_rewards: format_token_amount(total_staker_rewards_raw),
            total_staker_rewards_raw,
            reward_rate: reward_rate_label(
                tracked_lp,
                tail_hwm,
                flat_threshold,
                flat_rate_ppm,
                tail_decay_scale,
            ),
            mint_rate: format_token_amount(mint_rate_raw),
            mint_rate_raw,
            tracked_lp: format_i128_amount(tracked_lp),
            tracked_lp_raw: tracked_lp as i64,
            tail_hwm: format_token_amount(tail_hwm),
            tail_hwm_raw: tail_hwm,
            flat_threshold: format_token_amount(flat_threshold),
            flat_threshold_raw: flat_threshold,
            flat_rate_ppm,
            tail_decay_scale: format_token_amount(tail_decay_scale),
            tail_decay_scale_raw: tail_decay_scale,
            reward_token: "USDC".into(),
            total_supply: format_token_amount(total_supply_raw),
            total_supply_raw,
            staked_supply: format_token_amount(total_staked),
            staked_supply_raw: total_staked,
            stake_ratio_pct: stake_ratio_pct(total_staked, total_supply_raw),
            staking_contract: market_raw.to_string(),
        })
    }

    pub async fn paper_stake_ws(
        &self,
        chain: &SharedChainClient,
        market: &str,
        address: &str,
    ) -> Option<crate::ws::models::PaperStakeWsMessage> {
        let stake = self.paper_stake_view(chain, market, address).await?;
        let mut ts_ms = self.indexing_block_timestamp_ms();
        if ts_ms == 0 {
            ts_ms = super::index_state::wall_clock_ms();
        }
        let tip = self.paper_stake_history(market, None, 1);
        let (rev, supply, reward) = tip
            .last()
            .map(|t| {
                (
                    t.staking_revenue,
                    t.total_paper_supply,
                    t.total_staking_reward,
                )
            })
            .unwrap_or((
                stake.reward_pool_raw,
                stake.total_supply_raw,
                stake.total_staker_rewards_raw,
            ));
        Some(crate::ws::models::PaperStakeWsMessage {
            msg_type: "paper_stake".into(),
            market: stake.market.clone(),
            ts_ms,
            staking_revenue: format_token_amount(rev),
            staking_revenue_raw: rev,
            total_paper_supply: format_token_amount(supply),
            total_paper_supply_raw: supply,
            total_staking_reward: format_token_amount(reward),
            total_staking_reward_raw: reward,
            stake: Some(stake),
        })
    }

    pub async fn paper_user_state(
        &self,
        chain: &SharedChainClient,
        user: &str,
        market: &str,
    ) -> Option<PaperUserStateWsMessage> {
        let paper = self.paper_stake_view(chain, market, user).await?;
        let address = Address::from_str(user).ok()?;
        let market_addr = parse_token_contract(market).ok()?;
        let acct = match chain.get_paper_account(address, market_addr).await {
            Ok(acct) => acct,
            Err(err) => {
                tracing::warn!(error = %err, "paper user state account failed");
                lightpool_sdk::lightpool_types::call::GetPaperAccount {
                    available: 0,
                    queued: 0,
                    entry_head: 0,
                    entry_tail: 0,
                    queued_truncated: false,
                }
            }
        };
        let margin_in_use = self.sum_open_margin(user, Some(market)).await;
        let open_positions = self.count_open_positions(user, Some(market)).await;
        let available = format_token_amount(acct.available);
        let queued = format_token_amount(acct.queued);
        let margin = format_token_amount(margin_in_use);
        let equity_raw = acct
            .available
            .saturating_add(acct.queued)
            .saturating_add(margin_in_use);
        let (positions, _, _) = self
            .query_paper_positions(PaperPositionQuery {
                owner: Some(user.to_string()),
                market: Some(market.to_string()),
                status: PaperPositionStatus::Open,
                limit: 200,
                offset: 0,
            })
            .await;
        Some(PaperUserStateWsMessage {
            msg_type: "paper.user.state".into(),
            user_address: user.to_string(),
            market: market.to_string(),
            account: PaperAccountView {
                market: market.to_string(),
                address: user.to_string(),
                available,
                available_raw: acct.available,
                queued,
                queued_raw: acct.queued,
                entry_head: acct.entry_head,
                entry_tail: acct.entry_tail,
                queued_truncated: acct.queued_truncated,
                margin_in_use: margin,
                margin_in_use_raw: margin_in_use,
                open_pnl: "0".into(),
                open_pnl_raw: 0,
                equity: format_token_amount(equity_raw),
                equity_raw: equity_raw as i64,
                open_positions,
            },
            positions,
            paper,
        })
    }
}
