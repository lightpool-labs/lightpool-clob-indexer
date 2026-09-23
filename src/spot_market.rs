// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::str::FromStr;

use lightpool_sdk::{parse_token_contract, Address, ContractAddress};

pub fn normalize_spot_market_key(value: &str) -> String {
    let trimmed = value.trim();
    if let Ok(contract) = parse_token_contract(trimmed) {
        return contract.to_string();
    }

    if let Ok(address) = Address::from_str(trimmed) {
        let bytes = address.as_bytes();
        let mut contract_bytes = [0u8; ContractAddress::CONTRACT_ADDRESS_LENGTH];
        contract_bytes.copy_from_slice(&bytes[..ContractAddress::CONTRACT_ADDRESS_LENGTH]);
        return ContractAddress::from_bytes(contract_bytes).to_string();
    }

    trimmed.to_string()
}

/// Composite id for an on-chain order: `{normalized_spot_market}:{onchain_order_id}`.
pub type OnchainOrderId = String;

pub fn onchain_order_id(spot_market: &str, chain_order_id: &str) -> OnchainOrderId {
    format!(
        "{}:{}",
        normalize_spot_market_key(spot_market),
        chain_order_id
    )
}

/// Split `{spot}:{chain_order_id}` into `(spot, chain_order_id)`.
pub fn split_onchain_order_id(id: &str) -> Option<(&str, &str)> {
    id.rsplit_once(':')
}
