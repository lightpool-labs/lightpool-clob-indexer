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

pub fn chain_order_key(spot_market: &str, chain_order_id: &str) -> String {
    format!(
        "{}:{}",
        normalize_spot_market_key(spot_market),
        chain_order_id
    )
}
