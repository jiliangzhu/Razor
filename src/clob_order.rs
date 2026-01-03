use anyhow::Context as _;
use ethereum_types::U256;
use k256::ecdsa::SigningKey;
use serde::Serialize;

use crate::eth;
use crate::types::Side;

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

const EXCHANGE_DOMAIN_NAME: &str = "Polymarket CTF Exchange";
const EXCHANGE_DOMAIN_VERSION: &str = "1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    /// Fill and Kill (IOC).
    Fak,
    /// Fill or Kill.
    #[allow(dead_code)]
    Fok,
    /// Good Till Cancelled.
    #[allow(dead_code)]
    Gtc,
}

impl OrderType {
    pub fn as_str(self) -> &'static str {
        match self {
            OrderType::Fak => "FAK",
            OrderType::Fok => "FOK",
            OrderType::Gtc => "GTC",
        }
    }
}

#[derive(Debug, Clone)]
pub struct SignedOrder {
    pub salt: u64,
    pub maker: String,
    pub signer: String,
    pub taker: String,
    pub token_id: U256,
    pub maker_amount: U256,
    pub taker_amount: U256,
    pub expiration: u64,
    pub nonce: u64,
    pub fee_rate_bps: u32,
    pub side: Side,
    pub signature_type: u8,
    pub signature: String,
}

#[derive(Debug, Serialize)]
pub struct OrderJson {
    pub salt: u64,
    pub maker: String,
    pub signer: String,
    pub taker: String,
    #[serde(rename = "tokenId")]
    pub token_id: String,
    #[serde(rename = "makerAmount")]
    pub maker_amount: String,
    #[serde(rename = "takerAmount")]
    pub taker_amount: String,
    pub expiration: String,
    pub nonce: String,
    #[serde(rename = "feeRateBps")]
    pub fee_rate_bps: String,
    pub side: String,
    #[serde(rename = "signatureType")]
    pub signature_type: u8,
    pub signature: String,
}

#[derive(Debug, Serialize)]
pub struct PostOrderBody<'a> {
    pub order: OrderJson,
    pub owner: &'a str,
    #[serde(rename = "orderType")]
    pub order_type: &'a str,
}

#[derive(Debug, Clone)]
pub struct BuildOrderParams<'a> {
    pub chain_id: u64,
    pub exchange_address: &'a str,
    pub token_id: U256,
    pub side: Side,
    pub limit_price: f64,
    pub qty: f64,
    pub min_tick_size: f64,
    pub fee_rate_bps: u32,
    pub salt: u64,
}

pub fn round_config_for_tick_size(min_tick_size: f64) -> anyhow::Result<(u32, u32)> {
    // Returns (price_digits, size_digits). Size digits are frozen at 2 in Polymarket order-utils.
    let size_digits = 2u32;
    let price_digits = if approx(min_tick_size, 0.1) {
        1
    } else if approx(min_tick_size, 0.01) {
        2
    } else if approx(min_tick_size, 0.001) {
        3
    } else if approx(min_tick_size, 0.0001) {
        4
    } else {
        anyhow::bail!("unsupported tick size {min_tick_size} (expected 0.1/0.01/0.001/0.0001)");
    };
    Ok((price_digits, size_digits))
}

fn approx(a: f64, b: f64) -> bool {
    (a - b).abs() <= 1e-12
}

fn abi_word_u64(x: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[24..].copy_from_slice(&x.to_be_bytes());
    out
}

fn abi_word_u256(x: U256) -> [u8; 32] {
    eth::u256_be(x)
}

fn abi_word_address(addr: [u8; 20]) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[12..].copy_from_slice(&addr);
    out
}

fn abi_encode(words: &[[u8; 32]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(words.len() * 32);
    for w in words {
        out.extend_from_slice(w);
    }
    out
}

fn domain_separator_exchange(chain_id: u64, verifying_contract: [u8; 20]) -> [u8; 32] {
    let type_hash = eth::keccak256(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    let name_hash = eth::keccak256(EXCHANGE_DOMAIN_NAME.as_bytes());
    let version_hash = eth::keccak256(EXCHANGE_DOMAIN_VERSION.as_bytes());
    let chain_word = abi_word_u64(chain_id);
    let vc_word = abi_word_address(verifying_contract);
    let enc = abi_encode(&[type_hash, name_hash, version_hash, chain_word, vc_word]);
    eth::keccak256(&enc)
}

#[derive(Debug, Clone)]
struct OrderForHash {
    salt: u64,
    maker: [u8; 20],
    signer: [u8; 20],
    taker: [u8; 20],
    token_id: U256,
    maker_amount: U256,
    taker_amount: U256,
    expiration: u64,
    nonce: u64,
    fee_rate_bps: u32,
    side: Side,
    signature_type: u8,
}

fn order_struct_hash(o: &OrderForHash) -> [u8; 32] {
    let type_hash = eth::keccak256(b"Order(uint256 salt,address maker,address signer,address taker,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint256 expiration,uint256 nonce,uint256 feeRateBps,uint8 side,uint8 signatureType)");

    let salt_word = abi_word_u64(o.salt);
    let maker_word = abi_word_address(o.maker);
    let signer_word = abi_word_address(o.signer);
    let taker_word = abi_word_address(o.taker);
    let token_word = abi_word_u256(o.token_id);
    let maker_amt_word = abi_word_u256(o.maker_amount);
    let taker_amt_word = abi_word_u256(o.taker_amount);
    let exp_word = abi_word_u64(o.expiration);
    let nonce_word = abi_word_u64(o.nonce);
    let fee_word = abi_word_u64(o.fee_rate_bps as u64);
    let side_word = abi_word_u64(match o.side {
        Side::Buy => 0,
        Side::Sell => 1,
    });
    let sig_type_word = abi_word_u64(o.signature_type as u64);

    let enc = abi_encode(&[
        type_hash,
        salt_word,
        maker_word,
        signer_word,
        taker_word,
        token_word,
        maker_amt_word,
        taker_amt_word,
        exp_word,
        nonce_word,
        fee_word,
        side_word,
        sig_type_word,
    ]);
    eth::keccak256(&enc)
}

fn eip712_preimage(domain_sep: [u8; 32], struct_hash: [u8; 32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + 32 + 32);
    out.extend_from_slice(b"\x19\x01");
    out.extend_from_slice(&domain_sep);
    out.extend_from_slice(&struct_hash);
    out
}

pub fn build_signed_order(
    signing_key: &SigningKey,
    p: BuildOrderParams<'_>,
) -> anyhow::Result<SignedOrder> {
    anyhow::ensure!(
        p.limit_price.is_finite() && p.limit_price > 0.0,
        "invalid price"
    );
    anyhow::ensure!(p.qty.is_finite() && p.qty > 0.0, "invalid qty");

    let (price_digits, _size_digits) = round_config_for_tick_size(p.min_tick_size)?;
    let price_scale = 10u128.pow(price_digits);
    let price_int = (p.limit_price * (price_scale as f64)).round() as u64;

    // Shares are truncated to 2 decimals (matching py-clob-client).
    let size_int = (p.qty * 100.0).floor() as u64;

    // Convert size to 1e6 decimals: size_int/100 * 1e6 = size_int*10_000
    let shares_1e6 = U256::from(size_int) * U256::from(10_000u64);

    // Cost side uses makerAmount = shares * price (both in 1e6 fixed-point).
    // makerAmount = round(size_int * price_int * 10_000 / price_scale)
    let num = (size_int as u128)
        .saturating_mul(price_int as u128)
        .saturating_mul(10_000u128);
    let maker_quote_1e6 = (num + (price_scale / 2)) / price_scale;
    let quote_1e6 = U256::from(maker_quote_1e6);

    let (maker_amount, taker_amount) = match p.side {
        Side::Buy => (quote_1e6, shares_1e6),
        Side::Sell => (shares_1e6, quote_1e6),
    };

    let maker_addr = eth::address_from_signing_key(signing_key);
    let maker_str = eth::eip55_checksum_address(maker_addr);
    let signer_str = maker_str.clone();

    let exchange_addr = eth::parse_hex_20(p.exchange_address).context("parse exchange address")?;
    let domain = domain_separator_exchange(p.chain_id, exchange_addr);

    let taker_addr = eth::parse_hex_20(ZERO_ADDRESS).expect("zero address parse");
    let for_hash = OrderForHash {
        salt: p.salt,
        maker: maker_addr,
        signer: maker_addr,
        taker: taker_addr,
        token_id: p.token_id,
        maker_amount,
        taker_amount,
        expiration: 0,
        nonce: 0,
        fee_rate_bps: p.fee_rate_bps,
        side: p.side,
        signature_type: 0,
    };
    let struct_hash = order_struct_hash(&for_hash);
    let preimage = eip712_preimage(domain, struct_hash);
    let sig = eth::sign_keccak256_hex_0x(signing_key, &preimage)?;

    Ok(SignedOrder {
        salt: p.salt,
        maker: maker_str,
        signer: signer_str,
        taker: ZERO_ADDRESS.to_string(),
        token_id: p.token_id,
        maker_amount,
        taker_amount,
        expiration: 0,
        nonce: 0,
        fee_rate_bps: p.fee_rate_bps,
        side: p.side,
        signature_type: 0,
        signature: sig,
    })
}

impl SignedOrder {
    pub fn to_order_json(&self) -> OrderJson {
        OrderJson {
            salt: self.salt,
            maker: self.maker.clone(),
            signer: self.signer.clone(),
            taker: self.taker.clone(),
            token_id: self.token_id.to_string(),
            maker_amount: self.maker_amount.to_string(),
            taker_amount: self.taker_amount.to_string(),
            expiration: self.expiration.to_string(),
            nonce: self.nonce.to_string(),
            fee_rate_bps: self.fee_rate_bps.to_string(),
            side: match self.side {
                Side::Buy => "BUY".to_string(),
                Side::Sell => "SELL".to_string(),
            },
            signature_type: self.signature_type,
            signature: self.signature.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_type_strings_are_stable() {
        assert_eq!(OrderType::Fak.as_str(), "FAK");
        assert_eq!(OrderType::Fok.as_str(), "FOK");
        assert_eq!(OrderType::Gtc.as_str(), "GTC");
    }

    #[test]
    fn maker_taker_amounts_match_reference_examples() -> anyhow::Result<()> {
        // Matches py-clob-client test vectors for tick_size=0.1, price=0.5, size=100.
        let sk_bytes: [u8; 32] =
            hex::decode("4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318")
                .unwrap()
                .try_into()
                .unwrap();
        let dummy_sk = SigningKey::from_bytes((&sk_bytes).into()).unwrap();
        let token_id = U256::from(100u64);

        let buy = build_signed_order(
            &dummy_sk,
            BuildOrderParams {
                chain_id: 80002,
                exchange_address: "0xdFE02Eb6733538f8Ea35D585af8DE5958AD99E40",
                token_id,
                side: Side::Buy,
                limit_price: 0.5,
                qty: 100.0,
                min_tick_size: 0.1,
                fee_rate_bps: 0,
                salt: 1,
            },
        )?;
        assert_eq!(buy.maker_amount.to_string(), "50000000");
        assert_eq!(buy.taker_amount.to_string(), "100000000");

        let sell = build_signed_order(
            &dummy_sk,
            BuildOrderParams {
                chain_id: 80002,
                exchange_address: "0xdFE02Eb6733538f8Ea35D585af8DE5958AD99E40",
                token_id,
                side: Side::Sell,
                limit_price: 0.5,
                qty: 100.0,
                min_tick_size: 0.1,
                fee_rate_bps: 0,
                salt: 1,
            },
        )?;
        assert_eq!(sell.maker_amount.to_string(), "100000000");
        assert_eq!(sell.taker_amount.to_string(), "50000000");

        Ok(())
    }
}
