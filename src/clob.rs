use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use base64::Engine as _;
use hmac::Mac as _;
use k256::ecdsa::SigningKey;
use serde::Deserialize;
use sha2::Sha256;

use crate::config::Config;
use crate::eth;

pub const POLY_ADDRESS: &str = "POLY_ADDRESS";
pub const POLY_SIGNATURE: &str = "POLY_SIGNATURE";
pub const POLY_TIMESTAMP: &str = "POLY_TIMESTAMP";
pub const POLY_NONCE: &str = "POLY_NONCE";
pub const POLY_API_KEY: &str = "POLY_API_KEY";
pub const POLY_PASSPHRASE: &str = "POLY_PASSPHRASE";

pub const CLOB_DOMAIN_NAME: &str = "ClobAuthDomain";
pub const CLOB_DOMAIN_VERSION: &str = "1";
pub const CLOB_AUTH_MESSAGE: &str = "This message attests that I control the given wallet";

#[derive(Clone, Debug)]
pub struct ApiCreds {
    pub api_key: String,
    pub api_secret: String,
    pub api_passphrase: String,
}

#[derive(Debug)]
pub struct ClobSigner {
    signing_key: SigningKey,
    address_bytes: [u8; 20],
    address_checksum: String,
    chain_id: u64,
}

impl ClobSigner {
    pub fn from_env(cfg: &Config) -> anyhow::Result<Self> {
        let env_name = cfg.live.private_key_env.trim();
        anyhow::ensure!(
            !env_name.is_empty(),
            "live.private_key_env must not be empty"
        );
        let pk = std::env::var(env_name).with_context(|| {
            format!("missing private key env var: {env_name} (set it for live.enabled=true)")
        })?;
        let pk32 = eth::parse_hex_32(&pk).context("parse private key")?;
        let signing_key =
            SigningKey::from_bytes((&pk32).into()).context("invalid secp256k1 private key")?;
        let address_bytes = eth::address_from_signing_key(&signing_key);
        let address_checksum = eth::eip55_checksum_address(address_bytes);
        Ok(Self {
            signing_key,
            address_bytes,
            address_checksum,
            chain_id: cfg.live.chain_id,
        })
    }

    pub fn address(&self) -> &str {
        &self.address_checksum
    }

    pub fn signing_key(&self) -> &SigningKey {
        &self.signing_key
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }
}

pub fn now_unix_s() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn abi_word_address(addr: [u8; 20]) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[12..].copy_from_slice(&addr);
    out
}

fn abi_word_u256(x: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[24..].copy_from_slice(&x.to_be_bytes());
    out
}

fn abi_encode(words: &[[u8; 32]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(words.len() * 32);
    for w in words {
        out.extend_from_slice(w);
    }
    out
}

fn domain_separator_clob(chain_id: u64) -> [u8; 32] {
    let type_hash = eth::keccak256(b"EIP712Domain(string name,string version,uint256 chainId)");
    let name_hash = eth::keccak256(CLOB_DOMAIN_NAME.as_bytes());
    let version_hash = eth::keccak256(CLOB_DOMAIN_VERSION.as_bytes());
    let chain_word = abi_word_u256(chain_id);
    let enc = abi_encode(&[type_hash, name_hash, version_hash, chain_word]);
    eth::keccak256(&enc)
}

fn clob_auth_struct_hash(address: [u8; 20], timestamp_s: u64, nonce: u64) -> [u8; 32] {
    let type_hash =
        eth::keccak256(b"ClobAuth(address address,string timestamp,uint256 nonce,string message)");
    let addr_word = abi_word_address(address);
    let ts_hash = eth::keccak256(timestamp_s.to_string().as_bytes());
    let nonce_word = abi_word_u256(nonce);
    let msg_hash = eth::keccak256(CLOB_AUTH_MESSAGE.as_bytes());
    let enc = abi_encode(&[type_hash, addr_word, ts_hash, nonce_word, msg_hash]);
    eth::keccak256(&enc)
}

fn eip712_preimage(domain_sep: [u8; 32], struct_hash: [u8; 32]) -> Vec<u8> {
    let mut preimage = Vec::with_capacity(2 + 32 + 32);
    preimage.extend_from_slice(b"\x19\x01");
    preimage.extend_from_slice(&domain_sep);
    preimage.extend_from_slice(&struct_hash);
    preimage
}

fn sign_clob_auth_message(
    signer: &ClobSigner,
    timestamp_s: u64,
    nonce: u64,
) -> anyhow::Result<String> {
    let domain = domain_separator_clob(signer.chain_id);
    let struct_hash = clob_auth_struct_hash(signer.address_bytes, timestamp_s, nonce);
    let preimage = eip712_preimage(domain, struct_hash);
    eth::sign_keccak256_hex_0x(&signer.signing_key, &preimage)
}

pub fn create_level1_headers(
    signer: &ClobSigner,
    nonce: u64,
) -> anyhow::Result<HashMap<String, String>> {
    let timestamp_s = now_unix_s();
    let signature = sign_clob_auth_message(signer, timestamp_s, nonce)?;

    let mut headers = HashMap::new();
    headers.insert(POLY_ADDRESS.to_string(), signer.address().to_string());
    headers.insert(POLY_SIGNATURE.to_string(), signature);
    headers.insert(POLY_TIMESTAMP.to_string(), timestamp_s.to_string());
    headers.insert(POLY_NONCE.to_string(), nonce.to_string());
    Ok(headers)
}

pub fn create_level2_headers(
    signer: &ClobSigner,
    creds: &ApiCreds,
    method: &str,
    request_path: &str,
    body: Option<&str>,
) -> anyhow::Result<HashMap<String, String>> {
    let timestamp_s = now_unix_s();
    let ts = timestamp_s.to_string();

    let secret = base64::engine::general_purpose::URL_SAFE
        .decode(creds.api_secret.as_bytes())
        .context("base64 decode api_secret")?;

    let mut msg = String::new();
    msg.push_str(&ts);
    msg.push_str(method);
    msg.push_str(request_path);
    if let Some(b) = body {
        if !b.is_empty() {
            msg.push_str(b);
        }
    }

    type HmacSha256 = hmac::Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(&secret).context("hmac key")?;
    mac.update(msg.as_bytes());
    let sig_bytes = mac.finalize().into_bytes();
    let sig_b64 = base64::engine::general_purpose::URL_SAFE.encode(sig_bytes);

    let mut headers = HashMap::new();
    headers.insert(POLY_ADDRESS.to_string(), signer.address().to_string());
    headers.insert(POLY_SIGNATURE.to_string(), sig_b64);
    headers.insert(POLY_TIMESTAMP.to_string(), ts);
    headers.insert(POLY_API_KEY.to_string(), creds.api_key.clone());
    headers.insert(POLY_PASSPHRASE.to_string(), creds.api_passphrase.clone());
    Ok(headers)
}

#[derive(Deserialize)]
struct ApiCredsResp {
    #[serde(rename = "apiKey")]
    api_key: String,
    #[serde(rename = "secret")]
    api_secret: String,
    #[serde(rename = "passphrase")]
    api_passphrase: String,
}

pub async fn create_or_derive_api_creds(
    cfg: &Config,
    signer: &ClobSigner,
    http: &reqwest::Client,
) -> anyhow::Result<ApiCreds> {
    let base = cfg.polymarket.clob_base.trim_end_matches('/');
    let nonce = cfg.live.api_key_nonce;

    let l1 = create_level1_headers(signer, nonce).context("create level1 headers")?;

    let create_url = format!("{base}/auth/api-key");
    let create_res = http
        .post(&create_url)
        .headers(map_to_headermap(&l1)?)
        .send()
        .await;

    if let Ok(resp) = create_res {
        if resp.status().is_success() {
            let raw: ApiCredsResp = resp.json().await.context("decode api creds")?;
            return Ok(ApiCreds {
                api_key: raw.api_key,
                api_secret: raw.api_secret,
                api_passphrase: raw.api_passphrase,
            });
        }
    }

    // Fallback: derive existing creds for this (address, nonce).
    let derive_url = format!("{base}/auth/derive-api-key");
    let resp = http
        .get(&derive_url)
        .headers(map_to_headermap(&l1)?)
        .send()
        .await
        .context("derive api creds")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("derive api creds failed: status={status} body={body}");
    }
    let raw: ApiCredsResp = resp.json().await.context("decode derived api creds")?;
    Ok(ApiCreds {
        api_key: raw.api_key,
        api_secret: raw.api_secret,
        api_passphrase: raw.api_passphrase,
    })
}

fn map_to_headermap(map: &HashMap<String, String>) -> anyhow::Result<reqwest::header::HeaderMap> {
    use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
    let mut out = HeaderMap::new();
    for (k, v) in map {
        let name = HeaderName::from_bytes(k.as_bytes()).context("invalid header name")?;
        let val = HeaderValue::from_str(v).context("invalid header value")?;
        out.insert(name, val);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_signature_matches_known_vector() -> anyhow::Result<()> {
        // Vector computed by Python reference:
        // secret_raw=b"abc"; secret_b64=urlsafe_b64encode(secret_raw)="YWJj"
        // ts="1700000000", method="POST", path="/order", body="{}"
        // msg="1700000000POST/order{}"
        // sig_b64=urlsafe_b64encode(hmac_sha256(secret_raw,msg))="MBAD1bcrB1PsSYNZemDF5QV7g_V_e2YDSAz4lgA_bAs="
        let creds = ApiCreds {
            api_key: "k".to_string(),
            api_secret: "YWJj".to_string(),
            api_passphrase: "p".to_string(),
        };

        // Build expected signature with our implementation.
        let secret = base64::engine::general_purpose::URL_SAFE
            .decode(creds.api_secret.as_bytes())
            .context("decode")?;
        type HmacSha256 = hmac::Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(&secret).context("hmac")?;
        mac.update(b"1700000000POST/order{}");
        let sig_bytes = mac.finalize().into_bytes();
        let sig_b64 = base64::engine::general_purpose::URL_SAFE.encode(sig_bytes);
        assert_eq!(sig_b64, "MBAD1bcrB1PsSYNZemDF5QV7g_V_e2YDSAz4lgA_bAs=");
        Ok(())
    }
}
