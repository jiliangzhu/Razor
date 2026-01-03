use anyhow::Context as _;
use ethereum_types::U256;
use k256::ecdsa::{RecoveryId, SigningKey, VerifyingKey};
use sha3::Digest as _;

pub fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut h = sha3::Keccak256::new();
    h.update(data);
    let out = h.finalize();
    let mut b = [0u8; 32];
    b.copy_from_slice(&out);
    b
}

pub fn eip55_checksum_address(addr: [u8; 20]) -> String {
    let hex_lower = hex::encode(addr);
    let hash = keccak256(hex_lower.as_bytes());
    let mut out = String::with_capacity(2 + 40);
    out.push_str("0x");
    for (i, ch) in hex_lower.chars().enumerate() {
        let nibble = if i % 2 == 0 {
            (hash[i / 2] >> 4) & 0x0f
        } else {
            hash[i / 2] & 0x0f
        };
        if ch.is_ascii_alphabetic() && nibble >= 8 {
            out.push(ch.to_ascii_uppercase());
        } else {
            out.push(ch);
        }
    }
    out
}

pub fn parse_hex_32(s: &str) -> anyhow::Result<[u8; 32]> {
    let raw = s.trim().strip_prefix("0x").unwrap_or(s.trim());
    let bytes = hex::decode(raw).context("hex decode")?;
    anyhow::ensure!(
        bytes.len() == 32,
        "expected 32-byte hex, got {}",
        bytes.len()
    );
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

pub fn u256_be(x: U256) -> [u8; 32] {
    let mut out = [0u8; 32];
    x.to_big_endian(&mut out);
    out
}

pub fn address_from_signing_key(sk: &SigningKey) -> [u8; 20] {
    let vk: VerifyingKey = *sk.verifying_key();
    address_from_verifying_key(&vk)
}

pub fn address_from_verifying_key(vk: &VerifyingKey) -> [u8; 20] {
    let pubkey = vk.to_encoded_point(false);
    let bytes = pubkey.as_bytes();
    // Uncompressed SEC1 encoding: 0x04 || X(32) || Y(32)
    let hash = keccak256(&bytes[1..]);
    let mut out = [0u8; 20];
    out.copy_from_slice(&hash[12..]);
    out
}

pub fn parse_hex_20(s: &str) -> anyhow::Result<[u8; 20]> {
    let raw = s.trim().strip_prefix("0x").unwrap_or(s.trim());
    let bytes = hex::decode(raw).context("hex decode")?;
    anyhow::ensure!(
        bytes.len() == 20,
        "expected 20-byte hex, got {}",
        bytes.len()
    );
    let mut out = [0u8; 20];
    out.copy_from_slice(&bytes);
    Ok(out)
}

pub fn sig_hex_0x(sig65: &[u8; 65]) -> String {
    let mut out = String::with_capacity(2 + 130);
    out.push_str("0x");
    out.push_str(&hex::encode(sig65));
    out
}

pub fn sign_keccak256(signing_key: &SigningKey, preimage: &[u8]) -> anyhow::Result<[u8; 65]> {
    let digest = sha3::Keccak256::new_with_prefix(preimage);
    let (sig, recid) = signing_key
        .sign_digest_recoverable(digest)
        .context("sign digest recoverable")?;

    let mut out = [0u8; 65];
    out[..64].copy_from_slice(sig.to_bytes().as_slice());
    let recid_u8: u8 = recid.to_byte();
    anyhow::ensure!(recid_u8 <= RecoveryId::MAX, "invalid recid {recid_u8}");
    out[64] = 27u8 + recid_u8;
    Ok(out)
}

pub fn sign_keccak256_hex_0x(signing_key: &SigningKey, preimage: &[u8]) -> anyhow::Result<String> {
    let sig65 = sign_keccak256(signing_key, preimage)?;
    Ok(sig_hex_0x(&sig65))
}
