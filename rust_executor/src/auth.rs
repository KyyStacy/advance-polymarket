/// auth.rs - HMAC-SHA256 Polymarket L2 authentication headers.
///
/// Polymarket CLOB v2 uses L2 auth on the /order endpoint:
///   msg       = timestamp_str + "POST" + path + body_str
///   signature = base64(HMAC-SHA256(base64decode(api_secret), msg))
///   headers   = { POLY_ADDRESS, POLY_SIGNATURE, POLY_TIMESTAMP, POLY_PASSPHRASE }
///
/// All computation is done in native Rust - no Python round-trip, no GIL.

use base64::{engine::general_purpose::URL_SAFE as B64, Engine};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct ApiCreds {
    pub api_key: String,
    /// Raw (not base64-encoded) secret bytes - decoded once at construction.
    pub secret_bytes: Vec<u8>,
    pub passphrase: String,
}

impl ApiCreds {
    /// `api_secret` may be raw or base64-encoded; we try base64 first, fall
    /// back to raw bytes so both forms work without requiring caller knowledge.
    pub fn new(api_key: &str, api_secret: &str, api_passphrase: &str) -> Self {
        let trimmed = api_secret.trim();
        let secret_bytes = base64::engine::general_purpose::URL_SAFE
            .decode(trimmed)
            .or_else(|_| base64::engine::general_purpose::STANDARD.decode(trimmed))
            .unwrap_or_else(|_| trimmed.as_bytes().to_vec());
        ApiCreds {
            api_key: api_key.to_owned(),
            secret_bytes,
            passphrase: api_passphrase.to_owned(),
        }
    }
}

/// Build the four Polymarket L2 auth headers for a POST /order request.
///
/// Returns a `HashMap<String, String>` that callers insert into reqwest headers.
pub fn build_auth_headers(
    creds: &ApiCreds,
    signer_address: &str,
    path: &str,
    body: &str,
) -> HashMap<String, String> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_secs()
        .to_string();

    // msg = timestamp + "POST" + path + body
    let mut msg = String::with_capacity(ts.len() + 4 + path.len() + body.len());
    msg.push_str(&ts);
    msg.push_str("POST");
    msg.push_str(path);
    msg.push_str(body);

    let mut mac = HmacSha256::new_from_slice(&creds.secret_bytes)
        .expect("HMAC accepts any key length");
    mac.update(msg.as_bytes());
    let sig_bytes = mac.finalize().into_bytes();
    let signature = B64.encode(sig_bytes);

    let mut headers = HashMap::with_capacity(5);
    headers.insert("POLY_ADDRESS".to_owned(), signer_address.to_owned());
    headers.insert("POLY_SIGNATURE".to_owned(), signature);
    headers.insert("POLY_TIMESTAMP".to_owned(), ts);
    headers.insert("POLY_API_KEY".to_owned(), creds.api_key.clone());
    headers.insert("POLY_PASSPHRASE".to_owned(), creds.passphrase.clone());
    headers
}
