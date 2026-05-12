/// http_pool.rs — Persistent HTTP/2 connection pool for Polymarket CLOB.
///
/// Uses `reqwest::Client` which internally maintains a connection pool
/// (up to `pool_max_idle_per_host` idle connections, HTTP/2 multiplexing).
/// The client is created ONCE and reused across all orders — eliminates
/// TLS handshake overhead on every subsequent request (~80–200 ms saved).
///
/// Architecture:
///   - `reqwest::Client` is `Arc`-wrapped and `Clone`-able (pool is shared).
///   - `post_order_json()` is the hot-path async function. It:
///       1. Builds HMAC auth headers in Rust (no Python/GIL).
///       2. POSTs over the persistent HTTP/2 connection.
///       3. Returns the response body as a raw `String` (caller parses JSON).

use crate::auth::{build_auth_headers, ApiCreds};
use reqwest::{Client, ClientBuilder};
use std::time::Duration;

/// Shared HTTP/2 connection pool for clob.polymarket.com.
#[derive(Clone)]
pub struct HttpPool {
    client: Client,
    base_url: String,
}

impl HttpPool {
    /// Build a new pool with HTTP/2 + TLS pre-configured.
    ///
    /// `base_url` — e.g. `"https://clob.polymarket.com"`.
    /// `timeout_ms` — per-request timeout in milliseconds.
    pub fn new(base_url: &str, timeout_ms: u64) -> Result<Self, reqwest::Error> {
        let client = ClientBuilder::new()
            // HTTP/2 with adaptive flow control and PING keepalive.
            .http2_prior_knowledge()          // skip HTTP/1.1 upgrade dance
            .http2_keep_alive_interval(Duration::from_secs(10))
            .http2_keep_alive_timeout(Duration::from_secs(20))
            .http2_keep_alive_while_idle(true)
            // Connection pool: keep 2 idle connections open so burst orders
            // don't wait for a new connection.
            .pool_max_idle_per_host(2)
            .pool_idle_timeout(Duration::from_secs(90))
            // Per-request wall-clock timeout.
            .timeout(Duration::from_millis(timeout_ms))
            // Use rustls (pure Rust TLS — no OpenSSL dependency on Windows).
            .use_rustls_tls()
            .build()?;

        Ok(HttpPool {
            client,
            base_url: base_url.trim_end_matches('/').to_owned(),
        })
    }

    /// Pre-warm the connection: send a cheap HEAD request so the TLS
    /// handshake happens NOW, not on the first real order.
    pub async fn warm_up(&self) {
        let url = format!("{}/ok", self.base_url);
        let _ = self.client.get(&url).send().await;
    }

    /// POST a signed order JSON body to `/order`, returning the raw response
    /// body string.  Auth headers (HMAC, timestamp) are generated in Rust.
    ///
    /// # Arguments
    /// * `order_body` — the full JSON string to POST (pre-serialized in Python).
    /// * `creds`      — API credentials (pre-built once at startup).
    pub async fn post_order_json(
        &self,
        order_body: &str,
        creds: &ApiCreds,
    ) -> Result<String, reqwest::Error> {
        let path = "/order";
        let auth = build_auth_headers(creds, path, order_body);

        let url = format!("{}{}", self.base_url, path);
        let mut req = self.client.post(&url).body(order_body.to_owned());

        req = req.header("Content-Type", "application/json");
        for (k, v) in &auth {
            req = req.header(k.as_str(), v.as_str());
        }

        let resp = req.send().await?;
        resp.text().await
    }
}
