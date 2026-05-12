/// lib.rs — PyO3 module entry point for `polymarket_hft`.
///
/// Exposes two Python classes and one helper function:
///
///   `RustApiCreds(api_key, api_secret, api_passphrase)`
///     — Pre-decoded credentials (constructed once at startup, reused per order).
///
///   `RustHttpPool(clob_host, timeout_ms=1100)`
///     — Persistent HTTP/2 connection pool.  Call `.warm_up()` at startup.
///
///   `RustHttpPool.post_order(order_json_str, creds) -> str`
///     — Blocking (GIL-releasing) POST.  Returns raw response JSON string.
///       Called via `asyncio.to_thread()` in Python so the event loop stays live.
///
/// Design rationale:
///   Python asyncio continues to own scheduling; Rust owns the fast I/O path.
///   We release the GIL with `py.allow_threads(|| {...})` so other asyncio
///   callbacks can proceed while we wait for the HTTP response.
///
///   A single shared `tokio::runtime::Runtime` is lazily initialised in
///   `TOKIO_RT` and reused across all calls — avoids spawning a new thread
///   pool per request.

mod auth;
mod http_pool;

use auth::ApiCreds;
use http_pool::HttpPool;
use once_cell::sync::Lazy;
use pyo3::prelude::*;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Shared Tokio runtime — built once, used by every `post_order` call.
static TOKIO_RT: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)       // 2 threads: one for active order, one for warmup/reconnect
        .thread_name("poly-hft")
        .enable_all()
        .build()
        .expect("failed to build Tokio runtime")
});

// ---------------------------------------------------------------------------
// Python class: RustApiCreds
// ---------------------------------------------------------------------------

/// Pre-decoded Polymarket CLOB API credentials.
///
/// Construct once per bot session:
///   ```python
///   creds = polymarket_hft.RustApiCreds(api_key, api_secret, api_passphrase)
///   ```
/// Pass the same object to every `post_order()` call — no re-decoding overhead.
#[pyclass]
struct RustApiCreds {
    inner: ApiCreds,
}

#[pymethods]
impl RustApiCreds {
    #[new]
    fn new(api_key: &str, api_secret: &str, api_passphrase: &str) -> Self {
        RustApiCreds {
            inner: ApiCreds::new(api_key, api_secret, api_passphrase),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "RustApiCreds(api_key={}...)",
            &self.inner.api_key[..8.min(self.inner.api_key.len())]
        )
    }
}

// ---------------------------------------------------------------------------
// Python class: RustHttpPool
// ---------------------------------------------------------------------------

/// Persistent HTTP/2 connection pool for clob.polymarket.com.
///
/// Usage:
///   ```python
///   pool = polymarket_hft.RustHttpPool("https://clob.polymarket.com", timeout_ms=1100)
///   await asyncio.to_thread(pool.warm_up)          # pre-warm TLS on startup
///   # ... later in hot path:
///   resp_json = await asyncio.to_thread(pool.post_order, order_json, creds)
///   ```
#[pyclass]
struct RustHttpPool {
    pool: Arc<HttpPool>,
}

#[pymethods]
impl RustHttpPool {
    #[new]
    #[pyo3(signature = (clob_host, timeout_ms = 1100))]
    fn new(clob_host: &str, timeout_ms: u64) -> PyResult<Self> {
        let pool = HttpPool::new(clob_host, timeout_ms).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "RustHttpPool init failed: {e}"
            ))
        })?;
        Ok(RustHttpPool {
            pool: Arc::new(pool),
        })
    }

    /// Pre-warm the TLS connection (blocking, call once via asyncio.to_thread).
    ///
    /// After this returns, the first real order avoids the TLS handshake
    /// latency (~80–200 ms on cold start).
    fn warm_up(&self, py: Python<'_>) {
        let pool = Arc::clone(&self.pool);
        py.allow_threads(|| {
            TOKIO_RT.block_on(async move { pool.warm_up().await });
        });
    }

    /// POST a signed order to Polymarket CLOB.
    ///
    /// # Arguments
    /// * `order_json` — full JSON body string (pre-serialized by Python caller).
    /// * `creds`      — `RustApiCreds` instance.
    ///
    /// # Returns
    /// Raw response body as a Python `str` (JSON).  The Python caller is
    /// responsible for `json.loads()` / `orjson.loads()`.
    ///
    /// # GIL behaviour
    /// Releases the GIL during the entire HTTP round-trip so the asyncio
    /// event loop can process other callbacks (feed ticks, WS messages) while
    /// we wait for the network.  This is the key latency win vs the old
    /// `asyncio.to_thread(post_order)` approach where GIL contention
    /// occasionally delayed the thread start by ~0.5–1 ms.
    fn post_order(
        &self,
        py: Python<'_>,
        order_json: &str,
        creds: &RustApiCreds,
    ) -> PyResult<String> {
        let pool = Arc::clone(&self.pool);
        let creds_inner = creds.inner.clone();
        let body = order_json.to_owned();

        let result = py.allow_threads(move || {
            TOKIO_RT.block_on(async move { pool.post_order_json(&body, &creds_inner).await })
        });

        match result {
            Ok(resp) => Ok(resp),
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "RustHttpPool.post_order error: {e}"
            ))),
        }
    }

    fn __repr__(&self) -> &'static str {
        "RustHttpPool(http2=on, rustls=on)"
    }
}

// ---------------------------------------------------------------------------
// Module registration
// ---------------------------------------------------------------------------

#[pymodule]
fn polymarket_hft(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RustApiCreds>()?;
    m.add_class::<RustHttpPool>()?;
    Ok(())
}
