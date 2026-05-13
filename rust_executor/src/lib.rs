mod auth;
mod http_pool;
mod signer;

use auth::ApiCreds;
use http_pool::HttpPool;
use once_cell::sync::Lazy;
use pyo3::prelude::*;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Shared Tokio runtime
static TOKIO_RT: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("poly-hft")
        .enable_all()
        .build()
        .expect("failed to build Tokio runtime")
});

/// Pre-computed signing context — immutable after construction.
/// Stored in Arc so sign_and_post_order clones only the pointer (~1 atomic op)
/// instead of 12+ String heap allocations per order.
#[derive(Clone)]
struct SigningContext {
    chain_id: String,
    private_key: String,
    app_domain_ctf: String,
    app_domain_neg_risk: String,
    order_type_hash: String,
    solady_type_hash: String,
    wallet_name_hash: String,
    wallet_version_hash: String,
    wallet_salt: String,
    contents_type_suffix: String,
    signer_address: String,
}

#[pyclass]
struct RustApiCreds {
    inner: ApiCreds,
    /// Pre-computed signing context (Arc-wrapped for zero-copy sharing).
    ctx: Arc<SigningContext>,
}

#[pymethods]
impl RustApiCreds {
    #[new]
    #[pyo3(signature = (api_key, api_secret, api_passphrase, chain_id, private_key, app_domain_ctf, app_domain_neg_risk, order_type_hash, solady_type_hash, wallet_name_hash, wallet_version_hash, wallet_salt, contents_type_suffix, signer_address))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        api_key: &str,
        api_secret: &str,
        api_passphrase: &str,
        chain_id: &str,
        private_key: &str,
        app_domain_ctf: &str,
        app_domain_neg_risk: &str,
        order_type_hash: &str,
        solady_type_hash: &str,
        wallet_name_hash: &str,
        wallet_version_hash: &str,
        wallet_salt: &str,
        contents_type_suffix: &str,
        signer_address: &str,
    ) -> Self {
        RustApiCreds {
            inner: ApiCreds::new(api_key, api_secret, api_passphrase),
            ctx: Arc::new(SigningContext {
                chain_id: chain_id.to_string(),
                private_key: private_key.to_string(),
                app_domain_ctf: app_domain_ctf.to_string(),
                app_domain_neg_risk: app_domain_neg_risk.to_string(),
                order_type_hash: order_type_hash.to_string(),
                solady_type_hash: solady_type_hash.to_string(),
                wallet_name_hash: wallet_name_hash.to_string(),
                wallet_version_hash: wallet_version_hash.to_string(),
                wallet_salt: wallet_salt.to_string(),
                contents_type_suffix: contents_type_suffix.to_string(),
                signer_address: signer_address.to_string(),
            }),
        }
    }
}

#[pyclass]
struct RustHttpPool {
    pool: Arc<HttpPool>,
}

#[pymethods]
impl RustHttpPool {
    #[new]
    #[pyo3(signature = (clob_host, timeout_ms = 2200))]
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

    fn warm_up(&self, py: Python<'_>) {
        let pool = Arc::clone(&self.pool);
        py.allow_threads(|| {
            TOKIO_RT.block_on(async move { pool.warm_up().await });
        });
    }

    fn heartbeat(&self, py: Python<'_>, creds: &RustApiCreds) {
        let pool = Arc::clone(&self.pool);
        let inner_creds = creds.inner.clone();
        let signer = creds.ctx.signer_address.clone();
        py.allow_threads(|| {
            TOKIO_RT.block_on(async move { pool.heartbeat(&inner_creds, &signer).await });
        });
    }

    /// Complete HFT flow: Sign EIP-1271 order and POST in Rust.
    /// Uses Arc<SigningContext> — only 1 atomic ref-count increment per call
    /// instead of 12+ String heap allocations.
    #[allow(clippy::too_many_arguments)]
    fn sign_and_post_order(
        &self,
        py: Python<'_>,
        creds: &RustApiCreds,
        maker: &str,
        signer: &str,
        token_id: &str,
        maker_amount: &str,
        taker_amount: &str,
        side: u8,
        neg_risk: bool,
    ) -> PyResult<String> {
        let pool = Arc::clone(&self.pool);
        let inner_creds = creds.inner.clone();
        let ctx = Arc::clone(&creds.ctx);

        let maker = maker.to_string();
        let signer = signer.to_string();
        let token_id = token_id.to_string();
        let maker_amount = maker_amount.to_string();
        let taker_amount = taker_amount.to_string();

        let result = py.allow_threads(move || {
            // Select domain based on neg_risk flag.
            let app_domain = if neg_risk { &ctx.app_domain_neg_risk } else { &ctx.app_domain_ctf };

            // 1. Generate Order & Sign
            let (order_json, _signature) = signer::build_and_sign_order(
                &ctx.chain_id, &ctx.private_key, app_domain, &ctx.order_type_hash,
                &ctx.solady_type_hash, &ctx.wallet_name_hash, &ctx.wallet_version_hash,
                &ctx.wallet_salt, &ctx.contents_type_suffix,
                &maker, &signer, &token_id, &maker_amount, &taker_amount, side
            );
            
            // 2. Format final payload
            let payload = format!(
                r#"{{"order":{},"owner":"{}","orderType":"FAK","deferExec":false,"postOnly":false}}"#,
                order_json, inner_creds.api_key
            );

            // 3. POST
            TOKIO_RT.block_on(async move {
                pool.post_order_json(&payload, &inner_creds, &ctx.signer_address).await
            })
        });

        match result {
            Ok(resp) => Ok(resp),
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "RustHttpPool error: {e}"
            ))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn debug_payload(
        &self,
        _py: Python<'_>,
        creds: &RustApiCreds,
        maker: &str,
        signer: &str,
        token_id: &str,
        maker_amount: &str,
        taker_amount: &str,
        side: u8,
        neg_risk: bool,
    ) -> PyResult<String> {
        let inner_creds = creds.inner.clone();
        let ctx = &creds.ctx;
        let app_domain = if neg_risk { &ctx.app_domain_neg_risk } else { &ctx.app_domain_ctf };

        let (order_json, _sig) = signer::build_and_sign_order(
            &ctx.chain_id, &ctx.private_key, app_domain, &ctx.order_type_hash,
            &ctx.solady_type_hash, &ctx.wallet_name_hash, &ctx.wallet_version_hash,
            &ctx.wallet_salt, &ctx.contents_type_suffix,
            maker, signer, token_id, maker_amount, taker_amount, side
        );
        let payload = format!(
            r#"{{"order":{},"owner":"{}","orderType":"FAK","deferExec":false,"postOnly":false}}"#,
            order_json, inner_creds.api_key
        );
        Ok(payload)
    }

    /// POST a pre-signed JSON order (used for sell orders that rely on orderbook fetching)
    fn post_order(
        &self,
        py: Python<'_>,
        order_json: &str,
        creds: &RustApiCreds,
    ) -> PyResult<String> {
        let pool = Arc::clone(&self.pool);
        let creds_inner = creds.inner.clone();
        let body = order_json.to_owned();
        let signer_addr = creds.ctx.signer_address.clone();

        let result = py.allow_threads(move || {
            TOKIO_RT.block_on(async move { pool.post_order_json(&body, &creds_inner, &signer_addr).await })
        });

        match result {
            Ok(resp) => Ok(resp),
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "RustHttpPool.post_order error: {e}"
            ))),
        }
    }
}

#[pymodule]
fn polymarket_hft(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RustApiCreds>()?;
    m.add_class::<RustHttpPool>()?;
    Ok(())
}
