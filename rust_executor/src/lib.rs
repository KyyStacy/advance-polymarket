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

#[pyclass]
struct RustApiCreds {
    inner: ApiCreds,
    // Signing context pre-computed by Python
    #[pyo3(get)]
    chain_id: String,
    #[pyo3(get)]
    private_key: String,
    #[pyo3(get)]
    app_domain_ctf: String,
    #[pyo3(get)]
    app_domain_neg_risk: String,
    #[pyo3(get)]
    order_type_hash: String,
    #[pyo3(get)]
    solady_type_hash: String,
    #[pyo3(get)]
    wallet_name_hash: String,
    #[pyo3(get)]
    wallet_version_hash: String,
    #[pyo3(get)]
    wallet_salt: String,
    #[pyo3(get)]
    contents_type_suffix: String,
    #[pyo3(get)]
    signer_address: String,
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

    fn warm_up(&self, py: Python<'_>) {
        let pool = Arc::clone(&self.pool);
        py.allow_threads(|| {
            TOKIO_RT.block_on(async move { pool.warm_up().await });
        });
    }

    fn heartbeat(&self, py: Python<'_>, creds: &RustApiCreds) {
        let pool = Arc::clone(&self.pool);
        let inner_creds = creds.inner.clone();
        let signer = creds.signer_address.clone();
        py.allow_threads(|| {
            TOKIO_RT.block_on(async move { pool.heartbeat(&inner_creds, &signer).await });
        });
    }

    /// Complete HFT flow: Sign EIP-1271 order and POST in Rust
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

        // Pass signing context
        let chain_id = creds.chain_id.clone();
        let pk = creds.private_key.clone();
        let app_domain = if neg_risk { creds.app_domain_neg_risk.clone() } else { creds.app_domain_ctf.clone() };
        let type_hash = creds.order_type_hash.clone();
        let solady_hash = creds.solady_type_hash.clone();
        let w_name = creds.wallet_name_hash.clone();
        let w_version = creds.wallet_version_hash.clone();
        let w_salt = creds.wallet_salt.clone();
        let contents_suffix = creds.contents_type_suffix.clone();

        let maker = maker.to_string();
        let signer = signer.to_string();
        let token_id = token_id.to_string();
        let maker_amount = maker_amount.to_string();
        let taker_amount = taker_amount.to_string();

        let result = py.allow_threads(move || {
            // 1. Generate Order & Sign
            let (order_json, _signature) = signer::build_and_sign_order(
                &chain_id, &pk, &app_domain, &type_hash, &solady_hash,
                &w_name, &w_version, &w_salt, &contents_suffix,
                &maker, &signer, &token_id, &maker_amount, &taker_amount, side
            );
            
            // 2. Format final payload - must include deferExec+postOnly like order_to_json_v2()
            let payload = format!(
                r#"{{"order":{},"owner":"{}","orderType":"FAK","deferExec":false,"postOnly":false}}"#,
                order_json, inner_creds.api_key
            );
            let hmac_signer = creds.signer_address.clone();

            // 3. POST
            TOKIO_RT.block_on(async move { pool.post_order_json(&payload, &inner_creds, &hmac_signer).await })
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
        let chain_id = creds.chain_id.clone();
        let pk = creds.private_key.clone();
        let app_domain = if neg_risk { creds.app_domain_neg_risk.clone() } else { creds.app_domain_ctf.clone() };
        let type_hash = creds.order_type_hash.clone();
        let solady_hash = creds.solady_type_hash.clone();
        let w_name = creds.wallet_name_hash.clone();
        let w_version = creds.wallet_version_hash.clone();
        let w_salt = creds.wallet_salt.clone();
        let contents_suffix = creds.contents_type_suffix.clone();

        let maker = maker.to_string();
        let signer = signer.to_string();
        let token_id = token_id.to_string();
        let maker_amount = maker_amount.to_string();
        let taker_amount = taker_amount.to_string();

        let (order_json, _sig) = signer::build_and_sign_order(
            &chain_id, &pk, &app_domain, &type_hash, &solady_hash,
            &w_name, &w_version, &w_salt, &contents_suffix,
            &maker, &signer, &token_id, &maker_amount, &taker_amount, side
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
        let signer_addr = creds.signer_address.clone();

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
