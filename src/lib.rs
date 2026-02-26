pub mod clob_client;
pub mod config;
pub mod dashboard;
pub mod logger;
pub mod maker_strategy;
pub mod market_monitor;
pub mod market_stats;
pub mod trade_logger;
pub mod types;
pub mod ws_client;

use std::sync::Once;

static RUSTLS_PROVIDER_INIT: Once = Once::new();

/// Rustls 0.23 requires explicitly selecting a crypto provider in some feature combos.
/// Install once process-wide before any TLS sockets are opened.
pub fn init_rustls_provider() {
    RUSTLS_PROVIDER_INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}
