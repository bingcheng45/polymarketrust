//! Polymarket Rust Arbitrage Bot
//!
//! Entry point and scheduler. Mirrors the behaviour of index.ts:
//!   - Validate config & build client
//!   - Initialize market monitor (balance, positions, WS)
//!   - Run check_opportunity every ~1 second
//!   - Run maker_fills check every 2 seconds
//!   - Run redeem_resolved_positions every 20 seconds
//!   - Run WS health check every 30 seconds
//!   - Flush stats and shutdown gracefully on SIGINT/SIGTERM

use anyhow::Result;
use polymarketrust::clob_client::ClobClient;
use polymarketrust::config::Config;
use polymarketrust::market_monitor::MarketMonitor;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::Mutex;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // ─── Tracing / Logging ────────────────────────────────────────────────────
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("RUST_LOG")
                .unwrap_or_else(|_| EnvFilter::new("polymarketrust=info")),
        )
        .with_target(false)
        .compact()
        .init();

    info!("═══════════════════════════════════════════════════════");
    info!("  POLYMARKETRUST — Polymarket Arbitrage Bot (Rust)");
    info!("═══════════════════════════════════════════════════════");

    // ─── Config ───────────────────────────────────────────────────────────────
    let config = match Config::load() {
        Ok(c) => Arc::new(c),
        Err(e) => {
            error!("Config error: {e}");
            error!("Copy .env.example to .env and fill in your credentials.");
            std::process::exit(1);
        }
    };

    info!("Market slugs : {}", config.market_slugs.join(", "));
    info!("Max trade    : {} shares", config.max_trade_size);
    info!("Min profit   : ${}", config.min_net_profit_usd);
    info!("Mock mode    : {}", config.mock_currency);
    info!("Sig type     : {}", config.signature_type);

    // ─── CLOB Client ─────────────────────────────────────────────────────────
    let client = match ClobClient::new(Arc::clone(&config)) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            error!("Failed to build CLOB client: {e}");
            std::process::exit(1);
        }
    };

    info!("Signer  : {:?}", client.signer_address());
    info!("Maker   : {:?}", client.maker_address());

    // ─── Market Monitor ──────────────────────────────────────────────────────
    let monitor = match MarketMonitor::new(Arc::clone(&config), Arc::clone(&client)).await {
        Ok(m) => Arc::new(Mutex::new(m)),
        Err(e) => {
            error!("Failed to create market monitor: {e}");
            std::process::exit(1);
        }
    };

    // Initialize (balance fetch, WS connect, market discovery)
    {
        let mut m = monitor.lock().await;
        if let Err(e) = m.initialize().await {
            error!("Initialization failed: {e}");
            std::process::exit(1);
        }
    }

    info!("Bot running — press Ctrl+C to stop");

    // ─── Scheduler ───────────────────────────────────────────────────────────

    let monitor_arb = Arc::clone(&monitor);
    let monitor_maker = Arc::clone(&monitor);
    let monitor_redeem = Arc::clone(&monitor);
    let monitor_ws = Arc::clone(&monitor);

    // 1. Opportunity check every ~1 second
    let arb_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(1_000));
        loop {
            interval.tick().await;
            let mut m = monitor_arb.lock().await;
            m.check_opportunity().await;
        }
    });

    // 2. Maker fill check every 2 seconds
    let maker_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(2_000));
        loop {
            interval.tick().await;
            let mut m = monitor_maker.lock().await;
            m.check_maker_fills().await;
        }
    });

    // 3. Redeem resolved positions every 20 seconds
    let redeem_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(20));
        loop {
            interval.tick().await;
            let mut m = monitor_redeem.lock().await;
            m.redeem_resolved_positions().await;
        }
    });

    // 4. WS health check every 30 seconds
    let ws_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            let mut m = monitor_ws.lock().await;
            m.check_ws_health().await;
        }
    });

    // ─── Shutdown Signal ─────────────────────────────────────────────────────
    signal::ctrl_c().await?;
    info!("Shutdown signal received");

    // Abort all tasks
    arb_handle.abort();
    maker_handle.abort();
    redeem_handle.abort();
    ws_handle.abort();

    // Graceful shutdown
    {
        let mut m = monitor.lock().await;
        m.shutdown().await;
    }

    info!("Bye!");
    Ok(())
}
