//! Polymarket Rust Arbitrage Bot
//!
//! Entry point and scheduler. Mirrors the behaviour of index.ts:
//!   - Validate config & build client
//!   - Initialize market monitor (balance, positions, WS)
//!   - Run check_opportunity every ~1 second
//!   - Run maker_fills check every 2 seconds
//!   - Run WS health check every 30 seconds
//!   - Flush stats and shutdown gracefully on SIGINT/SIGTERM

use anyhow::Result;
use polymarketrust::clob_client::ClobClient;
use polymarketrust::config::Config;
use polymarketrust::market_monitor::MarketMonitor;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::Mutex;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // ─── Tracing / Logging (→ file, not stdout) ──────────────────────────────
    //
    // The TUI dashboard owns stdout. All tracing output goes to logs/bot.log
    // so it doesn't interleave with the dashboard rendering.
    fs::create_dir_all("logs").ok();
    let log_file = fs::File::create("logs/bot.log")?;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("RUST_LOG")
                .unwrap_or_else(|_| EnvFilter::new("polymarketrust=info")),
        )
        .with_target(false)
        .compact()
        .with_ansi(false)
        .with_writer(log_file)
        .init();

    // Print startup banner to stdout (visible before dashboard takes over)
    println!("═══════════════════════════════════════════════════════");
    println!("  POLYMARKETRUST — Polymarket Arbitrage Bot (Rust)");
    println!("═══════════════════════════════════════════════════════");

    // ─── Config ───────────────────────────────────────────────────────────────
    let config = match Config::load() {
        Ok(c) => Arc::new(c),
        Err(e) => {
            eprintln!("Config error: {e}");
            eprintln!("Copy .env.example to .env and fill in your credentials.");
            std::process::exit(1);
        }
    };

    println!("Market slugs : {}", config.market_slugs.join(", "));
    println!("Max trade    : {} shares", config.max_trade_size);
    println!("Min profit   : ${}", config.min_net_profit_usd);
    println!("Mock mode    : {}", config.mock_currency);
    println!("Sig type     : {}", config.signature_type);

    // ─── CLOB Client ─────────────────────────────────────────────────────────
    let client = match ClobClient::new(Arc::clone(&config)).await {
        Ok(c) => Arc::new(c),
        Err(e) => {
            eprintln!("Failed to build CLOB client: {e}");
            std::process::exit(1);
        }
    };

    println!("Signer  : {}", client.signer_address());
    println!("Maker   : {}", client.maker_address());
    println!("Initializing...");

    // ─── Market Monitor ──────────────────────────────────────────────────────
    let monitor = match MarketMonitor::new(Arc::clone(&config), Arc::clone(&client)).await {
        Ok(m) => Arc::new(Mutex::new(m)),
        Err(e) => {
            eprintln!("Failed to create market monitor: {e}");
            std::process::exit(1);
        }
    };

    // Initialize (balance fetch, WS connect, market discovery)
    {
        let mut m = monitor.lock().await;
        if let Err(e) = m.initialize().await {
            eprintln!("Initialization failed: {e}");
            std::process::exit(1);
        }
    }

    // Dashboard will clear the screen on first render

    // ─── Scheduler ───────────────────────────────────────────────────────────

    let monitor_arb = Arc::clone(&monitor);
    let monitor_maker = Arc::clone(&monitor);
    let monitor_ws = Arc::clone(&monitor);
    let monitor_balance = Arc::clone(&monitor);
    let monitor_gas = Arc::clone(&monitor);
    let monitor_claim = Arc::clone(&monitor);

    // 1. Opportunity check — WS-driven (event-based with 20ms throttle) with 5s REST heartbeat fallback
    //    Mirrors TypeScript bot: fires checkOpportunity() instantly on WS book updates.
    //    Runs REST every 5s as a heartbeat if WS is quiet.
    let arb_handle = tokio::spawn(async move {
        const WS_THROTTLE: Duration = Duration::from_millis(20);
        const REST_HEARTBEAT: Duration = Duration::from_millis(5_000);
        const REST_FALLBACK: Duration = Duration::from_millis(1_000);
        
        let mut last_check = std::time::Instant::now() - WS_THROTTLE;

        loop {
            let notify_opt = { monitor_arb.lock().await.get_ws_notify() };

            if let Some(notify) = notify_opt {
                tokio::select! {
                    _ = notify.notified() => {
                        let now = std::time::Instant::now();
                        if now.duration_since(last_check) >= WS_THROTTLE {
                            last_check = now;
                            let mut m = monitor_arb.lock().await;
                            let started = std::time::Instant::now();
                            m.check_opportunity().await;
                            m.record_check_latency_sample(started.elapsed());
                        }
                    }
                    _ = tokio::time::sleep(REST_HEARTBEAT) => {
                        // 5s Heartbeat when WS is connected but silent
                        let mut m = monitor_arb.lock().await;
                        let started = std::time::Instant::now();
                        m.check_opportunity().await;
                        m.record_check_latency_sample(started.elapsed());
                    }
                }
            } else {
                // Fully disconnected fallback: poll every 1s
                let mut m = monitor_arb.lock().await;
                let started = std::time::Instant::now();
                m.check_opportunity().await;
                m.record_check_latency_sample(started.elapsed());
                drop(m);
                tokio::time::sleep(REST_FALLBACK).await;
            }
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

    // 3. WS health check every 5 seconds
    let ws_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            let mut m = monitor_ws.lock().await;
            m.check_ws_health().await;
        }
    });

    // 4. Balance refresh worker (every 2 seconds, outside strategy hot path)
    let balance_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        loop {
            interval.tick().await;
            MarketMonitor::run_balance_refresh_cycle(&monitor_balance).await;
        }
    });

    // 5. Gas refresh worker (every 30 seconds, outside strategy hot path)
    let gas_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            MarketMonitor::run_gas_refresh_cycle(&monitor_gas).await;
        }
    });

    // 6. Claim refresh worker (every 5 seconds, outside strategy hot path)
    let claim_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            MarketMonitor::run_claim_cycle(&monitor_claim).await;
        }
    });

    // ─── Shutdown Signal ─────────────────────────────────────────────────────
    signal::ctrl_c().await?;
    println!("\nShutdown signal received");

    // Abort all tasks
    arb_handle.abort();
    maker_handle.abort();
    ws_handle.abort();
    balance_handle.abort();
    gas_handle.abort();
    claim_handle.abort();

    // Graceful shutdown
    {
        let mut m = monitor.lock().await;
        m.shutdown().await;
    }

    println!("Bye!");
    Ok(())
}
