//! Polymarket Rust Arbitrage Bot
//!
//! Entry point and scheduler. Mirrors the behaviour of index.ts:
//!   - Validate config & build client
//!   - Initialize market monitor (balance, positions, WS)
//!   - Run check_opportunity every ~1 second
//!   - Run resting-order fill check every 2 seconds
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

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<()> {
    // ─── Tracing / Logging (→ file, not stdout) ──────────────────────────────
    //
    // The TUI dashboard owns stdout. All tracing output goes to logs/bot.log
    // so it doesn't interleave with the dashboard rendering.
    fs::create_dir_all("logs").ok();
    let log_file = fs::File::create("logs/bot.log")?;
    let (non_blocking, _guard) = tracing_appender::non_blocking(log_file);
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("RUST_LOG")
                .unwrap_or_else(|_| EnvFilter::new("polymarketrust=info")),
        )
        .with_target(false)
        .compact()
        .with_ansi(false)
        .with_writer(non_blocking)
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
    println!("Strategy     : {}", config.strategy_mode.as_str());
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
    let monitor_render = Arc::clone(&monitor);
    let monitor_merge_reconcile = Arc::clone(&monitor);
    let monitor_discovery_probe = Arc::clone(&monitor);

    // 1. Opportunity check — WS-driven with adaptive triggering + 5s REST heartbeat fallback.
    let arb_handle = tokio::spawn(async move {
        const REST_HEARTBEAT: Duration = Duration::from_millis(5_000);
        const REST_FALLBACK: Duration = Duration::from_millis(1_000);

        loop {
            let notify_opt = if let Ok(m) = monitor_arb.try_lock() {
                m.get_ws_notify()
            } else {
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            };

            if let Some(notify) = notify_opt {
                tokio::select! {
                    _ = notify.notified() => {
                        let lock_wait_start = std::time::Instant::now();
                        let mut m = monitor_arb.lock().await;
                        let lock_wait = lock_wait_start.elapsed();
                        m.record_lock_wait_latency_sample(lock_wait);
                        if m.should_run_on_ws_notify().await {
                            let started = std::time::Instant::now();
                            m.check_opportunity().await;
                            m.record_check_latency_sample(started.elapsed());
                        }
                    }
                    _ = tokio::time::sleep(REST_HEARTBEAT) => {
                        // 5s Heartbeat when WS is connected but silent
                        let lock_wait_start = std::time::Instant::now();
                        let mut m = monitor_arb.lock().await;
                        let lock_wait = lock_wait_start.elapsed();
                        let started = std::time::Instant::now();
                        m.record_lock_wait_latency_sample(lock_wait);
                        m.check_opportunity().await;
                        m.record_check_latency_sample(started.elapsed());
                    }
                }
            } else {
                // Fully disconnected fallback: poll every 1s
                let lock_wait_start = std::time::Instant::now();
                let mut m = monitor_arb.lock().await;
                let lock_wait = lock_wait_start.elapsed();
                let started = std::time::Instant::now();
                m.record_lock_wait_latency_sample(lock_wait);
                m.check_opportunity().await;
                m.record_check_latency_sample(started.elapsed());
                drop(m);
                tokio::time::sleep(REST_FALLBACK).await;
            }
        }
    });

    // 2. Resting strategy fill check every 2 seconds
    let maker_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(2_000));
        loop {
            interval.tick().await;
            if let Ok(mut m) = monitor_maker.try_lock() {
                m.check_maker_fills().await;
            }
        }
    });

    // 3. WS health check every 5 seconds
    let ws_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            if let Ok(mut m) = monitor_ws.try_lock() {
                m.check_ws_health().await;
            }
        }
    });

    // 3b. Lightweight discovery probe in degraded mode.
    let discovery_probe_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            MarketMonitor::run_discovery_probe_cycle(&monitor_discovery_probe).await;
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

    // 7. Merge reconciliation worker (config-driven)
    let merge_reconcile_secs = config.merge_reconcile_interval_secs.max(1);
    let merge_reconcile_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(merge_reconcile_secs));
        loop {
            interval.tick().await;
            MarketMonitor::run_merge_reconcile_cycle(&monitor_merge_reconcile).await;
        }
    });

    // 8. Dashboard render worker (every 250ms, decoupled from strategy hot path)
    let render_handle = tokio::spawn(async move {
        let render_interval_ms = env_u64("DASHBOARD_RENDER_INTERVAL_MS", 250);
        let mut interval = tokio::time::interval(Duration::from_millis(render_interval_ms));
        loop {
            interval.tick().await;
            if let Ok(mut m) = monitor_render.try_lock() {
                m.render_if_requested();
            }
        }
    });

    // ─── Shutdown Signal ─────────────────────────────────────────────────────
    signal::ctrl_c().await?;
    println!("\nShutdown signal received");

    // Abort all tasks
    arb_handle.abort();
    maker_handle.abort();
    ws_handle.abort();
    discovery_probe_handle.abort();
    balance_handle.abort();
    gas_handle.abort();
    claim_handle.abort();
    merge_reconcile_handle.abort();
    render_handle.abort();

    // Graceful shutdown
    {
        let mut m = monitor.lock().await;
        m.shutdown().await;
    }

    println!("Bye!");
    Ok(())
}
