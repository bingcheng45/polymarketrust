//! Core market orchestrator (replicates market_monitor.ts).
//!
//! Manages the full lifecycle of a Polymarket arbitrage bot:
//!   - Market discovery & rollover
//!   - Orderbook fetching (WS primary, REST fallback)
//!   - Arbitrage detection & execution (FOK or GTC batched)
//!   - Position tracking (YES/NO sizes, costs)
//!   - Hedge / sell-back logic on partial fills
//!   - Merge & redemption automation
//!   - Circuit breaker enforcement
//!   - Gas cache refresh

use crate::clob_client::ClobClient;
use crate::config::Config;
use crate::dashboard::{Dashboard, DashboardState};
use crate::logger::SessionLogger;
use crate::maker_strategy::MakerStrategy;
use crate::market_stats::MarketStatsTracker;
use crate::trade_logger::TradeLogger;
use crate::types::{
    ArbOpportunity, GammaMarket, GasCache, MarketInfo, OrderBook, Position, Side, TimeInForce,
};
use crate::ws_client::WsClient;
use anyhow::Result;
use chrono::{Local, Utc};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tracing::{debug, info, warn};


// Buffer before market expiry to stop new arbs (seconds)
const EXPIRY_BUFFER_SECS: i64 = 10;
// Merge retry interval
const MERGE_RETRY_SECS: u64 = 30;
// Position imbalance hedge cooldown (exponential up to 120s)
const MAX_HEDGE_COOLDOWN_SECS: u64 = 120;
// Retry interval when no active market is available yet.
const MARKET_DISCOVERY_RETRY_SECS: u64 = 5;
// Claimability polling fallback interval.
const CLAIM_POLL_INTERVAL_SECS: u64 = 5;

pub struct MarketMonitor {
    config: Arc<Config>,
    client: Arc<ClobClient>,
    ws_client: Option<WsClient>,
    ws_notify: Option<Arc<Notify>>,
    session_logger: Option<SessionLogger>,
    trade_logger: TradeLogger,
    stats: MarketStatsTracker,
    maker: Option<MakerStrategy>,

    // Active market
    market_info: Option<MarketInfo>,

    // Position state
    position: Position,
    session_locked_value: f64,
    session_start_balance: f64,

    // Circuit breaker
    daily_pnl: f64,
    consecutive_failures: u32,
    circuit_breaker_until: Option<Instant>,

    // Hedge cooldown (after imbalance)
    hedge_cooldown_until: Option<Instant>,
    consecutive_hedge_failures: u32,

    // Assets with open conditional-token positions (for redemption)
    active_assets: HashSet<String>,

    // Gas cache (refreshed every 30s)
    gas_cache: GasCache,
    _gas_cache_refreshed: Option<Instant>,

    // Last merge attempt
    last_merge_attempt: Option<Instant>,
    last_claim_poll: Option<Instant>,
    last_market_discovery_attempt: Option<Instant>,

    // Last time we checked for opportunity
    last_check: Option<Instant>,

    // Errors collected for session summary
    errors: Vec<String>,

    // Prevents concurrent Safe redemptions (same nonce → "already known" mempool error)
    is_redeeming: Arc<AtomicBool>,
    pending_claim_value: f64,

    // TUI dashboard
    dashboard: Dashboard,
    last_balance: Option<f64>,
    last_balance_refresh: Instant,
    last_data_source: String,
    cached_fee_rate_bps: u64,
}

impl MarketMonitor {
    pub async fn new(config: Arc<Config>, client: Arc<ClobClient>) -> Result<Self> {
        let trade_logger = TradeLogger::new().await?;
        let stats = MarketStatsTracker::load().await?;
        let maker = if config.maker_mode_enabled {
            Some(MakerStrategy::new(Arc::clone(&config)))
        } else {
            None
        };

        Ok(Self {
            config,
            client,
            ws_client: None,
            ws_notify: None,
            session_logger: None,
            trade_logger,
            stats,
            maker,
            market_info: None,
            cached_fee_rate_bps: 0,
            position: Position::default(),
            session_locked_value: 0.0,
            session_start_balance: 0.0,
            daily_pnl: 0.0,
            consecutive_failures: 0,
            circuit_breaker_until: None,
            hedge_cooldown_until: None,
            consecutive_hedge_failures: 0,
            active_assets: HashSet::new(),
            gas_cache: GasCache::default(),
            _gas_cache_refreshed: None,
            last_merge_attempt: None,
            last_claim_poll: None,
            last_market_discovery_attempt: None,
            last_check: None,
            errors: Vec::new(),
            is_redeeming: Arc::new(AtomicBool::new(false)),
            pending_claim_value: 0.0,
            dashboard: Dashboard::new(),
            last_balance: None,
            last_balance_refresh: Instant::now(),
            last_data_source: "REST".to_string(),
        })
    }

    // ─── Initialization ───────────────────────────────────────────────────────

    pub async fn initialize(&mut self) -> Result<()> {
        // 1. Fetch starting balance
        let balance = if self.config.mock_currency {
            1000.0
        } else {
            match self.client.get_balance().await {
                Ok(b) => b,
                Err(e) => {
                    warn!("Balance fetch failed: {e:#}");
                    0.0
                }
            }
        };
        self.session_start_balance = balance;
        self.last_balance = Some(balance);

        // 2. Load prior trade history to recover open positions
        self.recover_positions_from_trades().await;

        // 3. Init session logger
        let market_slug = self.config.market_slugs.join(",");
        let logger = SessionLogger::new(
            &self.config.private_key,
            self.config.signature_type,
            &market_slug,
            balance,
            self.session_locked_value,
            self.config.mock_currency,
        )
        .await?;
        self.session_logger = Some(logger);

        // 4. Find active market (retry forever until one appears)
        loop {
            self.last_market_discovery_attempt = Some(Instant::now());
            match self.find_active_market().await {
                Ok(true) => break,
                Ok(false) => {
                    self.log_action(&format!(
                        "⏳ No active market yet. Retrying in {}s...",
                        MARKET_DISCOVERY_RETRY_SECS
                    ))
                    .await;
                }
                Err(e) => {
                    warn!("Market discovery error during init: {e}");
                    self.log_action(&format!(
                        "⚠️ Market discovery failed (retrying in {}s): {}",
                        MARKET_DISCOVERY_RETRY_SECS, e
                    ))
                    .await;
                }
            }
            tokio::time::sleep(Duration::from_secs(MARKET_DISCOVERY_RETRY_SECS)).await;
        }

        // 5. Connect WebSocket if enabled
        self.connect_ws_for_active_market(false).await;

        // 6. Refresh gas cache
        self.refresh_gas_cache().await;

        // 7. Ensure CTF approvals for Gnosis Safe (SIGNATURE_TYPE=2)
        if self.config.uses_proxy() && !self.config.mock_currency {
            let rpc = "https://polygon-rpc.com";
            if let Err(e) = self.client.ensure_ctf_approvals(rpc).await {
                warn!("CTF approval check failed (continuing): {e}");
            }
        }

        self.log_action("✅ Initialization complete").await;
        if let Some(ref logger) = self.session_logger {
            logger.log("Initialization complete").await;
        }
        Ok(())
    }

    /// Try to recover position state from trade history.
    async fn recover_positions_from_trades(&mut self) {
        if let Some(ref mi) = self.market_info.clone() {
            if let Ok(trades) = self.client.get_trades(Some(&mi.condition_id)).await {
                for trade in &trades {
                    let size: f64 = trade.size.parse().unwrap_or(0.0);
                    let price: f64 = trade.price.parse().unwrap_or(0.0);
                    let cost = size * price;

                    if trade.token_id == mi.tokens.yes {
                        self.position.yes_size += size;
                        self.position.yes_cost += cost;
                        self.active_assets.insert(mi.tokens.yes.clone());
                    } else if trade.token_id == mi.tokens.no {
                        self.position.no_size += size;
                        self.position.no_cost += cost;
                        self.active_assets.insert(mi.tokens.no.clone());
                    }
                }
                if self.position.yes_size > 0.01 || self.position.no_size > 0.01 {
                    info!(
                        "Recovered position: YES={:.2} NO={:.2}",
                        self.position.yes_size, self.position.no_size
                    );
                }
            }
        }
    }

    /// Find & cache the active market from Gamma API.
    async fn find_active_market(&mut self) -> Result<bool> {
        self.log_action("🔎 Finding next active market...").await;
        for slug in &self.config.market_slugs.clone() {
            match self.client.find_active_market(slug).await {
                Ok(Some(mi)) => {
                    let fee_bps = self
                        .client
                        .get_market_fee_rate_bps(&mi.tokens.yes)
                        .await
                        .unwrap_or(0);
                    self.cached_fee_rate_bps = fee_bps;
                    
                    info!(
                        "Active market: {} (ends {}), fee={}bps, neg_risk={}",
                        mi.question,
                        mi.end_date.format("%H:%M UTC"),
                        fee_bps,
                        mi.neg_risk
                    );
                    
                    
                    let active_fee_rate = if fee_bps > 0 { self.config.clob_fee_rate } else { 0.0 };
                    let max_theoretical_fee = active_fee_rate * (0.5 * 0.5_f64).powf(self.config.clob_fee_exponent);
                    let max_fee_pct = max_theoretical_fee * 100.0;
                    
                    self.log_action(&format!(
                        "📊 CLOB feeRateBps: {} (Max fee: {:.2}% per share)",
                        fee_bps, max_fee_pct
                    )).await;
                    
                    self.market_info = Some(mi);
                    return Ok(true);
                }
                Ok(None) => {
                    warn!("No active market found for slug '{slug}'");
                }
                Err(e) => {
                    warn!("Error finding market for slug '{slug}': {e}");
                }
            }
        }
        Ok(false)
    }

    fn market_discovery_due(&self) -> bool {
        self.last_market_discovery_attempt
            .map(|t| t.elapsed().as_secs() >= MARKET_DISCOVERY_RETRY_SECS)
            .unwrap_or(true)
    }

    async fn try_find_active_market_if_due(&mut self) {
        if self.market_info.is_some() || !self.market_discovery_due() {
            return;
        }

        self.last_market_discovery_attempt = Some(Instant::now());
        match self.find_active_market().await {
            Ok(true) => {
                self.connect_ws_for_active_market(false).await;
            }
            Ok(false) => {
                self.log_action(&format!(
                    "⏳ No active market yet. Retrying in {}s...",
                    MARKET_DISCOVERY_RETRY_SECS
                ))
                .await;
            }
            Err(e) => {
                warn!("Market discovery error: {e}");
                self.log_action(&format!(
                    "⚠️ Market discovery failed (retrying in {}s): {}",
                    MARKET_DISCOVERY_RETRY_SECS, e
                ))
                .await;
            }
        }
    }

    async fn connect_ws_for_active_market(&mut self, is_reconnect: bool) {
        if !self.config.ws_enabled {
            return;
        }

        let token_ids = match self.market_info.as_ref() {
            Some(mi) => vec![mi.tokens.yes.clone(), mi.tokens.no.clone()],
            None => return,
        };

        if is_reconnect {
            self.log_action("📡 Re-initialising WebSocket for new market...")
                .await;
        }

        self.ws_client = None;
        self.ws_notify = None;

        match WsClient::connect(
            token_ids,
            self.config.poly_api_key.clone(),
            self.config.poly_api_secret.clone(),
            self.config.poly_api_passphrase.clone(),
        )
        .await
        {
            Ok((ws, notify)) => {
                self.ws_client = Some(ws);
                self.ws_notify = Some(notify);
                info!("WS client connected");
                self.log_action("📡 WebSocket orderbook feed connected").await;
            }
            Err(e) => {
                warn!("WS connect failed, using REST only: {e}");
                if is_reconnect {
                    self.log_action(&format!("⚠️ WebSocket failed to connect: {}", e))
                        .await;
                }
            }
        }
    }

    // ─── Main Opportunity Check ────────────────────────────────────────────────

    /// Returns a clone of the underlying WS Notify, if connected.
    pub fn get_ws_notify(&self) -> Option<Arc<Notify>> {
        self.ws_notify.clone()
    }

    /// Whether a WS client is connected.
    pub fn has_ws(&self) -> bool {
        self.ws_client.is_some()
    }

    pub async fn check_opportunity(&mut self) {
        // Note: rate-limiting is now controlled by the caller (main.rs)
        // via WS-driven 20ms throttle + 1s REST fallback.
        self.last_check = Some(Instant::now());

        // Process any incoming User WS events
        self.process_user_events().await;
        self.maybe_redeem_resolved_positions(false).await;

        // Circuit breaker check
        if let Some(until) = self.circuit_breaker_until {
            if Instant::now() < until {
                return;
            }
            self.circuit_breaker_until = None;
            info!("Circuit breaker expired, resuming");
            self.log_action("⚡ Circuit breaker expired, resuming").await;
        }

        // Refresh balance periodically (every 30s) for the dashboard
        if self.last_balance_refresh.elapsed().as_secs() >= 30 {
            if !self.config.mock_currency {
                if let Ok(bal) = self.client.get_balance().await {
                    self.last_balance = Some(bal);
                }
            }
            self.last_balance_refresh = Instant::now();
        }

        let mi = match self.market_info.clone() {
            Some(m) => m,
            None => {
                self.try_find_active_market_if_due().await;
                self.render_dashboard();
                return;
            }
        };

        // Market expiry check (stop new arbs 10s before expiry)
        let now_utc = Utc::now();
        let secs_to_expiry = (mi.end_date - now_utc).num_seconds();
        if secs_to_expiry <= EXPIRY_BUFFER_SECS {
            self.handle_market_rollover().await;
            return;
        }

        // Refresh gas cache if stale
        if self.gas_cache.is_stale() {
            self.refresh_gas_cache().await;
        }

        // Fetch orderbooks
        let (yes_book, no_book) = match self.fetch_books(&mi).await {
            Some(books) => books,
            None => {
                self.render_dashboard();
                return;
            }
        };

        let yes_ask = match yes_book.best_ask() {
            Some(p) => p,
            None => {
                self.render_dashboard();
                return;
            }
        };
        let no_ask = match no_book.best_ask() {
            Some(p) => p,
            None => {
                self.render_dashboard();
                return;
            }
        };

        let total_cost = yes_ask + no_ask;
        let active_fee_rate = if self.cached_fee_rate_bps > 0 { self.config.clob_fee_rate } else { 0.0 };
        let fee_yes = active_fee_rate * (yes_ask * (1.0 - yes_ask)).powf(self.config.clob_fee_exponent);
        let fee_no = active_fee_rate * (no_ask * (1.0 - no_ask)).powf(self.config.clob_fee_exponent);
        let max_fee = fee_yes.max(fee_no);
        let effective_threshold = 1.0 - max_fee;
        let net_spread = effective_threshold - total_cost;

        // ── Price tick for TUI ────────────────────────────
        {
            let now_str = Local::now().format("%I:%M:%S.%3f %p");

            let yes_liq = yes_book.best_ask_size().floor() as i64;
            let no_liq = no_book.best_ask_size().floor() as i64;

            let cost_color = if total_cost < effective_threshold {
                "\x1b[32m" // green
            } else {
                "\x1b[31m" // red
            };
            let reset = "\x1b[0m";

            let tick = format!(
                "[{}] Up: {:.2} (x{}) | Down: {:.2} (x{}) | SUM: {}{:.3}{} (net: {:.3})",
                now_str, yes_ask, yes_liq, no_ask, no_liq,
                cost_color, total_cost, reset, net_spread
            );
            self.dashboard.add_tick(tick);
        }

        // Handle position imbalance first
        if self.position.has_imbalance() {
            self.handle_imbalance(&mi, yes_ask, no_ask).await;
            self.render_dashboard();
            return;
        }

        // Trigger merge if we have a balanced position
        if self.position.mergeable_amount() >= 1.0 {
            if self.last_merge_attempt.map(|t| t.elapsed().as_secs() >= MERGE_RETRY_SECS).unwrap_or(true) {
                let amount = self.position.mergeable_amount();
                self.fire_merge(amount, &mi).await;
            }
            // Do NOT return here! The bot can continue hunting for new arbs with remaining capital!
        }

        if total_cost < effective_threshold {
            let max_leg_price = 1.0 - self.config.min_leg_price;
            if yes_ask < self.config.min_leg_price || yes_ask > max_leg_price ||
                no_ask < self.config.min_leg_price || no_ask > max_leg_price {
                self.log_action(&format!(
                    "⏭  Extreme prices skipped: UP@{:.2}, DOWN@{:.2} — near-zero liquidity on cheap leg (min: {})",
                    yes_ask, no_ask, self.config.min_leg_price
                )).await;
                self.render_dashboard();
                return;
            }

            // Record opportunity for stats
            self.stats.record_opportunity(net_spread);

            let arb_msg = format!("🚨 ARB OPPORTUNITY! Cost: {:.2}", total_cost);
            self.log_action(&arb_msg).await;
        } else {
            self.render_dashboard();
            return;
        }

        // Maker mode: post limit orders instead of taking
        if self.config.maker_mode_enabled {
            if let Some(ref mut maker) = self.maker {
                // Approximate max fee for maker tracking purposes
                let active_fee_rate = if self.cached_fee_rate_bps > 0 { self.config.clob_fee_rate } else { 0.0 };
                let fee_bps = (active_fee_rate * 10_000.0).round() as u64;
                let _ = maker.update_orders(&self.client, &mi, yes_ask, no_ask, fee_bps).await;
            }
            self.render_dashboard();
            return;
        }

        // Calculate trade size
        let size = self.calculate_size(&yes_book, &no_book, yes_ask, no_ask).await;
        if size < 1.0 {
            self.render_dashboard();
            return;
        }

        // Profitability check including gas
        let batches = (size / self.config.min_liquidity_size).ceil() as f64;
        let gas_estimate = self.gas_cache.fee_per_merge_usd * (batches + 1.0);
        
        // Fee calculation using proper exponent equation independently per leg
        let actual_size_yes = self.fee_adjust_shares(size, yes_ask);
        let actual_size_no = self.fee_adjust_shares(size, no_ask);
        let actual_payout_size = actual_size_yes.min(actual_size_no);
        
        let expected_profit = actual_payout_size * 1.0 - total_cost * size - gas_estimate;

        if expected_profit < self.config.min_net_profit_usd {
            self.render_dashboard();
            return;
        }

        let opportunity = ArbOpportunity {
            yes_price: yes_ask,
            no_price: no_ask,
            yes_token_id: mi.tokens.yes.clone(),
            no_token_id: mi.tokens.no.clone(),
            total_cost,
            spread: net_spread,
        };

        self.execute_arb(opportunity, &mi, size).await;

        self.render_dashboard();
    }

    // ─── Book Fetching ─────────────────────────────────────────────────────────

    async fn fetch_books(&mut self, mi: &MarketInfo) -> Option<(OrderBook, OrderBook)> {
        // Try WS cache first
        if let Some(ref ws) = self.ws_client {
            let yes_book = ws.get_order_book(&mi.tokens.yes).await;
            let no_book = ws.get_order_book(&mi.tokens.no).await;
            if let (Some(y), Some(n)) = (yes_book, no_book) {
                if y.best_ask().is_some() && n.best_ask().is_some() {
                    self.last_data_source = "WS".to_string();
                    return Some((y, n));
                }
            }
        }

        // REST fallback
        self.last_data_source = "REST".to_string();
        let (yes_res, no_res) = tokio::join!(
            self.client.get_order_book(&mi.tokens.yes),
            self.client.get_order_book(&mi.tokens.no)
        );

        match (yes_res, no_res) {
            (Ok(y), Ok(n)) => Some((y, n)),
            _ => None,
        }
    }

    // ─── Size Calculation ──────────────────────────────────────────────────────

    async fn calculate_size(
        &mut self,
        yes_book: &OrderBook,
        no_book: &OrderBook,
        yes_ask: f64,
        no_ask: f64,
    ) -> f64 {
        let yes_liq = yes_book.best_ask_size();
        let no_liq = no_book.best_ask_size();
        let max_liq = f64::min(yes_liq, no_liq);

        let balance = if self.config.mock_currency {
            1000.0
        } else {
            self.client.get_balance().await.unwrap_or(0.0)
        };

        let balance_size = balance / (yes_ask + no_ask);
        let mut size = self.config.max_trade_size;

        if max_liq < size {
            self.log_action(&format!("⚠️ Liquidity limited: {:.1} < {size}", max_liq)).await;
            size = max_liq;
        }

        if balance_size < size {
            self.log_action(&format!("⚠️ Balance limited size to: {:.1}", balance_size)).await;
            size = balance_size;
        }

        let active_fee_rate = if self.cached_fee_rate_bps > 0 { self.config.clob_fee_rate } else { 0.0 };
        let fee_yes = active_fee_rate * (yes_ask * (1.0 - yes_ask)).powf(self.config.clob_fee_exponent);
        let fee_no = active_fee_rate * (no_ask * (1.0 - no_ask)).powf(self.config.clob_fee_exponent);
        let max_fee = fee_yes.max(fee_no);

        let min_sellable = 5.0;
        let min_order_size = ((min_sellable + 0.5) / (1.0 - max_fee)).ceil();

        size = size.floor();

        if size < min_order_size {
            self.log_action(&format!("⏭ Size {} below minimum ({}). Skipping.", size, min_order_size)).await;
            return 0.0;
        }

        let batch_size = f64::max(self.config.min_liquidity_size, min_order_size);
        let num_batches = (size / batch_size).floor() as usize;

        if num_batches < 1 {
            self.log_action(&format!("⏭ Size {} below batch size ({}). Skipping.", size, batch_size)).await;
            return 0.0;
        }

        size = (num_batches as f64) * batch_size;
        size
    }

    // ─── Arbitrage Execution ───────────────────────────────────────────────────

    async fn execute_arb(&mut self, opp: ArbOpportunity, mi: &MarketInfo, total_size: f64) {
        let active_fee_rate = if self.cached_fee_rate_bps > 0 { self.config.clob_fee_rate } else { 0.0 };
        let fee_yes = active_fee_rate * (opp.yes_price * (1.0 - opp.yes_price)).powf(self.config.clob_fee_exponent);
        let fee_no = active_fee_rate * (opp.no_price * (1.0 - opp.no_price)).powf(self.config.clob_fee_exponent);
        let max_fee = fee_yes.max(fee_no);
        let min_sellable = 5.0;
        let min_order_size = ((min_sellable + 0.5) / (1.0 - max_fee)).ceil();
        let batch_size = f64::max(self.config.min_liquidity_size, min_order_size);
        let batches = (total_size / batch_size).floor() as usize;

        self.log_action("⚡️ Executing Arbitrage Orders...").await;

        let mut user_balance = self.last_balance.unwrap_or(0.0);
        if user_balance == 0.0 && !self.config.mock_currency {
            user_balance = self.client.get_balance().await.unwrap_or(0.0);
            self.last_balance = Some(user_balance);
        }
        self.log_action(&format!("💰 Balance: ${:.2} USDC", user_balance)).await;

        self.log_action(&format!(
            "🎯 Size: {} ({}x{}) | UP@{} DOWN@{}",
            total_size, batches, batch_size, opp.yes_price, opp.no_price
        )).await;

        let mut total_profit = 0.0_f64;
        let mut any_success = false;
        let mut failed_batches = 0;

        for batch in 0..batches {
            let this_size = if batch == batches - 1 {
                let remainder = total_size - (batch as f64 * batch_size);
                if remainder > 0.0 {
                    (remainder * 100.0).floor() / 100.0 + batch_size
                } else {
                    batch_size
                }
            } else {
                batch_size
            };

            let cheap_str = if opp.yes_price <= opp.no_price {
                format!("UP@{} (cheap first) + DOWN@{}", opp.yes_price, opp.no_price)
            } else {
                format!("DOWN@{} (cheap first) + UP@{}", opp.no_price, opp.yes_price)
            };
            self.log_action(&format!(
                "📤 Batch {}/{}: GTC {} | {} shares",
                batch + 1, batches, cheap_str, this_size
            )).await;

            match self.execute_batch(&opp, mi, this_size).await {
                Ok(profit) => {
                    total_profit += profit;
                    any_success = true;
                    self.consecutive_failures = 0;
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    if err_msg.contains("Both legs missed") {
                        self.log_action(&format!(
                            "🚫 Batch {}/{}: both legs unfilled after timeout — no position taken",
                            batch + 1, batches
                        )).await;
                    } else {
                        warn!("Batch {} failed: {}", batch + 1, err_msg);
                        self.errors.push(format!("Batch {}: {}", batch + 1, err_msg));
                    }
                    failed_batches += 1;
                    self.consecutive_failures += 1;

                    if self.consecutive_failures >= self.config.max_consecutive_failures {
                        let cooldown = Duration::from_millis(self.config.circuit_breaker_cooldown_ms);
                        self.circuit_breaker_until = Some(Instant::now() + cooldown);
                        warn!("Circuit breaker tripped after {} failures", self.consecutive_failures);
                        break;
                    }
                }
            }
        }

        if failed_batches > 0 {
            self.log_action(&format!(
                "❌ {}/{} batches cancelled (FOK) UP@{} DOWN@{}.",
                failed_batches, batches, opp.yes_price, opp.no_price
            )).await;
            if failed_batches == batches {
                self.log_action(&format!(
                    "❌ All {} batches failed to fill. FOK cancelled safely.",
                    batches
                )).await;
            }
        }

        if any_success {
            self.daily_pnl += total_profit;
            self.stats.record_execution(true, total_profit);

            if self.daily_pnl <= -self.config.max_daily_loss_usd {
                let cooldown = Duration::from_millis(self.config.circuit_breaker_cooldown_ms);
                self.circuit_breaker_until = Some(Instant::now() + cooldown);
                warn!("Daily loss limit hit: ${:.4}", self.daily_pnl);
            }
        }
    }

    async fn execute_batch(
        &mut self,
        opp: &ArbOpportunity,
        mi: &MarketInfo,
        size: f64,
    ) -> Result<f64> {
        // Apply dynamic fee adjustment per leg
        let actual_size_yes = self.fee_adjust_shares(size, opp.yes_price);
        let actual_size_no = self.fee_adjust_shares(size, opp.no_price);
        let actual_payout_size = actual_size_yes.min(actual_size_no);

        if self.config.mock_currency {
            self.log_action("🤖 [MOCK EXECUTION] Simulating batch...").await;
            let cost = (opp.yes_price + opp.no_price) * size;
            let profit = actual_payout_size - cost - self.gas_cache.fee_per_merge_usd;
            info!("[MOCK] Batch profit: ${profit:.4}");
            self.log_action(&format!(
                "🎉 [MOCK] ARB COMPLETE! Cost: ${:.2} (gas: ${:.3}). PnL: +${:.3}",
                cost, self.gas_cache.fee_per_merge_usd, profit
            )).await;
            return Ok(profit);
        }

        // Sign both legs concurrently
        let (yes_result, no_result) = tokio::join!(
            self.client.sign_order(
                &opp.yes_token_id,
                opp.yes_price,
                size,
                Side::Buy,
                TimeInForce::Gtc,
                mi.neg_risk,
                self.cached_fee_rate_bps,
            ),
            self.client.sign_order(
                &opp.no_token_id,
                opp.no_price,
                size,
                Side::Buy,
                TimeInForce::Gtc,
                mi.neg_risk,
                self.cached_fee_rate_bps,
            )
        );

        let yes_order = yes_result?;
        let no_order = no_result?;

        // Post cheaper leg first (less liquid = higher network priority)
        let (first, second) = if opp.yes_price <= opp.no_price {
            (yes_order, no_order)
        } else {
            (no_order, yes_order)
        };

        // Post both as GTC batch
        let responses = self
            .client
            .post_orders(vec![first, second], "GTC")
            .await?;

        if responses.is_empty() {
            anyhow::bail!("No responses from post_orders");
        }

        // Wait for fills
        let timeout_ms = self.config.gtc_taker_timeout_ms;
        let (yes_filled, no_filled) = self
            .poll_fills(&responses, &opp, mi, timeout_ms)
            .await;

        // Cancel any remaining open orders
        for resp in &responses {
            if !resp.order_id.is_empty() {
                let matched = resp.size_matched.as_deref().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                if matched < size * 0.99 {
                    let _ = self.client.cancel_order(&resp.order_id).await;
                }
            }
        }

        let cost = yes_filled * opp.yes_price + no_filled * opp.no_price;

        if yes_filled >= size * 0.95 && no_filled >= size * 0.95 {
            // Both legs filled — merge
            let mergeable = actual_payout_size.min(yes_filled).min(no_filled);
            self.position.yes_size += yes_filled;
            self.position.no_size += no_filled;
            self.position.yes_cost += yes_filled * opp.yes_price;
            self.position.no_cost += no_filled * opp.no_price;
            self.active_assets.insert(mi.tokens.yes.clone());
            self.active_assets.insert(mi.tokens.no.clone());

            // Fire merge in background
            let fire_merge_amount = self.position.mergeable_amount();
            self.fire_merge(fire_merge_amount, mi).await;

            let profit = mergeable * 1.0 - cost - self.gas_cache.fee_per_merge_usd;
            self.trade_logger
                .log_execution(
                    &mi.condition_id,
                    Some(&mi.question),
                    opp.yes_price,
                    opp.no_price,
                    mergeable,
                    cost,
                    self.gas_cache.fee_per_merge_usd,
                    profit,
                )
                .await;

            let fill_msg = format!(
                "✅ Filled Up={yes_filled:.2} Down={no_filled:.2} | profit=${profit:.4}"
            );
            self.log_action(&fill_msg).await;
            if let Some(ref logger) = self.session_logger {
                logger.log(&fill_msg).await;
            }

            Ok(profit)
        } else if yes_filled > 0.05 || no_filled > 0.05 {
            // Partial fill — hedge the missing leg
            self.handle_partial_fill(
                yes_filled, no_filled, opp, mi, self.cached_fee_rate_bps, cost,
            )
            .await
        } else {
            anyhow::bail!("Both legs missed (yes={yes_filled:.2} no={no_filled:.2})")
        }
    }

    /// Poll for GTC fill status until timeout.
    async fn poll_fills(
        &self,
        responses: &[crate::types::OrderResponse],
        opp: &ArbOpportunity,
        _mi: &MarketInfo,
        timeout_ms: u64,
    ) -> (f64, f64) {
        let start = Instant::now();
        let mut yes_filled = 0.0_f64;
        let mut no_filled = 0.0_f64;

        // Map order IDs to which leg they belong
        let yes_id = responses.get(0).map(|r| r.order_id.clone()).unwrap_or_default();
        let no_id = responses.get(1).map(|r| r.order_id.clone()).unwrap_or_default();

        while start.elapsed().as_millis() < timeout_ms as u128 {
            tokio::time::sleep(Duration::from_millis(200)).await;

            let yes_future = async {
                if !yes_id.is_empty() {
                    if let Ok(Some(ord)) = self.client.get_order(&yes_id).await {
                        return Some(ord.matched_f64());
                    }
                }
                None
            };

            let no_future = async {
                if !no_id.is_empty() {
                    if let Ok(Some(ord)) = self.client.get_order(&no_id).await {
                        return Some(ord.matched_f64());
                    }
                }
                None
            };

            let (yes_res, no_res) = tokio::join!(yes_future, no_future);

            if let Some(matched) = yes_res {
                yes_filled = matched;
            }
            if let Some(matched) = no_res {
                no_filled = matched;
            }

            let target = opp.yes_price.max(opp.no_price);
            if yes_filled >= target * 0.95 && no_filled >= target * 0.95 {
                break;
            }
        }

        (yes_filled, no_filled)
    }

    // ─── Partial Fill / Hedge Logic ────────────────────────────────────────────

    async fn handle_partial_fill(
        &mut self,
        yes_filled: f64,
        no_filled: f64,
        opp: &ArbOpportunity,
        mi: &MarketInfo,
        fee_rate_bps: u64,
        cost_so_far: f64,
    ) -> Result<f64> {
        if yes_filled > no_filled {
            // YES filled, NO missing — hedge by buying NO or selling YES
            let size = yes_filled - no_filled;
            self.smart_hedge(
                &opp.no_token_id,
                &opp.yes_token_id,
                size,
                opp.no_price,
                opp.yes_price,
                mi,
                fee_rate_bps,
                cost_so_far,
                false,
            )
            .await
        } else {
            // NO filled, YES missing — hedge by buying YES or selling NO
            let size = no_filled - yes_filled;
            self.smart_hedge(
                &opp.yes_token_id,
                &opp.no_token_id,
                size,
                opp.yes_price,
                opp.no_price,
                mi,
                fee_rate_bps,
                cost_so_far,
                true,
            )
            .await
        }
    }

    /// Decide whether to hedge (buy missing leg) or sell back (sell filled leg).
    async fn smart_hedge(
        &mut self,
        missing_token_id: &str,
        filled_token_id: &str,
        size: f64,
        missing_price: f64,
        filled_price: f64,
        mi: &MarketInfo,
        fee_rate_bps: u64,
        filled_cost: f64,
        filled_is_yes: bool,
    ) -> Result<f64> {
        // Estimate hedge P&L: buy missing leg and merge
        let actual_size_missing = self.fee_adjust_shares(size, missing_price);
        let actual_size_filled = self.fee_adjust_shares(size, filled_price);
        let mergeable = actual_size_missing.min(actual_size_filled);
        let hedge_pnl =
            mergeable * 1.0 - filled_cost - missing_price * size - self.gas_cache.fee_per_merge_usd;

        // Estimate sell-back P&L: sell filled leg at current bid
        // Approximate sell proceeds as filled_price * 0.95 (pessimistic)
        let sell_proceeds = filled_price * size * 0.95;
        let sell_pnl = sell_proceeds - filled_cost;

        let sold_back = sell_pnl >= hedge_pnl;

        let result = if sold_back {
            // Sell back filled leg
            let sell_order = self
                .client
                .sign_order(
                    filled_token_id,
                    filled_price * 0.99,
                    size,
                    Side::Sell,
                    TimeInForce::Fok,
                    mi.neg_risk,
                    fee_rate_bps,
                )
                .await?;
            let resp = self
                .client
                .post_order(sell_order, "FOK")
                .await?;
            let matched = resp.size_matched.as_deref().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
            let proceeds = matched * filled_price * 0.99;
            self.stats.record_sellback();
            self.trade_logger
                .log_hedge(
                    &mi.condition_id,
                    Some(&mi.question),
                    matched,
                    filled_cost,
                    proceeds - filled_cost,
                    true,
                    matched > 0.0,
                    None,
                )
                .await;
            proceeds - filled_cost
        } else {
            // Hedge: buy missing leg
            let hedge_order = self
                .client
                .sign_order(
                    missing_token_id,
                    missing_price * 1.03, // +3 ticks aggressive
                    size,
                    Side::Buy,
                    TimeInForce::Fok,
                    mi.neg_risk,
                    fee_rate_bps,
                )
                .await?;
            let resp = self.client.post_order(hedge_order, "FOK").await?;
            let matched = resp.size_matched.as_deref().unwrap_or("0").parse::<f64>().unwrap_or(0.0);

            if matched > 0.0 {
                if filled_is_yes {
                    self.position.yes_size += matched;
                    self.position.no_size += matched;
                } else {
                    self.position.no_size += matched;
                    self.position.yes_size += matched;
                }
                self.active_assets.insert(mi.tokens.yes.clone());
                self.active_assets.insert(mi.tokens.no.clone());
                let fire_amount = self.position.mergeable_amount();
                self.fire_merge(fire_amount, mi).await;
                self.stats.record_hedge();
            }

            let actual_matched_missing = self.fee_adjust_shares(matched, missing_price);
            let actual_matched_filled = self.fee_adjust_shares(matched, filled_price);
            let actual_mergeable = actual_matched_missing.min(actual_matched_filled);
            let actual_hedge_pnl = actual_mergeable * 1.0 - filled_cost - matched * missing_price - self.gas_cache.fee_per_merge_usd;
            self.trade_logger
                .log_hedge(
                    &mi.condition_id,
                    Some(&mi.question),
                    matched,
                    filled_cost + matched * missing_price,
                    actual_hedge_pnl,
                    false,
                    matched > 0.0,
                    None,
                )
                .await;
            actual_hedge_pnl
        };

        Ok(result)
    }

    // ─── Position Imbalance ────────────────────────────────────────────────────

    async fn handle_imbalance(&mut self, mi: &MarketInfo, yes_ask: f64, no_ask: f64) {
        if let Some(until) = self.hedge_cooldown_until {
            if Instant::now() < until {
                return;
            }
        }

        let yes_excess = self.position.yes_size - self.position.no_size;
        let (missing_token, missing_price, filled_is_yes) = if yes_excess > 0.0 {
            (&mi.tokens.no, no_ask, true)
        } else {
            (&mi.tokens.yes, yes_ask, false)
        };
        let size = yes_excess.abs().min(self.config.max_trade_size);
        if size < 0.01 {
            return;
        }

        info!(
            "Imbalance: YES={:.2} NO={:.2} — hedging {size:.2} of missing leg",
            self.position.yes_size, self.position.no_size
        );

        let fee_bps = self.cached_fee_rate_bps;
        match self
            .client
            .sign_order(
                missing_token,
                missing_price * 1.03,
                size,
                Side::Buy,
                TimeInForce::Fok,
                mi.neg_risk,
                fee_bps,
            )
            .await
        {
            Ok(order) => match self.client.post_order(order, "FOK").await {
                Ok(resp) => {
                    let matched = resp.size_matched.as_deref().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                    if matched > 0.0 {
                        if filled_is_yes {
                            self.position.no_size += matched;
                        } else {
                            self.position.yes_size += matched;
                        }
                        self.consecutive_hedge_failures = 0;
                        self.hedge_cooldown_until = None;
                        info!("Hedge OK: filled {matched:.2}");
                    } else {
                        self.on_hedge_failure();
                    }
                }
                Err(e) => {
                    warn!("Hedge order failed: {e}");
                    self.on_hedge_failure();
                }
            },
            Err(e) => {
                warn!("Hedge sign failed: {e}");
            }
        }
    }

    fn on_hedge_failure(&mut self) {
        self.consecutive_hedge_failures += 1;
        let cooldown_secs = (30 * self.consecutive_hedge_failures as u64).min(MAX_HEDGE_COOLDOWN_SECS);
        self.hedge_cooldown_until = Some(Instant::now() + Duration::from_secs(cooldown_secs));
        warn!(
            "Hedge failed #{} — cooldown {cooldown_secs}s",
            self.consecutive_hedge_failures
        );
    }

    // ─── Merge ─────────────────────────────────────────────────────────────────

    /// Burns YES + NO shares on-chain → releases USDC.
    /// Fire-and-forget: spawns a background task so it never blocks the hot path.
    /// Replicates TypeScript `mergeBalancedPosition()`.
    async fn fire_merge(&mut self, amount: f64, mi: &MarketInfo) {
        if amount < 0.01 {
            return;
        }

        self.last_merge_attempt = Some(Instant::now());

        // Cap to actual position
        let capped = amount
            .min(self.position.yes_size)
            .min(self.position.no_size);

        // ── Relayer delay guard ───────────────────────────────────────────────
        // Polymarket's fill relayer can take a few seconds to settle CTF tokens
        // on-chain.  If we call mergePositions before the balance appears, the
        // transaction reverts.  Check actual on-chain balances first; if zero,
        // defer and retry in MERGE_RETRY_SECS without touching in-memory state.
        if !self.config.mock_currency {
            let rpc = "https://polygon-rpc.com";
            let holder = self.client.maker_address();
            let yes_on_chain = self
                .client
                .get_ctf_balance(&mi.tokens.yes, holder, rpc)
                .await
                .unwrap_or(0.0);
            let no_on_chain = self
                .client
                .get_ctf_balance(&mi.tokens.no, holder, rpc)
                .await
                .unwrap_or(0.0);
            if yes_on_chain.min(no_on_chain) < 0.01 {
                warn!(
                    "Merge deferred: CTF tokens not yet on-chain \
                     (YES={yes_on_chain:.4}, NO={no_on_chain:.4}) — relayer delay"
                );
                return; // last_merge_attempt set; retry after MERGE_RETRY_SECS
            }
        }

        // Reduce in-memory position immediately (optimistic update)
        self.position.yes_size -= capped;
        self.position.no_size -= capped;
        self.session_locked_value += capped; // USDC value locked until merge confirms

        let condition_id = mi.condition_id.clone();
        let question = mi.question.clone();
        let neg_risk = mi.neg_risk;

        if self.config.mock_currency {
            // Mock mode — just log, no on-chain call
            info!("[MOCK] Merged {capped:.2} YES+NO → USDC");
            self.trade_logger
                .log_merge(&condition_id, Some(&question), capped, true, None)
                .await;
            self.session_locked_value -= capped;
            return;
        }

        info!("Merging {capped:.2} YES+NO → USDC (on-chain, fire-and-forget)");

        let client = Arc::clone(&self.client);
        let _trade_logger_path = "logs/trades.jsonl".to_string(); // Re-create to avoid borrow
        let _locked_ref = self.session_locked_value; // snapshot — background task won't mutate shared state

        tokio::spawn(async move {
            // Try Polygon RPC providers in order
            let rpcs = [
                "https://polygon-rpc.com",
                "https://polygon.llamarpc.com",
                "https://rpc.ankr.com/polygon",
            ];

            let mut last_err = None;
            for rpc in &rpcs {
                match client
                    .merge_positions(&condition_id, capped, neg_risk, rpc)
                    .await
                {
                    Ok(tx_hash) => {
                        info!(
                            "Merge confirmed: {:.2} shares → USDC | tx={tx_hash:?}",
                            capped
                        );
                        // Log success
                        if let Ok(logger) = crate::trade_logger::TradeLogger::new().await {
                            logger
                                .log_merge(&condition_id, Some(&question), capped, true, None)
                                .await;
                        }
                        return;
                    }
                    Err(e) => {
                        warn!("Merge failed on {rpc}: {e}");
                        last_err = Some(e);
                    }
                }
            }

            // All RPCs failed
            if let Some(e) = last_err {
                warn!("Merge exhausted all RPCs: {e}");
                if let Ok(logger) = crate::trade_logger::TradeLogger::new().await {
                    logger
                        .log_merge(
                            &condition_id,
                            Some(&question),
                            capped,
                            false,
                            Some(e.to_string()),
                        )
                        .await;
                }
            }
        });
    }

    // ─── Redemption ────────────────────────────────────────────────────────────

    /// Redeem winnings from resolved markets.
    /// Replicates TypeScript `redeemResolvedPositions()`.
    pub async fn redeem_resolved_positions(&mut self) {
        if self.config.mock_currency {
            return;
        }

        // Prevent concurrent redemptions
        if self.is_redeeming.load(Ordering::Relaxed) {
            debug!("Redemption already in progress — skipping");
            return;
        }

        // Augment active_assets from historical trades
        let holder = self.client.maker_address();
        if let Ok(trades) = self.client.get_trades(None).await {
            for trade in trades {
                self.active_assets.insert(trade.token_id);
            }
        }

        if self.active_assets.is_empty() {
            return;
        }

        let rpcs = [
            "https://polygon-rpc.com",
            "https://polygon.llamarpc.com",
            "https://rpc.ankr.com/polygon",
        ];
        let rpc = rpcs[0];

        let mut to_remove = Vec::new();
        let mut redeemable_conditions = std::collections::HashSet::new();

        self.pending_claim_value = 0.0;

        for token_id in self.active_assets.clone() {
            // 1. Check on-chain balance
            let balance = self.client.get_ctf_balance(&token_id, holder, rpc).await.unwrap_or(0.0);
            
            if balance < 0.01 {
                to_remove.push(token_id);
                continue;
            }

            // 2. Query Gamma API
            let url = format!("https://gamma-api.polymarket.com/markets?clob_token_ids={}", token_id);
            let client = reqwest::Client::new();
            if let Ok(resp) = client.get(&url).send().await {
                if let Ok(markets) = resp.json::<Vec<GammaMarket>>().await {
                    for market in markets {
                        if market.closed.unwrap_or(false) && !market.condition_id.is_empty() {
                            if !redeemable_conditions.contains(&market.condition_id) {
                                redeemable_conditions.insert(market.condition_id.clone());
                                
                                self.pending_claim_value += balance;
                                info!("Redeeming {}: pool={:.2}", market.condition_id, balance);

                                let cid = market.condition_id.clone();
                                let question = market.question.clone();
                                let is_redeeming_flag = Arc::clone(&self.is_redeeming);
                                let cl = Arc::clone(&self.client);

                                is_redeeming_flag.store(true, Ordering::Relaxed);

                                tokio::spawn(async move {
                                    for r in &rpcs {
                                        match cl.redeem_positions(&cid, r).await {
                                            Ok(tx) => {
                                                info!("Redemption confirmed: {:.2} | tx={tx:?}", balance);
                                                if let Ok(logger) = crate::trade_logger::TradeLogger::new().await {
                                                    logger.log_redemption(&cid, Some(&question), balance, balance, true, None).await;
                                                }
                                                is_redeeming_flag.store(false, Ordering::Relaxed);
                                                return;
                                            }
                                            Err(e) => warn!("Redemption failed on {r}: {e}"),
                                        }
                                    }
                                    is_redeeming_flag.store(false, Ordering::Relaxed);
                                });
                            }
                        }
                    }
                }
            }
        }

        for token in to_remove {
            self.active_assets.remove(&token);
        }
    }

    async fn maybe_redeem_resolved_positions(&mut self, force: bool) {
        if !force {
            if let Some(last_poll) = self.last_claim_poll {
                if last_poll.elapsed().as_secs() < CLAIM_POLL_INTERVAL_SECS {
                    return;
                }
            }
        }
        self.last_claim_poll = Some(Instant::now());
        self.redeem_resolved_positions().await;
    }

    // ─── Market Rollover ───────────────────────────────────────────────────────

    async fn handle_market_rollover(&mut self) {
        let old_question = self
            .market_info
            .as_ref()
            .map(|m| m.question.clone())
            .unwrap_or_default();

        self.log_action(&format!("🔄 Market expired: {old_question} — searching for next")).await;
        self.dashboard.clear_prices();
        info!("Market expired: {old_question} — searching for next market");

        // Flush stats
        self.stats.flush().await;

        // Cancel maker orders
        if let Some(ref mut maker) = self.maker {
            let _ = maker.cancel_all(&self.client).await;
        }

        // Clear active market + WS while we search for the next market.
        self.market_info = None;
        self.ws_client = None;
        self.ws_notify = None;
        self.last_market_discovery_attempt = Some(Instant::now());

        // Find next market (non-fatal if unavailable during a short gap)
        match self.find_active_market().await {
            Ok(true) => {}
            Ok(false) => {
                self.log_action(&format!(
                    "⏳ No active market yet. Waiting for next window (retry {}s)...",
                    MARKET_DISCOVERY_RETRY_SECS
                ))
                .await;
                return;
            }
            Err(e) => {
                warn!("Failed to find next market: {e}");
                self.log_action(&format!(
                    "⚠️ Failed to discover next market (retry {}s): {}",
                    MARKET_DISCOVERY_RETRY_SECS, e
                ))
                .await;
                return;
            }
        }

        if self.market_info.is_none() {
            return;
        }

        // Resubscribe WS (Hard Reconnect)
        self.connect_ws_for_active_market(true).await;

        let new_q = self.market_info.as_ref().map(|m| m.question.clone()).unwrap_or_else(|| "unknown".to_string());
        self.log_action(&format!("✅ Rolled over to: {new_q}")).await;
        info!("Rolled over to: {new_q}");
        self.render_dashboard();
    }

    // ─── Gas Cache ─────────────────────────────────────────────────────────────

    async fn refresh_gas_cache(&mut self) {
        let (gas_gwei, pol_price) = tokio::join!(
            self.client.get_gas_price_gwei(),
            self.client.get_pol_price_usd()
        );

        // Merge gas ≈ 300k gas units
        let merge_gas = 300_000_u64;
        let fee_per_merge_usd = (gas_gwei * 1e-9 * merge_gas as f64) * pol_price;

        self.gas_cache = GasCache {
            gas_price_gwei: gas_gwei,
            pol_price_usd: pol_price,
            fee_per_merge_usd,
            updated_at: Instant::now(),
        };

        debug!(
            "Gas cache: {gas_gwei:.1} Gwei, POL=${pol_price:.4}, merge≈${fee_per_merge_usd:.5}"
        );
    }

    // ─── Maker Fill Check ─────────────────────────────────────────────────────

    pub async fn check_maker_fills(&mut self) {
        if !self.config.maker_mode_enabled {
            return;
        }
        let mi = match self.market_info.clone() {
            Some(m) => m,
            None => return,
        };

        if let Some(ref mut maker) = self.maker {
            if let Ok((yes_filled, no_filled)) =
                maker.check_fills(&self.client, &mi.condition_id).await
            {
                if yes_filled > 0.01 || no_filled > 0.01 {
                    self.position.yes_size += yes_filled;
                    self.position.no_size += no_filled;
                    self.active_assets.insert(mi.tokens.yes.clone());
                    self.active_assets.insert(mi.tokens.no.clone());

                    info!("Maker fills: YES={yes_filled:.2} NO={no_filled:.2}");

                    let fire_amount = self.position.mergeable_amount();
                    if fire_amount >= 1.0 {
                        self.fire_merge(fire_amount, &mi).await;
                    }
                }
            }
        }
    }

    // ─── User WS Events Processing ────────────────────────────────────────────

    async fn process_user_events(&mut self) {
        let mut ws_client_opt = self.ws_client.take();
        let mut force_claim_scan = false;
        if let Some(ref mut ws) = ws_client_opt {
            while let Ok(msg) = ws.user_events_rx.try_recv() {
                // Peek the event_type
                if let Some(event_type) = msg.get("event_type").and_then(|v| v.as_str()) {
                    match event_type {
                        "order" => {
                            if let Ok(ev) = serde_json::from_value::<crate::types::WsOrderEvent>(msg) {
                                debug!("WS Order Event: {} | {} | {} | matched: {}", ev.id, ev.side, ev.status, ev.size_matched);
                                // The balance/position logic here could be very complex for partial fills.
                                // We are currently tracking this gracefully in our `execute_batch` HTTP loop,
                                // but we log this to confirm User WS works flawlessly.
                                // In the future, we can drop HTTP `poll_fills` entirely.
                            }
                        }
                        "trade" => {
                            if let Ok(ev) = serde_json::from_value::<crate::types::WsTradeEvent>(msg) {
                                info!("WS Trade Event: Executed {} {} shares of {} at {}", ev.side, ev.size, ev.asset_id, ev.price);
                                self.active_assets.insert(ev.asset_id);
                                // This provides ultra-low latency confirmation that our order actually matched.
                                // We rely on this stream going forward to maintain zero-query position state.
                            }
                        }
                        "balance" => {
                            // Example Polymarket future event if they add it directly.
                            debug!("WS Balance update received");
                            self.last_balance_refresh = Instant::now() - Duration::from_secs(30); // force a REST refresh
                        }
                        _ => {
                            debug!("Ignored user event: {}", event_type);
                        }
                    }
                }
            }

            while let Ok(msg) = ws.market_events_rx.try_recv() {
                if let Some(event_type) = msg.get("event_type").and_then(|v| v.as_str()) {
                    match event_type {
                        "market_resolved" => {
                            if let Ok(ev) =
                                serde_json::from_value::<crate::types::WsMarketResolvedEvent>(msg)
                            {
                                if !ev.winning_asset_id.is_empty() {
                                    self.active_assets.insert(ev.winning_asset_id.clone());
                                }
                                for token_id in &ev.asset_ids {
                                    self.active_assets.insert(token_id.clone());
                                }
                                self.last_balance_refresh =
                                    Instant::now() - Duration::from_secs(30);
                                force_claim_scan = true;
                                let q = ev
                                    .question
                                    .clone()
                                    .unwrap_or_else(|| "resolved market".to_string());
                                self.log_action(&format!(
                                    "🏁 WS market resolved: {} ({} won) — checking claimability",
                                    q, ev.winning_outcome
                                ))
                                .await;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        self.ws_client = ws_client_opt;

        if force_claim_scan {
            self.maybe_redeem_resolved_positions(true).await;
        }
    }

    // ─── WS Health Check ──────────────────────────────────────────────────────

    pub async fn check_ws_health(&mut self) {
        if !self.config.ws_enabled {
            return;
        }

        let mi = match &self.market_info {
            Some(m) => m.clone(),
            None => return,
        };

        let healthy = if let Some(ref ws) = self.ws_client {
            ws.is_healthy(&mi.tokens.yes).await && ws.is_healthy(&mi.tokens.no).await
        } else {
            false
        };

        if !healthy {
            warn!("WS unhealthy — reconnecting");
            let token_ids = vec![mi.tokens.yes.clone(), mi.tokens.no.clone()];
            match WsClient::connect(
                token_ids,
                self.config.poly_api_key.clone(),
                self.config.poly_api_secret.clone(),
                self.config.poly_api_passphrase.clone(),
            ).await {
                Ok((ws, notify)) => {
                    self.ws_client = Some(ws);
                    self.ws_notify = Some(notify);
                    info!("WS reconnected");
                    self.log_action("📡 WebSocket orderbook feed reconnected").await;
                }
                Err(e) => {
                    warn!("WS reconnect failed: {e}");
                }
            }
        }
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    pub async fn log_action(&mut self, msg: &str) {
        self.dashboard.log_action(msg);
        if let Some(ref logger) = self.session_logger {
            logger.log(msg).await;
        }
    }

    fn fee_adjust_shares(&self, ordered_size: f64, price: f64) -> f64 {
        let active_fee_rate = if self.cached_fee_rate_bps > 0 { self.config.clob_fee_rate } else { 0.0 };
        let effective_fee = active_fee_rate * (price * (1.0 - price)).powf(self.config.clob_fee_exponent);
        let actual = ordered_size * (1.0 - effective_fee);
        (actual * 100.0).floor() / 100.0
    }

    /// Render the TUI dashboard with current state.
    fn render_dashboard(&mut self) {
        let mi = self.market_info.as_ref();
        let wallet_addr = format!("{}", self.client.maker_address());
        let wallet_type = if self.config.uses_proxy() {
            "Proxy Wallet"
        } else {
            "EOA"
        };

        let cb_active = self
            .circuit_breaker_until
            .map(|t| Instant::now() < t)
            .unwrap_or(false);
        let cb_remaining = self
            .circuit_breaker_until
            .map(|t| t.saturating_duration_since(Instant::now()).as_secs() as i64)
            .unwrap_or(0);

        // Drain any queued background errors into our local list before rendering
        if let Some(ref mut ws) = self.ws_client {
            while let Ok(err) = ws.error_rx.try_recv() {
                self.errors.push(err);
            }
        }

        let state = DashboardState {
            market_slug: &self.config.market_slugs.join(","),
            market_question: mi.map(|m| m.question.as_str()),
            market_end_date: mi.map(|m| m.end_date),
            mock_mode: self.config.mock_currency,
            wallet_address: &wallet_addr,
            wallet_type,
            balance_usdc: self.last_balance,
            yes_size: self.position.yes_size,
            no_size: self.position.no_size,
            yes_cost: self.position.yes_cost,
            no_cost: self.position.no_cost,
            outcome_yes_label: "Up",
            outcome_no_label: "Down",
            session_start_balance: self.session_start_balance,
            daily_successes: self.stats.successes(),
            consecutive_failures: self.consecutive_failures,
            circuit_breaker_active: cb_active,
            circuit_breaker_remaining_secs: cb_remaining,
            data_source: &self.last_data_source,
            pending_claim_usdc: Some(self.pending_claim_value),
        };

        self.dashboard.render(&state);
    }

    // ─── Shutdown ─────────────────────────────────────────────────────────────

    pub async fn shutdown(&mut self) {
        info!("Shutting down market monitor…");

        // Flush stats
        self.stats.flush().await;

        // Cancel maker orders
        if let Some(ref mut maker) = self.maker {
            let _ = maker.cancel_all(&self.client).await;
        }

        // Close session logger
        if let Some(mut logger) = self.session_logger.take() {
            logger.close(&self.errors).await;
        }
    }

    pub fn daily_pnl(&self) -> f64 {
        self.daily_pnl
    }

    pub fn position(&self) -> &Position {
        &self.position
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PriceLevel;

    fn mock_config() -> Option<Arc<Config>> {
        dotenv::dotenv().ok();
        let mut cfg = Config::load().ok()?;
        cfg.market_slugs = vec!["test-market".to_string()];
        cfg.max_trade_size = 100.0;
        cfg.min_liquidity_size = 10.0;
        cfg.min_net_profit_usd = 1.0;
        cfg.min_leg_price = 0.05;
        cfg.mock_currency = true; // bypasses live balance calls in calculate_size
        cfg.max_daily_loss_usd = 50.0;
        cfg.max_consecutive_failures = 5;
        cfg.circuit_breaker_cooldown_ms = 300_000;
        cfg.ws_enabled = false;
        cfg.pre_sign_enabled = false;
        cfg.maker_mode_enabled = false;
        cfg.maker_spread_ticks = 2;
        cfg.gtc_taker_timeout_ms = 5_000;
        cfg.clob_fee_rate = 0.15;
        cfg.clob_fee_exponent = 2.0;
        Some(Arc::new(cfg))
    }

    async fn mock_monitor() -> Option<MarketMonitor> {
        let config = mock_config()?;

        let client = ClobClient::new(Arc::clone(&config)).await.ok()?;
        Some(MarketMonitor {
            config: Arc::clone(&config),
            client: Arc::new(client),
            ws_client: None,
            ws_notify: None,
            session_logger: None,
            trade_logger: TradeLogger::new().await.ok()?,
            stats: MarketStatsTracker::load().await.ok()?,
            maker: None,
            market_info: None,
            position: Position::default(),
            session_locked_value: 0.0,
            session_start_balance: 1000.0, // Mock balance
            daily_pnl: 0.0,
            consecutive_failures: 0,
            circuit_breaker_until: None,
            hedge_cooldown_until: None,
            consecutive_hedge_failures: 0,
            active_assets: HashSet::new(),
            gas_cache: GasCache::default(),
            _gas_cache_refreshed: None,
            last_merge_attempt: None,
            last_claim_poll: None,
            last_market_discovery_attempt: None,
            last_check: None,
            errors: Vec::new(),
            is_redeeming: Arc::new(AtomicBool::new(false)),
            pending_claim_value: 0.0,
            dashboard: Dashboard::new(),
            last_balance: Some(1000.0),
            last_balance_refresh: Instant::now(),
            last_data_source: "REST".to_string(),
            cached_fee_rate_bps: 100, // 1% basis points
        })
    }

    #[tokio::test]
    async fn test_fee_adjust_shares() {
        let Some(monitor) = mock_monitor().await else {
            return;
        };
        // active_fee_rate = 0.15
        // effective_fee = 0.15 * (p * (1-p))^2
        // If price = 0.5: effective_fee = 0.15 * (0.25)^2 = 0.15 * 0.0625 = 0.009375
        // If size = 100: actual = 100 * (1 - 0.009375) = 99.0625 -> floor to 99.06
        let res = monitor.fee_adjust_shares(100.0, 0.5);
        assert_eq!(res, 99.06);

        // If price = 0.1: effective_fee = 0.15 * (0.09)^2 = 0.15 * 0.0081 = 0.001215
        // actual = 100 * (1 - 0.001215) = 99.8785 -> floor to 99.87
        let res2 = monitor.fee_adjust_shares(100.0, 0.1);
        assert_eq!(res2, 99.87);
    }

    #[tokio::test]
    async fn test_calculate_size_liquidity_capped() {
        let Some(mut monitor) = mock_monitor().await else {
            return;
        };
        monitor.config = Arc::new(Config {
            max_trade_size: 500.0,
            ..(*monitor.config).clone()
        });

        let mut yes_book = OrderBook::default();
        yes_book.asks.push(PriceLevel { price: "0.40".to_string(), size: "50.0".to_string() });

        let mut no_book = OrderBook::default();
        no_book.asks.push(PriceLevel { price: "0.50".to_string(), size: "200.0".to_string() });

        let size = monitor.calculate_size(&yes_book, &no_book, 0.40, 0.50).await;
        // Should be capped by yes_book's liquidity of 50.0
        assert_eq!(size, 50.0);
    }

    #[tokio::test]
    async fn test_calculate_size_balance_capped() {
        let Some(mut monitor) = mock_monitor().await else {
            return;
        };
        monitor.config = Arc::new(Config {
            max_trade_size: 5000.0,
            ..(*monitor.config).clone()
        });
        
        // Mock currency is set to true in monitor.
        // The mock balance is hardcoded to 1000.0 in `calculate_size` when `mock_currency == true`.
        // Combined cost: 0.40 + 0.58 = 0.98
        // balance_size = 1000.0 / 0.98 = 1020.408
        
        let mut yes_book = OrderBook::default();
        yes_book.asks.push(PriceLevel { price: "0.40".to_string(), size: "2000.0".to_string() });

        let mut no_book = OrderBook::default();
        no_book.asks.push(PriceLevel { price: "0.58".to_string(), size: "2000.0".to_string() });

        let size = monitor.calculate_size(&yes_book, &no_book, 0.40, 0.58).await;
        assert_eq!(size, 1020.0); // Floored
    }
}
