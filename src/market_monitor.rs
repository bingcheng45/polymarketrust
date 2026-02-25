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
use crate::logger::SessionLogger;
use crate::maker_strategy::MakerStrategy;
use crate::market_stats::MarketStatsTracker;
use crate::trade_logger::TradeLogger;
use crate::types::{
    ArbOpportunity, GasCache, MarketInfo, OrderBook, Position, Side, SignedOrder, TimeInForce,
};
use crate::ws_client::WsClient;
use anyhow::Result;
use chrono::Utc;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

// Hedge: post at +3 ticks above best ask
const HEDGE_TICK_OFFSET: f64 = 3.0;
// Hedge fallback: post GTC at -2 ticks below best ask
const HEDGE_FALLBACK_TICK_OFFSET: f64 = -2.0;
// Buffer before market expiry to stop new arbs (seconds)
const EXPIRY_BUFFER_SECS: i64 = 10;
// Merge retry interval
const MERGE_RETRY_SECS: u64 = 30;
// Position imbalance hedge cooldown (exponential up to 120s)
const MAX_HEDGE_COOLDOWN_SECS: u64 = 120;

pub struct MarketMonitor {
    config: Arc<Config>,
    client: Arc<ClobClient>,
    ws_client: Option<WsClient>,
    ws_rx: Option<mpsc::Receiver<(String, OrderBook)>>,
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
    gas_cache_refreshed: Option<Instant>,

    // Last merge attempt
    last_merge_attempt: Option<Instant>,

    // Last time we checked for opportunity
    last_check: Option<Instant>,

    // Errors collected for session summary
    errors: Vec<String>,

    // Prevents concurrent Safe redemptions (same nonce → "already known" mempool error)
    is_redeeming: Arc<AtomicBool>,
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
            ws_rx: None,
            session_logger: None,
            trade_logger,
            stats,
            maker,
            market_info: None,
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
            gas_cache_refreshed: None,
            last_merge_attempt: None,
            last_check: None,
            errors: Vec::new(),
            is_redeeming: Arc::new(AtomicBool::new(false)),
        })
    }

    // ─── Initialization ───────────────────────────────────────────────────────

    pub async fn initialize(&mut self) -> Result<()> {
        // 1. Fetch starting balance
        let balance = if self.config.mock_currency {
            1000.0
        } else {
            self.client.get_balance().await.unwrap_or(0.0)
        };
        self.session_start_balance = balance;

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

        // 4. Find active market
        self.find_active_market().await?;

        // 5. Connect WebSocket if enabled
        if self.config.ws_enabled {
            if let Some(ref mi) = self.market_info {
                let token_ids = vec![mi.tokens.yes.clone(), mi.tokens.no.clone()];
                match WsClient::connect(token_ids).await {
                    Ok((ws, rx)) => {
                        self.ws_client = Some(ws);
                        self.ws_rx = Some(rx);
                        info!("WS client connected");
                    }
                    Err(e) => {
                        warn!("WS connect failed, using REST only: {e}");
                    }
                }
            }
        }

        // 6. Refresh gas cache
        self.refresh_gas_cache().await;

        // 7. Ensure CTF approvals for Gnosis Safe (SIGNATURE_TYPE=2)
        if self.config.uses_proxy() && !self.config.mock_currency {
            let rpc = "https://polygon-rpc.com";
            if let Err(e) = self.client.ensure_ctf_approvals(rpc).await {
                warn!("CTF approval check failed (continuing): {e}");
            }
        }

        self.log("Initialization complete").await;
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
                        self.active_assets.insert(mi.condition_id.clone());
                    } else if trade.token_id == mi.tokens.no {
                        self.position.no_size += size;
                        self.position.no_cost += cost;
                        self.active_assets.insert(mi.condition_id.clone());
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
    async fn find_active_market(&mut self) -> Result<()> {
        for slug in &self.config.market_slugs.clone() {
            match self.client.find_active_market(slug).await {
                Ok(Some(mi)) => {
                    let fee_bps = self
                        .client
                        .get_market_fee_rate_bps(&mi.condition_id)
                        .await
                        .unwrap_or(0);
                    info!(
                        "Active market: {} (ends {}), fee={}bps, neg_risk={}",
                        mi.question,
                        mi.end_date.format("%H:%M UTC"),
                        fee_bps,
                        mi.neg_risk
                    );
                    self.market_info = Some(mi);
                    return Ok(());
                }
                Ok(None) => {
                    warn!("No active market found for slug '{slug}'");
                }
                Err(e) => {
                    warn!("Error finding market for slug '{slug}': {e}");
                }
            }
        }
        anyhow::bail!("No active market found for any configured slugs")
    }

    // ─── Main Opportunity Check ────────────────────────────────────────────────

    pub async fn check_opportunity(&mut self) {
        // Rate-limit REST calls
        if let Some(last) = self.last_check {
            if last.elapsed().as_millis() < 950 {
                return;
            }
        }
        self.last_check = Some(Instant::now());

        // Circuit breaker check
        if let Some(until) = self.circuit_breaker_until {
            if Instant::now() < until {
                return;
            }
            self.circuit_breaker_until = None;
            info!("Circuit breaker expired, resuming");
        }

        let mi = match self.market_info.clone() {
            Some(m) => m,
            None => return,
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
            None => return,
        };

        let yes_ask = match yes_book.best_ask() {
            Some(p) => p,
            None => return,
        };
        let no_ask = match no_book.best_ask() {
            Some(p) => p,
            None => return,
        };

        // Filter out near-resolved markets
        if yes_ask < self.config.min_leg_price || no_ask < self.config.min_leg_price {
            return;
        }

        // Handle position imbalance first
        if self.position.has_imbalance() {
            self.handle_imbalance(&mi, yes_ask, no_ask).await;
            return;
        }

        // Trigger merge if we have a balanced position
        if self.position.mergeable_amount() >= 1.0 {
            if self.last_merge_attempt.map(|t| t.elapsed().as_secs() >= MERGE_RETRY_SECS).unwrap_or(true) {
                let amount = self.position.mergeable_amount();
                self.fire_merge(amount, &mi).await;
            }
            return;
        }

        let total_cost = yes_ask + no_ask;
        let spread = 1.0 - total_cost;

        if spread <= 0.0 {
            return;
        }

        // Record opportunity for stats
        self.stats.record_opportunity(spread);

        let max_fee = self.config.max_fee_per_share(yes_ask, no_ask);
        let threshold = 1.0 - max_fee;

        if total_cost >= threshold {
            return; // Not profitable after fees
        }

        // Maker mode: post limit orders instead of taking
        if self.config.maker_mode_enabled {
            if let Some(ref mut maker) = self.maker {
                let fee_bps = (max_fee * 10_000.0).round() as u64;
                let _ = maker.update_orders(&self.client, &mi, yes_ask, no_ask, fee_bps).await;
            }
            return;
        }

        // Calculate trade size
        let size = self.calculate_size(&yes_book, &no_book, yes_ask, no_ask).await;
        if size < 1.0 {
            return;
        }

        // Profitability check including gas
        let fee_per_share = max_fee;
        let batches = (size / self.config.min_liquidity_size).ceil() as f64;
        let gas_estimate = self.gas_cache.fee_per_merge_usd * (batches + 1.0);
        let mergeable_per_unit = (1.0 - fee_per_share).max(0.0);
        let expected_profit = size * mergeable_per_unit * 1.0 - total_cost * size - gas_estimate;

        if expected_profit < self.config.min_net_profit_usd {
            return;
        }

        let opportunity = ArbOpportunity {
            yes_price: yes_ask,
            no_price: no_ask,
            yes_token_id: mi.tokens.yes.clone(),
            no_token_id: mi.tokens.no.clone(),
            total_cost,
            spread,
        };

        self.execute_arb(opportunity, &mi, size).await;
    }

    // ─── Book Fetching ─────────────────────────────────────────────────────────

    async fn fetch_books(&self, mi: &MarketInfo) -> Option<(OrderBook, OrderBook)> {
        // Try WS cache first
        if let Some(ref ws) = self.ws_client {
            let yes_book = ws.get_order_book(&mi.tokens.yes).await;
            let no_book = ws.get_order_book(&mi.tokens.no).await;
            if let (Some(y), Some(n)) = (yes_book, no_book) {
                if y.best_ask().is_some() && n.best_ask().is_some() {
                    return Some((y, n));
                }
            }
        }

        // REST fallback
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
        &self,
        yes_book: &OrderBook,
        no_book: &OrderBook,
        yes_ask: f64,
        no_ask: f64,
    ) -> f64 {
        let yes_liq = yes_book.ask_liquidity_at(yes_ask + 0.005);
        let no_liq = no_book.ask_liquidity_at(no_ask + 0.005);
        let max_liq = f64::min(yes_liq, no_liq);

        let balance = if self.config.mock_currency {
            1000.0
        } else {
            self.client.get_balance().await.unwrap_or(0.0)
        };

        let balance_size = balance / (yes_ask + no_ask);
        let mut size = self.config.max_trade_size.min(max_liq).min(balance_size);

        // Ensure Polymarket $1 minimum order value
        let min_size_for_min_order = 1.0 / yes_ask.max(no_ask);
        if size < min_size_for_min_order {
            return 0.0;
        }

        // Floor to 2 decimal places
        size = (size * 100.0).floor() / 100.0;
        size
    }

    // ─── Arbitrage Execution ───────────────────────────────────────────────────

    async fn execute_arb(&mut self, opp: ArbOpportunity, mi: &MarketInfo, total_size: f64) {
        let fee_rate_bps = self.get_fee_rate_bps(mi).await;
        let batch_size = self.config.min_liquidity_size;
        let batches = (total_size / batch_size).ceil() as usize;

        self.log(&format!(
            "ARB: YES@{:.3} + NO@{:.3} = {:.3} | spread={:.4} | size={total_size} | batches={batches}",
            opp.yes_price, opp.no_price, opp.total_cost, opp.spread
        ))
        .await;

        let mut total_profit = 0.0_f64;
        let mut any_success = false;

        for batch in 0..batches {
            let this_size = if batch == batches - 1 {
                // Last batch: remainder
                let remainder = total_size - (batch as f64 * batch_size);
                (remainder * 100.0).floor() / 100.0
            } else {
                batch_size
            };

            if this_size < 0.01 {
                break;
            }

            match self
                .execute_batch(&opp, mi, this_size, fee_rate_bps)
                .await
            {
                Ok(profit) => {
                    total_profit += profit;
                    any_success = true;
                    self.consecutive_failures = 0;
                }
                Err(e) => {
                    warn!("Batch {batch} failed: {e}");
                    self.errors.push(format!("Batch {batch}: {e}"));
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
        fee_rate_bps: u64,
    ) -> Result<f64> {
        // Apply fee adjustment and floor to 2 decimals
        let fee_rate = fee_rate_bps as f64 / 10_000.0;
        let actual_size = (size * (1.0 - fee_rate) * 100.0).floor() / 100.0;

        if self.config.mock_currency {
            let cost = (opp.yes_price + opp.no_price) * size;
            let payout = actual_size * 1.0;
            let profit = payout - cost - self.gas_cache.fee_per_merge_usd;
            info!("[MOCK] Batch profit: ${profit:.4}");
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
                fee_rate_bps,
            ),
            self.client.sign_order(
                &opp.no_token_id,
                opp.no_price,
                size,
                Side::Buy,
                TimeInForce::Gtc,
                mi.neg_risk,
                fee_rate_bps,
            )
        );

        let yes_order = yes_result?;
        let no_order = no_result?;

        // Post cheaper leg first (less liquid = higher network priority)
        let (first, second) = if opp.yes_price <= opp.no_price {
            (&yes_order, &no_order)
        } else {
            (&no_order, &yes_order)
        };

        // Post both as GTC batch
        let responses = self
            .client
            .post_orders(&[first, second], "GTC")
            .await?;

        if responses.is_empty() {
            anyhow::bail!("No responses from post_orders");
        }

        // Wait for fills
        let timeout_ms = self.config.gtc_taker_timeout_ms;
        let (yes_filled, no_filled) = self
            .poll_fills(&responses, &yes_order, &no_order, &opp, mi, timeout_ms)
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
            let mergeable = actual_size.min(yes_filled).min(no_filled);
            self.position.yes_size += yes_filled;
            self.position.no_size += no_filled;
            self.position.yes_cost += yes_filled * opp.yes_price;
            self.position.no_cost += no_filled * opp.no_price;
            self.active_assets.insert(mi.condition_id.clone());

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

            self.log(&format!(
                "  OK: filled YES={yes_filled:.2} NO={no_filled:.2} | profit=${profit:.4}"
            ))
            .await;

            Ok(profit)
        } else if yes_filled > 0.05 || no_filled > 0.05 {
            // Partial fill — hedge the missing leg
            self.handle_partial_fill(
                yes_filled, no_filled, opp, mi, fee_rate_bps, cost,
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
        _yes_order: &SignedOrder,
        _no_order: &SignedOrder,
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

            if !yes_id.is_empty() {
                if let Ok(Some(ord)) = self.client.get_order(&yes_id).await {
                    yes_filled = ord.matched_f64();
                }
            }
            if !no_id.is_empty() {
                if let Ok(Some(ord)) = self.client.get_order(&no_id).await {
                    no_filled = ord.matched_f64();
                }
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
        let _fee_rate = fee_rate_bps as f64 / 10_000.0;
        let max_fee = self.config.max_fee_per_share(missing_price, filled_price);
        let hedge_pnl =
            size * (1.0 - max_fee) - filled_cost - missing_price * size - self.gas_cache.fee_per_merge_usd;

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
                .post_order(&sell_order, "FOK")
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
            let resp = self.client.post_order(&hedge_order, "FOK").await?;
            let matched = resp.size_matched.as_deref().unwrap_or("0").parse::<f64>().unwrap_or(0.0);

            if matched > 0.0 {
                if filled_is_yes {
                    self.position.yes_size += matched;
                    self.position.no_size += matched;
                } else {
                    self.position.no_size += matched;
                    self.position.yes_size += matched;
                }
                self.active_assets.insert(mi.condition_id.clone());
                let fire_amount = self.position.mergeable_amount();
                self.fire_merge(fire_amount, mi).await;
                self.stats.record_hedge();
            }

            let actual_hedge_pnl = matched * (1.0 - max_fee) - filled_cost - matched * missing_price - self.gas_cache.fee_per_merge_usd;
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

        let fee_bps = self.get_fee_rate_bps(mi).await;
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
            Ok(order) => match self.client.post_order(&order, "FOK").await {
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
        let trade_logger_path = "logs/trades.jsonl".to_string(); // Re-create to avoid borrow
        let locked_ref = self.session_locked_value; // snapshot — background task won't mutate shared state

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
        if self.config.mock_currency || self.active_assets.is_empty() {
            return;
        }

        // Gnosis Safe: prevent concurrent redemptions — the Safe nonce is shared,
        // so two simultaneous execTransaction calls with the same nonce produce an
        // "already known" mempool error.  We use an AtomicBool flag and reset it
        // inside the spawned task once the redemption completes.
        if self.is_redeeming.load(Ordering::Relaxed) {
            debug!("Redemption already in progress — skipping");
            return;
        }

        let rpcs = [
            "https://polygon-rpc.com",
            "https://polygon.llamarpc.com",
            "https://rpc.ankr.com/polygon",
        ];
        let rpc = rpcs[0]; // Use primary; fallback would require cloning client

        let assets: Vec<String> = self.active_assets.iter().cloned().collect();
        let holder = self.client.maker_address();

        for condition_id in &assets {
            // Check YES token balance via on-chain balanceOf
            let mi = match self.market_info.as_ref() {
                Some(m) if m.condition_id == *condition_id => m.clone(),
                _ => continue,
            };

            // Check if we hold any CTF balance for either outcome
            let yes_bal = self
                .client
                .get_ctf_balance(&mi.tokens.yes, holder, rpc)
                .await
                .unwrap_or(0.0);
            let no_bal = self
                .client
                .get_ctf_balance(&mi.tokens.no, holder, rpc)
                .await
                .unwrap_or(0.0);

            if yes_bal < 0.01 && no_bal < 0.01 {
                // No balance — market either not resolved or already redeemed
                continue;
            }

            info!(
                "Redeeming {condition_id}: YES={yes_bal:.2} NO={no_bal:.2}"
            );

            // redeemPositions with full partition [1, 2]
            let index_sets = vec![ethers::types::U256::one(), ethers::types::U256::from(2u64)];
            let client = Arc::clone(&self.client);
            let cid = condition_id.clone();
            let question = mi.question.clone();
            let redeemable = yes_bal.max(no_bal);
            let is_redeeming_flag = Arc::clone(&self.is_redeeming);

            // Mark redemption as in-flight before spawning
            is_redeeming_flag.store(true, Ordering::Relaxed);

            tokio::spawn(async move {
                let rpcs = [
                    "https://polygon-rpc.com",
                    "https://polygon.llamarpc.com",
                    "https://rpc.ankr.com/polygon",
                ];
                for rpc in &rpcs {
                    match client.redeem_positions(&cid, &index_sets, rpc).await {
                        Ok(tx) => {
                            info!("Redemption confirmed: {redeemable:.2} | tx={tx:?}");
                            if let Ok(logger) = crate::trade_logger::TradeLogger::new().await {
                                logger
                                    .log_redemption(
                                        &cid,
                                        Some(&question),
                                        redeemable,
                                        redeemable,
                                        true,
                                        None,
                                    )
                                    .await;
                            }
                            is_redeeming_flag.store(false, Ordering::Relaxed);
                            return;
                        }
                        Err(e) => warn!("Redemption failed on {rpc}: {e}"),
                    }
                }
                // All RPCs failed — reset flag so we can retry next cycle
                is_redeeming_flag.store(false, Ordering::Relaxed);
            });

            // Remove from tracking after dispatching redemption
            self.active_assets.remove(condition_id);
        }
    }

    // ─── Market Rollover ───────────────────────────────────────────────────────

    async fn handle_market_rollover(&mut self) {
        let old_question = self
            .market_info
            .as_ref()
            .map(|m| m.question.clone())
            .unwrap_or_default();

        info!("Market expired: {old_question} — searching for next market");

        // Flush stats
        self.stats.flush().await;

        // Cancel maker orders
        if let Some(ref mut maker) = self.maker {
            let _ = maker.cancel_all(&self.client).await;
        }

        // Clear WS books
        self.market_info = None;

        // Find next market
        if let Err(e) = self.find_active_market().await {
            warn!("Failed to find next market: {e}");
            return;
        }

        // Resubscribe WS
        if let (Some(ref ws), Some(ref mi)) = (&self.ws_client, &self.market_info) {
            let token_ids = vec![mi.tokens.yes.clone(), mi.tokens.no.clone()];
            ws.resubscribe(token_ids).await;
        }

        info!("Rolled over to: {}", self.market_info.as_ref().map(|m| m.question.as_str()).unwrap_or("unknown"));
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
                    self.active_assets.insert(mi.condition_id.clone());

                    info!("Maker fills: YES={yes_filled:.2} NO={no_filled:.2}");

                    let fire_amount = self.position.mergeable_amount();
                    if fire_amount >= 1.0 {
                        self.fire_merge(fire_amount, &mi).await;
                    }
                }
            }
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
            match WsClient::connect(token_ids).await {
                Ok((ws, rx)) => {
                    self.ws_client = Some(ws);
                    self.ws_rx = Some(rx);
                    info!("WS reconnected");
                }
                Err(e) => {
                    warn!("WS reconnect failed: {e}");
                }
            }
        }
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    async fn get_fee_rate_bps(&self, mi: &MarketInfo) -> u64 {
        self.client
            .get_market_fee_rate_bps(&mi.condition_id)
            .await
            .unwrap_or(0)
    }

    async fn log(&self, msg: &str) {
        info!("{msg}");
        if let Some(ref logger) = self.session_logger {
            logger.log(msg).await;
        }
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
        if let Some(ref logger) = self.session_logger {
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
