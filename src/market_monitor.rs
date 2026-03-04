//! Core market orchestrator (replicates market_monitor.ts).
//!
//! Manages the full lifecycle of a Polymarket arbitrage bot:
//!   - Market discovery & rollover
//!   - Orderbook fetching (WS primary, REST fallback)
//!   - Arbitrage detection & execution (taker / maker / post-only)
//!   - Position tracking (YES/NO sizes, costs)
//!   - Hedge / sell-back logic on partial fills
//!   - Merge & redemption automation
//!   - Circuit breaker enforcement
//!   - Gas cache refresh

use crate::clob_client::ClobClient;
use crate::config::{Config, StrategyMode};
use crate::dashboard::{Dashboard, DashboardState};
use crate::logger::SessionLogger;
use crate::maker_strategy::MakerStrategy;
use crate::post_only_strategy::PostOnlyStrategy;
use crate::run_memory::{append_run_memory_entry, RunMemoryEntry};
use crate::market_stats::MarketStatsTracker;
use crate::trade_logger::TradeLogger;
use crate::types::{
    ArbOpportunity, ExecutionBatchState, ExecutionState, GasCache, MarketInfo, OrderBook,
    OrderLegState, PendingMergeState, PendingMergeStatus, Position, Side, TimeInForce,
};
use crate::ws_client::WsClient;
use anyhow::{anyhow, Context, Result};
use chrono::{Local, Utc};
use rand::Rng;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex, Notify};
use tracing::{debug, info, warn};


// Buffer before market expiry to stop new arbs (seconds)
const EXPIRY_BUFFER_SECS: i64 = 10;
// Merge retry interval
const MERGE_RETRY_SECS: u64 = 30;
// Position imbalance hedge cooldown (exponential up to 120s)
const MAX_HEDGE_COOLDOWN_SECS: u64 = 120;
// Retry interval when no active market is available yet.
const MARKET_DISCOVERY_RETRY_SECS: u64 = 5;
const DISCOVERY_RETRY_DEGRADED_STEP_SECS: u64 = 5;
const DISCOVERY_RETRY_DEGRADED_STEP_WINDOW_SECS: u64 = 60;
const DISCOVERY_RETRY_MAX_SECS: u64 = 30;
const DISCOVERY_PROBE_INTERVAL_SECS: u64 = 20;
// Claimability polling fallback interval.
const CLAIM_POLL_INTERVAL_SECS: u64 = 5;
const CLAIM_FAILURE_BACKOFF_SECS: u64 = 30;
const CLAIM_SUCCESS_SUPPRESSION_SECS: u64 = 90;
const CLAIM_MIN_AMOUNT_USDC: f64 = 0.0;
const PERF_WINDOW_SAMPLES: usize = 240;
const PERF_LOG_INTERVAL_SECS: u64 = 60;
const WS_AGE_SAMPLE_INTERVAL_MS: u64 = 250;
const ORDER_STATUS_TIMEOUT_MS: u64 = 1_500;
const SDK_CALL_TIMEOUT_MS: u64 = 1_500;
const SDK_POST_TIMEOUT_MS: u64 = 3_000;
const SDK_CANCEL_TIMEOUT_MS: u64 = 1_200;
const SDK_ORDER_LOOKUP_TIMEOUT_MS: u64 = 1_200;
const POST_RETRY_MAX_RETRIES: u64 = 2;
const CANCEL_RETRY_MAX_RETRIES: u64 = 1;
const SDK_RETRY_BASE_DELAY_MS: u64 = 60;
const SDK_RETRY_MAX_DELAY_MS: u64 = 600;
const SDK_RETRY_JITTER_MS: u64 = 35;
const PRE_SUBMIT_SIGNAL_MAX_AGE_MS: u64 = 450;
const PRE_SUBMIT_PAIR_DRIFT_MAX: f64 = 0.01;
const PRE_SUBMIT_MIN_LIQ_FACTOR: f64 = 0.90;
const WS_STALE_SAMPLE_LIMIT: u8 = 2;
const WS_STALE_GRACE_SECS: u64 = 10;
const DASHBOARD_FORCE_RENDER_MS: u64 = 1_000;
const MAX_WS_USER_EVENTS_PER_CYCLE: usize = 256;
const MAX_WS_MARKET_EVENTS_PER_CYCLE: usize = 64;
const MAX_SEEN_WS_TRADE_IDS: usize = 8_192;
const MAX_RESPONSE_APPLIED_ORDER_IDS: usize = 8_192;
const IMBALANCE_DUST_RECHECK_SECS: u64 = 20;
const RECONCILE_LARGE_DRIFT_SHARES: f64 = 2.0;
const RECONCILE_DUST_MAX_SHARES: f64 = 1.0;
const RECONCILE_DUST_DRIFT_MIN_SHARES: f64 = 0.05;
const RECONCILE_DUST_SYNC_STREAK: u32 = 3;
const RECONCILE_NONZERO_SYNC_STREAK: u32 = 3;
const RECONCILE_ZERO_SYNC_STREAK: u32 = 6;
const RECONCILE_ZERO_SYNC_GRACE_SECS: u64 = 180;
const ENTRY_LOCK_RECONCILE_DRIFT_STREAK: u32 = 2;
const ENTRY_LOCK_LOG_INTERVAL_SECS: u64 = 5;
const DISCOVERY_DEGRADED_WARN_INTERVAL_SECS: u64 = 60;
const DISCOVERY_SELF_HEAL_MIN_INTERVAL_SECS: u64 = 90;
const ADAPTIVE_WINDOW_SIZE: usize = 30;
const ADAPTIVE_MIN_WINDOW_SAMPLES: usize = 8;
const ADAPTIVE_HEDGE_WINDOW_SIZE: usize = 24;
const ADAPTIVE_TARGET_COMPLETION_RATIO: f64 = 0.65;
const ADAPTIVE_TARGET_ECONOMIC_RATIO: f64 = 0.55;
const ADAPTIVE_NEG_PNL_SCALE_USD: f64 = 0.25;
const ADAPTIVE_PRESSURE_SMOOTHING: f64 = 0.35;
const ADAPTIVE_MIN_PROFIT_CAP: f64 = 0.14;
const ADAPTIVE_MIN_SIZE_FLOOR: f64 = 5.0;
const ADAPTIVE_LOG_MIN_PROFIT_DELTA: f64 = 0.005;
const ADAPTIVE_LOG_MIN_SIZE_DELTA: f64 = 0.25;
const HEDGE_FAILURE_LOCK_MIN_SAMPLES: usize = 12;
const HEDGE_FAILURE_LOCK_RATIO: f64 = 0.70;
const HEDGE_FAILURE_LOCK_TTL_SECS: u64 = 300;
const REST_RECOVERY_POLL_LOOKBACK_MAX_TRADES: usize = 200;
const RUNTIME_RESUME_GAP_SECS: u64 = 20;
const RUNTIME_RESUME_RECOVERY_DEBOUNCE_SECS: u64 = 15;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryLockReason {
    Imbalance,
    TimeoutRecovery,
    AmbiguousFill,
    ReconcileDrift,
    HedgeFailureRisk,
}

#[derive(Debug, Clone, Default)]
struct PostedOrderIds {
    yes_order_id: String,
    no_order_id: String,
}

#[derive(Debug, Clone)]
struct ActionableBookSnapshot {
    yes_ask: f64,
    no_ask: f64,
    yes_size: f64,
    no_size: f64,
}

#[derive(Debug, Clone, Copy)]
struct OpportunityOutcome {
    completed: bool,
    pnl: f64,
}

#[derive(Debug, Clone)]
struct MergeResultMsg {
    merge_id: String,
    condition_id: String,
    question: String,
    size: f64,
    tx_hash: Option<String>,
    success: bool,
    error: Option<String>,
}

pub struct MarketMonitor {
    config: Arc<Config>,
    client: Arc<ClobClient>,
    ws_client: Option<WsClient>,
    ws_notify: Option<Arc<Notify>>,
    session_logger: Option<SessionLogger>,
    trade_logger: TradeLogger,
    stats: MarketStatsTracker,
    maker: Option<MakerStrategy>,
    post_only: Option<PostOnlyStrategy>,

    // Active market
    market_info: Option<MarketInfo>,

    // Position state
    position: Position,
    session_locked_value: f64,
    session_start_balance: f64,
    session_successes_baseline: u64,
    session_failures_baseline: u64,

    // Circuit breaker
    daily_pnl: f64,
    execution_pnl: f64,
    hedge_sellback_pnl: f64,
    redemption_pnl: f64,
    fees_gas_estimate: f64,
    execution_success_count: u64,
    economic_success_count: u64,
    consecutive_failures: u32,
    circuit_breaker_until: Option<Instant>,
    entry_lock_until: Option<Instant>,
    entry_lock_reason: Option<EntryLockReason>,
    timeout_recovery_active: bool,
    unresolved_ambiguous_batches: HashSet<String>,
    last_entry_lock_log_reason: Option<EntryLockReason>,
    last_entry_lock_log_at: Option<Instant>,

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
    discovery_failure_since: Option<Instant>,
    last_discovery_degraded_warn_at: Option<Instant>,
    last_discovery_self_heal_at: Option<Instant>,
    last_discovery_probe_at: Option<Instant>,
    force_claim_scan: bool,
    last_runtime_tick: Instant,
    last_resume_recovery_at: Option<Instant>,

    // Last time we checked for opportunity
    last_check: Option<Instant>,

    // Errors collected for session summary
    errors: Vec<String>,

    // Prevents concurrent Safe redemptions (same nonce → "already known" mempool error)
    is_redeeming: Arc<AtomicBool>,
    pending_claim_value: f64,

    // Event-driven execution tracking
    order_tracker: HashMap<String, OrderLegState>,
    order_to_batch: HashMap<String, String>,
    execution_batches: HashMap<String, ExecutionBatchState>,
    ws_fill_last_event_at: Option<Instant>,
    last_actionable_snapshot: Option<ActionableBookSnapshot>,

    // Merge reconciliation state
    pending_merges: HashMap<String, PendingMergeState>,
    pending_merge_reserved_size: f64,
    merge_result_tx: mpsc::UnboundedSender<MergeResultMsg>,
    merge_result_rx: mpsc::UnboundedReceiver<MergeResultMsg>,

    // TUI dashboard
    dashboard: Dashboard,
    last_balance: Option<f64>,
    last_balance_refresh: Instant,
    last_data_source: String,
    cached_fee_rate_bps: u64,
    last_tick_signature: Option<String>,
    ws_yes_age_ms: Option<u64>,
    ws_no_age_ms: Option<u64>,
    ws_reconnect_requested: bool,
    ws_stale_warned: bool,
    ws_stale_consecutive: u8,
    ws_connected_at: Option<Instant>,
    last_ws_age_sample: Option<Instant>,
    claim_retry_after: HashMap<String, Instant>,
    claim_success_suppression_until: HashMap<String, Instant>,
    check_latency_ms: VecDeque<u64>,
    detect_to_submit_latency_ms: VecDeque<u64>,
    first_fill_latency_ms: VecDeque<u64>,
    pair_complete_latency_ms: VecDeque<u64>,
    claim_latency_ms: VecDeque<u64>,
    balance_latency_ms: VecDeque<u64>,
    lock_wait_latency_ms: VecDeque<u64>,
    render_latency_ms: VecDeque<u64>,
    checks_total: u64,
    checks_fee_qualified: u64,
    checks_skipped_extreme: u64,
    checks_skipped_size: u64,
    checks_skipped_profit: u64,
    checks_executed: u64,
    opportunity_detected_count: u64,
    post_attempt_count: u64,
    post_paired_count: u64,
    hedge_event_count: u64,
    stale_poll_fallback_count: u64,
    merge_mismatch_count: u64,
    perf_last_log: Instant,
    render_requested: bool,
    last_render_at: Instant,
    seen_ws_trade_ids: HashSet<String>,
    ws_trade_id_queue: VecDeque<String>,
    response_applied_order_ids: HashSet<String>,
    response_applied_order_queue: VecDeque<String>,
    last_position_activity_at: Option<Instant>,
    reconcile_drift_streak: u32,
    adaptive_min_profit_usd: f64,
    adaptive_max_trade_size: f64,
    adaptive_pressure: f64,
    recent_opportunity_outcomes: VecDeque<OpportunityOutcome>,
    recent_hedge_outcomes: VecDeque<bool>,
    last_hedge_outcome_at: Option<Instant>,
    last_yes_mid_price: Option<f64>,
    last_no_mid_price: Option<f64>,
    last_no_edge_skip_key: Option<String>,
    last_no_edge_skip_at: Option<Instant>,
    seen_rest_trade_ids: HashSet<String>,
    rest_trade_id_queue: VecDeque<String>,
}

impl MarketMonitor {
    pub async fn new(config: Arc<Config>, client: Arc<ClobClient>) -> Result<Self> {
        let trade_logger = TradeLogger::new().await?;
        let stats = MarketStatsTracker::load().await?;
        let session_successes_baseline = stats.successes();
        let session_failures_baseline = stats.failures();
        let (maker, post_only) = match config.strategy_mode {
            StrategyMode::Maker => (Some(MakerStrategy::new(Arc::clone(&config))), None),
            StrategyMode::PostOnly => (None, Some(PostOnlyStrategy::new(Arc::clone(&config)))),
            StrategyMode::Taker => (None, None),
        };
        let (merge_result_tx, merge_result_rx) = mpsc::unbounded_channel();
        let adaptive_min_profit_usd = config.min_net_profit_usd;
        let adaptive_max_trade_size = config.max_trade_size;

        Ok(Self {
            config,
            client,
            ws_client: None,
            ws_notify: None,
            session_logger: None,
            trade_logger,
            stats,
            maker,
            post_only,
            market_info: None,
            cached_fee_rate_bps: 0,
            position: Position::default(),
            session_locked_value: 0.0,
            session_start_balance: 0.0,
            session_successes_baseline,
            session_failures_baseline,
            daily_pnl: 0.0,
            execution_pnl: 0.0,
            hedge_sellback_pnl: 0.0,
            redemption_pnl: 0.0,
            fees_gas_estimate: 0.0,
            execution_success_count: 0,
            economic_success_count: 0,
            consecutive_failures: 0,
            circuit_breaker_until: None,
            entry_lock_until: None,
            entry_lock_reason: None,
            timeout_recovery_active: false,
            unresolved_ambiguous_batches: HashSet::new(),
            last_entry_lock_log_reason: None,
            last_entry_lock_log_at: None,
            hedge_cooldown_until: None,
            consecutive_hedge_failures: 0,
            active_assets: HashSet::new(),
            gas_cache: GasCache::default(),
            _gas_cache_refreshed: None,
            last_merge_attempt: None,
            last_claim_poll: None,
            last_market_discovery_attempt: None,
            discovery_failure_since: None,
            last_discovery_degraded_warn_at: None,
            last_discovery_self_heal_at: None,
            last_discovery_probe_at: None,
            force_claim_scan: false,
            last_runtime_tick: Instant::now(),
            last_resume_recovery_at: None,
            last_check: None,
            errors: Vec::new(),
            is_redeeming: Arc::new(AtomicBool::new(false)),
            pending_claim_value: 0.0,
            order_tracker: HashMap::new(),
            order_to_batch: HashMap::new(),
            execution_batches: HashMap::new(),
            ws_fill_last_event_at: None,
            last_actionable_snapshot: None,
            pending_merges: HashMap::new(),
            pending_merge_reserved_size: 0.0,
            merge_result_tx,
            merge_result_rx,
            dashboard: Dashboard::new(),
            last_balance: None,
            last_balance_refresh: Instant::now(),
            last_data_source: "REST".to_string(),
            last_tick_signature: None,
            ws_yes_age_ms: None,
            ws_no_age_ms: None,
            ws_reconnect_requested: false,
            ws_stale_warned: false,
            ws_stale_consecutive: 0,
            ws_connected_at: None,
            last_ws_age_sample: None,
            claim_retry_after: HashMap::new(),
            claim_success_suppression_until: HashMap::new(),
            check_latency_ms: VecDeque::with_capacity(PERF_WINDOW_SAMPLES + 1),
            detect_to_submit_latency_ms: VecDeque::with_capacity(PERF_WINDOW_SAMPLES + 1),
            first_fill_latency_ms: VecDeque::with_capacity(PERF_WINDOW_SAMPLES + 1),
            pair_complete_latency_ms: VecDeque::with_capacity(PERF_WINDOW_SAMPLES + 1),
            claim_latency_ms: VecDeque::with_capacity(PERF_WINDOW_SAMPLES + 1),
            balance_latency_ms: VecDeque::with_capacity(PERF_WINDOW_SAMPLES + 1),
            lock_wait_latency_ms: VecDeque::with_capacity(PERF_WINDOW_SAMPLES + 1),
            render_latency_ms: VecDeque::with_capacity(PERF_WINDOW_SAMPLES + 1),
            checks_total: 0,
            checks_fee_qualified: 0,
            checks_skipped_extreme: 0,
            checks_skipped_size: 0,
            checks_skipped_profit: 0,
            checks_executed: 0,
            opportunity_detected_count: 0,
            post_attempt_count: 0,
            post_paired_count: 0,
            hedge_event_count: 0,
            stale_poll_fallback_count: 0,
            merge_mismatch_count: 0,
            perf_last_log: Instant::now(),
            render_requested: true,
            last_render_at: Instant::now(),
            seen_ws_trade_ids: HashSet::new(),
            ws_trade_id_queue: VecDeque::with_capacity(MAX_SEEN_WS_TRADE_IDS + 1),
            response_applied_order_ids: HashSet::new(),
            response_applied_order_queue: VecDeque::with_capacity(MAX_RESPONSE_APPLIED_ORDER_IDS + 1),
            last_position_activity_at: None,
            reconcile_drift_streak: 0,
            adaptive_min_profit_usd,
            adaptive_max_trade_size,
            adaptive_pressure: 0.0,
            recent_opportunity_outcomes: VecDeque::with_capacity(ADAPTIVE_WINDOW_SIZE + 1),
            recent_hedge_outcomes: VecDeque::with_capacity(ADAPTIVE_HEDGE_WINDOW_SIZE + 1),
            last_hedge_outcome_at: None,
            last_yes_mid_price: None,
            last_no_mid_price: None,
            last_no_edge_skip_key: None,
            last_no_edge_skip_at: None,
            seen_rest_trade_ids: HashSet::new(),
            rest_trade_id_queue: VecDeque::with_capacity(MAX_SEEN_WS_TRADE_IDS + 1),
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
        self.sync_daily_pnl_from_balance();

        // 2. Init session logger
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

        // 3. Find active market (retry forever until one appears)
        loop {
            self.last_market_discovery_attempt = Some(Instant::now());
            match self.find_active_market().await {
                Ok(true) => {
                    self.clear_discovery_degraded_state();
                    break;
                }
                Ok(false) => {
                    self.clear_discovery_degraded_state();
                    let retry_secs = self.discovery_retry_secs();
                    self.log_action(&format!(
                        "⏳ No active market yet. Retrying in {}s...",
                        retry_secs
                    ))
                    .await;
                }
                Err(e) => {
                    warn!("Market discovery error during init: {e}");
                    if self.discovery_failure_since.is_none() {
                        self.discovery_failure_since = Some(Instant::now());
                    }
                    let retry_secs = self.discovery_retry_secs();
                    let reason = Self::truncate_for_action(&Self::format_error_chain(&e), 220);
                    self.log_action(&format!(
                        "⚠️ Market discovery failed (retrying in {}s): {}",
                        retry_secs, reason
                    ))
                    .await;
                    self.maybe_log_discovery_degraded().await;
                }
            }
            tokio::time::sleep(Duration::from_secs(self.discovery_retry_secs())).await;
        }

        // 4. Recover prior trade history now that market_info is known.
        self.recover_positions_from_trades().await;

        // 5. Connect WebSocket if enabled
        self.connect_ws_for_active_market(false).await;

        // 6. Refresh gas cache
        self.refresh_gas_cache().await;

        // 7. Ensure CTF approvals for Gnosis Safe (SIGNATURE_TYPE=2)
        if self.config.uses_proxy() && !self.config.mock_currency {
            let rpc = crate::clob_client::POLYGON_RPCS[0];
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
                self.position = Position::default();
                for trade in &trades {
                    self.mark_rest_trade_seen(&trade.id);
                    let _ = self.apply_trade_record_to_position(trade, mi);
                }
                if self.position.yes_size > 0.01 || self.position.no_size > 0.01 {
                    self.note_position_activity();
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
        let mut had_successful_lookup = false;
        let mut last_lookup_error: Option<anyhow::Error> = None;

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
                    had_successful_lookup = true;
                    warn!("No active market found for slug '{slug}'");
                }
                Err(e) => {
                    warn!("Error finding market for slug '{slug}': {e}");
                    last_lookup_error = Some(e);
                }
            }
        }

        // Distinguish "no market exists" from "all discovery calls failed".
        if !had_successful_lookup {
            if let Some(e) = last_lookup_error {
                return Err(e).context("all market discovery lookups failed");
            }
        }

        Ok(false)
    }

    fn discovery_retry_secs(&self) -> u64 {
        let base = MARKET_DISCOVERY_RETRY_SECS.max(1);
        let Some(first_failure) = self.discovery_failure_since else {
            return base;
        };
        let elapsed = first_failure.elapsed().as_secs();
        if elapsed < self.config.discovery_degraded_secs {
            return base;
        }

        let degraded_elapsed = elapsed.saturating_sub(self.config.discovery_degraded_secs);
        let steps = degraded_elapsed / DISCOVERY_RETRY_DEGRADED_STEP_WINDOW_SECS + 1;
        let backoff = base
            .saturating_add(steps.saturating_mul(DISCOVERY_RETRY_DEGRADED_STEP_SECS))
            .min(DISCOVERY_RETRY_MAX_SECS);
        backoff.max(base)
    }

    fn market_discovery_due(&self) -> bool {
        self.last_market_discovery_attempt
            .map(|t| t.elapsed().as_secs() >= self.discovery_retry_secs())
            .unwrap_or(true)
    }

    fn clear_discovery_degraded_state(&mut self) {
        self.discovery_failure_since = None;
        self.last_discovery_degraded_warn_at = None;
        self.last_discovery_self_heal_at = None;
        self.last_discovery_probe_at = None;
    }

    fn discovery_probe_due(&self) -> bool {
        let interval_secs = Self::env_u64(
            "DISCOVERY_PROBE_INTERVAL_SECS",
            DISCOVERY_PROBE_INTERVAL_SECS,
        )
        .max(5);
        self.last_discovery_probe_at
            .map(|t| t.elapsed().as_secs() >= interval_secs)
            .unwrap_or(true)
    }

    async fn maybe_log_discovery_degraded(&mut self) {
        let Some(first_failure) = self.discovery_failure_since else {
            return;
        };
        if first_failure.elapsed().as_secs() < self.config.discovery_degraded_secs {
            return;
        }
        let warn_due = self
            .last_discovery_degraded_warn_at
            .map(|t| t.elapsed().as_secs() >= DISCOVERY_DEGRADED_WARN_INTERVAL_SECS)
            .unwrap_or(true);
        if warn_due {
            self.last_discovery_degraded_warn_at = Some(Instant::now());
            let retry_secs = self.discovery_retry_secs();
            self.log_action(&format!(
                "⚠️ Discovery degraded >{}s: Gamma /events + /markets failing. Holding in retry mode ({}s).",
                self.config.discovery_degraded_secs, retry_secs
            ))
            .await;
        }

        self.maybe_attempt_discovery_self_heal().await;
    }

    async fn maybe_attempt_discovery_self_heal(&mut self) {
        let Some(first_failure) = self.discovery_failure_since else {
            return;
        };
        if first_failure.elapsed().as_secs() < self.config.discovery_degraded_secs {
            return;
        }

        let heal_due = self
            .last_discovery_self_heal_at
            .map(|t| t.elapsed().as_secs() >= DISCOVERY_SELF_HEAL_MIN_INTERVAL_SECS)
            .unwrap_or(true);
        if !heal_due {
            return;
        }

        self.last_discovery_self_heal_at = Some(Instant::now());
        self.log_action(
            "🔧 Discovery self-heal: rebuilding CLOB client and forcing rediscovery.",
        )
        .await;

        match ClobClient::new(Arc::clone(&self.config)).await {
            Ok(new_client) => {
                self.client = Arc::new(new_client);
                self.log_action("🔧 Discovery self-heal: CLOB client rebuilt.")
                    .await;
            }
            Err(e) => {
                warn!("Discovery self-heal failed to rebuild CLOB client: {e}");
                let reason = Self::truncate_for_action(&Self::format_error_chain(&e), 220);
                self.log_action(&format!(
                    "⚠️ Discovery self-heal failed: client rebuild error: {reason}"
                ))
                .await;
                return;
            }
        }

        self.ws_client = None;
        self.ws_notify = None;
        self.ws_yes_age_ms = None;
        self.ws_no_age_ms = None;
        self.ws_reconnect_requested = true;
        self.ws_stale_warned = false;
        self.ws_stale_consecutive = 0;
        self.ws_connected_at = None;
        self.last_ws_age_sample = None;
        self.last_tick_signature = None;
        self.last_actionable_snapshot = None;

        self.last_market_discovery_attempt = Some(Instant::now());
        match self.find_active_market().await {
            Ok(true) => {
                self.clear_discovery_degraded_state();
                self.log_action("✅ Discovery self-heal recovered active market.")
                    .await;
                self.connect_ws_for_active_market(true).await;
            }
            Ok(false) => {
                self.clear_discovery_degraded_state();
                self.log_action("ℹ️ Discovery self-heal completed: no active market right now.")
                    .await;
            }
            Err(e) => {
                warn!("Discovery self-heal rediscovery attempt failed: {e}");
                let reason = Self::truncate_for_action(&Self::format_error_chain(&e), 220);
                self.log_action(&format!(
                    "⚠️ Discovery self-heal rediscovery failed: {reason}"
                ))
                .await;
            }
        }
    }

    async fn try_find_active_market_if_due(&mut self) {
        if self.market_info.is_some() || !self.market_discovery_due() {
            return;
        }

        self.last_market_discovery_attempt = Some(Instant::now());
        match self.find_active_market().await {
            Ok(true) => {
                self.clear_discovery_degraded_state();
                self.connect_ws_for_active_market(false).await;
            }
            Ok(false) => {
                self.clear_discovery_degraded_state();
                let retry_secs = self.discovery_retry_secs();
                self.log_action(&format!(
                    "⏳ No active market yet. Retrying in {}s...",
                    retry_secs
                ))
                .await;
            }
            Err(e) => {
                warn!("Market discovery error: {e}");
                if self.discovery_failure_since.is_none() {
                    self.discovery_failure_since = Some(Instant::now());
                }
                let retry_secs = self.discovery_retry_secs();
                let reason = Self::truncate_for_action(&Self::format_error_chain(&e), 220);
                self.log_action(&format!(
                    "⚠️ Market discovery failed (retrying in {}s): {}",
                    retry_secs, reason
                ))
                .await;
                self.maybe_log_discovery_degraded().await;
            }
        }
    }

    pub async fn run_discovery_probe_cycle(monitor: &Arc<Mutex<Self>>) {
        let client = {
            let mut m = match monitor.try_lock() {
                Ok(g) => g,
                Err(_) => return,
            };

            if m.market_info.is_some() {
                return;
            }
            let Some(first_failure) = m.discovery_failure_since else {
                return;
            };
            if first_failure.elapsed().as_secs() < m.config.discovery_degraded_secs {
                return;
            }
            if !m.discovery_probe_due() {
                return;
            }

            m.last_discovery_probe_at = Some(Instant::now());
            Arc::clone(&m.client)
        };

        if client.gamma_health_check().await.is_err() {
            return;
        }

        if let Ok(mut m) = monitor.try_lock() {
            if m.market_info.is_some() || m.discovery_failure_since.is_none() {
                return;
            }
            m.log_action("✅ Discovery probe: Gamma reachable again — forcing immediate rediscovery.")
                .await;
            m.last_market_discovery_attempt = None;
            m.try_find_active_market_if_due().await;
        }
    }

    async fn maybe_recover_after_runtime_pause(&mut self) {
        let now = Instant::now();
        let gap = now.saturating_duration_since(self.last_runtime_tick);
        self.last_runtime_tick = now;
        if gap.as_secs() < RUNTIME_RESUME_GAP_SECS {
            return;
        }

        let recently_recovered = self
            .last_resume_recovery_at
            .map(|t| t.elapsed().as_secs() < RUNTIME_RESUME_RECOVERY_DEBOUNCE_SECS)
            .unwrap_or(false);
        if recently_recovered {
            return;
        }
        self.last_resume_recovery_at = Some(now);

        self.log_action(&format!(
            "🔄 Runtime pause detected ({}s). Forcing network/WS recovery.",
            gap.as_secs()
        ))
        .await;

        self.last_tick_signature = None;
        self.last_actionable_snapshot = None;
        self.ws_reconnect_requested = true;
        self.ws_stale_warned = false;
        self.ws_stale_consecutive = 0;
        self.last_ws_age_sample = None;

        // After wake, run discovery immediately instead of waiting for next due window.
        self.last_market_discovery_attempt = None;
        self.try_find_active_market_if_due().await;

        if self.config.ws_enabled
            && self.market_info.is_some()
            && (self.ws_client.is_none() || self.ws_reconnect_requested)
        {
            self.connect_ws_for_active_market(true).await;
        }

        self.request_render();
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
            self.log_action("📡 Re-initialising WebSocket connection...")
                .await;
        }

        self.ws_client = None;
        self.ws_notify = None;
        self.ws_yes_age_ms = None;
        self.ws_no_age_ms = None;
        self.ws_reconnect_requested = false;
        self.ws_stale_warned = false;
        self.ws_stale_consecutive = 0;
        self.ws_connected_at = None;
        self.last_ws_age_sample = None;

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
                self.ws_connected_at = Some(Instant::now());
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

    /// Adaptive trigger gate for WS notifications.
    /// Actionable deltas run at min interval; noisy bursts are debounced.
    pub async fn should_run_on_ws_notify(&mut self) -> bool {
        let actionable = self.consume_ws_actionable_signal().await;
        let since_last = self
            .last_check
            .map(|t| t.elapsed())
            .unwrap_or_else(|| Duration::from_secs(3600));
        let threshold_ms = if actionable {
            self.config.adaptive_throttle_min_ms
        } else {
            self.config.adaptive_throttle_burst_debounce_ms
        };
        since_last >= Duration::from_millis(threshold_ms)
    }

    async fn consume_ws_actionable_signal(&mut self) -> bool {
        let Some(mi) = self.market_info.as_ref() else {
            return true;
        };
        let Some(ws) = self.ws_client.as_ref() else {
            return true;
        };
        let (yes_book_opt, no_book_opt) = tokio::join!(
            ws.get_order_book(&mi.tokens.yes),
            ws.get_order_book(&mi.tokens.no)
        );
        let (Some(yes_book), Some(no_book)) = (yes_book_opt, no_book_opt) else {
            return true;
        };
        let (Some(yes_ask), Some(no_ask)) = (yes_book.best_ask(), no_book.best_ask()) else {
            return true;
        };
        let yes_size = yes_book.best_ask_size();
        let no_size = no_book.best_ask_size();
        let snap = ActionableBookSnapshot {
            yes_ask,
            no_ask,
            yes_size,
            no_size,
        };

        let actionable = if let Some(prev) = self.last_actionable_snapshot.as_ref() {
            let tick = mi.tick_size.max(0.0001);
            let min_tick_move = self.config.actionable_delta_min_ticks as f64 * tick;
            let size_threshold = (self.config.min_liquidity_size * 0.25).max(1.0);
            (yes_ask - prev.yes_ask).abs() >= min_tick_move
                || (no_ask - prev.no_ask).abs() >= min_tick_move
                || (yes_size - prev.yes_size).abs() >= size_threshold
                || (no_size - prev.no_size).abs() >= size_threshold
        } else {
            true
        };

        self.last_actionable_snapshot = Some(snap);
        actionable
    }

    pub async fn check_opportunity(&mut self) {
        // Note: rate-limiting is now controlled by the caller (main.rs)
        // via WS-driven 20ms throttle + 1s REST fallback.
        self.maybe_recover_after_runtime_pause().await;
        self.last_check = Some(Instant::now());
        self.process_merge_results().await;

        // Process any incoming User WS events
        self.process_user_events().await;

        // Circuit breaker check
        if let Some(until) = self.circuit_breaker_until {
            if Instant::now() < until {
                return;
            }
            self.circuit_breaker_until = None;
            info!("Circuit breaker expired, resuming");
            self.log_action("⚡ Circuit breaker expired, resuming").await;
        }

        let mi = match self.market_info.clone() {
            Some(m) => m,
            None => {
                self.try_find_active_market_if_due().await;
                self.request_render();
                return;
            }
        };

        self.sample_ws_age_if_due(&mi).await;

        // Market expiry check (stop new arbs 10s before expiry)
        let now_utc = Utc::now();
        let secs_to_expiry = (mi.end_date - now_utc).num_seconds();
        if secs_to_expiry <= EXPIRY_BUFFER_SECS {
            self.handle_market_rollover().await;
            return;
        }

        // Fetch orderbooks
        let (yes_book, no_book) = match self.fetch_books(&mi).await {
            Some(books) => books,
            None => {
                self.request_render();
                return;
            }
        };

        let yes_ask = match yes_book.best_ask() {
            Some(p) => p,
            None => {
                self.request_render();
                return;
            }
        };
        let no_ask = match no_book.best_ask() {
            Some(p) => p,
            None => {
                self.request_render();
                return;
            }
        };
        self.last_yes_mid_price = yes_book
            .best_bid()
            .zip(Some(yes_ask))
            .map(|(bid, ask)| (bid + ask) * 0.5)
            .or(Some(yes_ask));
        self.last_no_mid_price = no_book
            .best_bid()
            .zip(Some(no_ask))
            .map(|(bid, ask)| (bid + ask) * 0.5)
            .or(Some(no_ask));
        self.checks_total = self.checks_total.saturating_add(1);

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
            let tick_signature = format!("{yes_ask:.6}|{yes_liq}|{no_ask:.6}|{no_liq}");

            if self
                .last_tick_signature
                .as_ref()
                .map(|s| s != &tick_signature)
                .unwrap_or(true)
            {
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
                self.last_tick_signature = Some(tick_signature);
                self.request_render();
            }
        }

        // Handle position imbalance first
        if self.position.has_imbalance() {
            self.handle_imbalance(&mi, yes_ask, no_ask).await;
            self.request_render();
            return;
        }

        // Trigger merge if we have a balanced position
        if self.mergeable_available() >= 1.0 {
            if self.last_merge_attempt.map(|t| t.elapsed().as_secs() >= MERGE_RETRY_SECS).unwrap_or(true) {
                let amount = self.mergeable_available();
                self.fire_merge(amount, &mi).await;
            }
            // Do NOT return here! The bot can continue hunting for new arbs with remaining capital!
        }

        self.update_entry_lock_state();
        if self.entry_lock_reason.is_some() {
            self.log_entry_lock_if_needed().await;
            self.request_render();
            return;
        }

        if total_cost < effective_threshold {
            self.checks_fee_qualified = self.checks_fee_qualified.saturating_add(1);
            let max_leg_price = 1.0 - self.config.min_leg_price;
            if yes_ask < self.config.min_leg_price || yes_ask > max_leg_price ||
                no_ask < self.config.min_leg_price || no_ask > max_leg_price {
                self.checks_skipped_extreme = self.checks_skipped_extreme.saturating_add(1);
                self.log_action(&format!(
                    "⏭  Extreme prices skipped: UP@{:.2}, DOWN@{:.2} — near-zero liquidity on cheap leg (min: {})",
                    yes_ask, no_ask, self.config.min_leg_price
                )).await;
                self.request_render();
                return;
            }

            // Record opportunity for stats
            self.stats.record_opportunity(net_spread);
            self.opportunity_detected_count = self.opportunity_detected_count.saturating_add(1);

            let arb_msg = format!(
                "🚨 ARB OPPORTUNITY: pair cost ${total_cost:.3}/share vs fee-adjusted cap ${effective_threshold:.3}/share (edge ${net_spread:.3}/share)"
            );
            self.log_action(&arb_msg).await;
        } else {
            self.request_render();
            return;
        }

        if self.config.uses_resting_orders() {
            let resting_min_edge = Self::env_f64("RESTING_MIN_EDGE_PER_SHARE", 0.01).max(0.0);
            if net_spread < resting_min_edge {
                self.checks_skipped_profit = self.checks_skipped_profit.saturating_add(1);
                if self.should_log_no_edge_skip(net_spread, resting_min_edge) {
                    self.log_action_fast(&format!(
                        "🧾 ORDER SEND: NO — resting edge ${net_spread:.3}/share below floor ${resting_min_edge:.3}/share."
                    ));
                }
                self.request_render();
                return;
            }
        }

        // Resting-quote strategies: post/update bids instead of taker crossing.
        let quote_balance_usdc = if self.config.mock_currency {
            1000.0
        } else {
            self.last_balance
                .or_else(|| {
                    if self.session_start_balance > 0.0 {
                        Some(self.session_start_balance)
                    } else {
                        None
                    }
                })
                .unwrap_or(0.0)
        };
        match self.config.strategy_mode {
            StrategyMode::Maker => {
                self.log_action_fast(
                    "🧾 ORDER SEND (taker): NO — maker strategy is enabled, posting/refreshing resting bids.",
                );
                if let Some(ref mut maker) = self.maker {
                    let active_fee_rate = if self.cached_fee_rate_bps > 0 {
                        self.config.clob_fee_rate
                    } else {
                        0.0
                    };
                    let fee_bps = (active_fee_rate * 10_000.0).round() as u64;
                    let _ = maker
                        .update_orders(
                            &self.client,
                            &mi,
                            yes_ask,
                            no_ask,
                            fee_bps,
                            quote_balance_usdc,
                        )
                        .await;
                }
                self.request_render();
                return;
            }
            StrategyMode::PostOnly => {
                self.log_action_fast(
                    "🧾 ORDER SEND (taker): NO — post-only strategy is enabled, posting postOnly resting bids.",
                );
                if let Some(ref mut strategy) = self.post_only {
                    let active_fee_rate = if self.cached_fee_rate_bps > 0 {
                        self.config.clob_fee_rate
                    } else {
                        0.0
                    };
                    let fee_bps = (active_fee_rate * 10_000.0).round() as u64;
                    let yes_bid = yes_book
                        .best_bid()
                        .unwrap_or((yes_ask - mi.tick_size).max(0.01));
                    let no_bid = no_book
                        .best_bid()
                        .unwrap_or((no_ask - mi.tick_size).max(0.01));
                    let _ = strategy
                        .update_orders(
                            &self.client,
                            &mi,
                            yes_ask,
                            no_ask,
                            yes_bid,
                            no_bid,
                            fee_bps,
                            quote_balance_usdc,
                        )
                        .await;
                }
                self.request_render();
                return;
            }
            StrategyMode::Taker => {}
        }

        // Calculate trade size
        let size = self.calculate_size(&yes_book, &no_book, yes_ask, no_ask).await;
        if size < 1.0 {
            self.checks_skipped_size = self.checks_skipped_size.saturating_add(1);
            self.log_action_fast(
                "🧾 ORDER SEND: NO — computed tradable size is below minimum after liquidity/balance constraints.",
            );
            self.request_render();
            return;
        }

        // Profitability check including gas
        let planned_batches = Self::plan_taker_batches(size);
        let gas_estimate = self.gas_cache.fee_per_merge_usd * (planned_batches.len() as f64 + 1.0);
        
        // Fee calculation using proper exponent equation independently per leg
        let actual_size_yes = self.fee_adjust_shares(size, yes_ask);
        let actual_size_no = self.fee_adjust_shares(size, no_ask);
        let actual_payout_size = actual_size_yes.min(actual_size_no);
        
        let expected_profit = actual_payout_size * 1.0 - total_cost * size - gas_estimate;

        let min_profit_usd = self.effective_min_profit_usd();
        if expected_profit < min_profit_usd {
            self.checks_skipped_profit = self.checks_skipped_profit.saturating_add(1);
            self.log_action_fast(&format!(
                "🧾 ORDER SEND: NO — expected net profit ${expected_profit:.3} is below threshold ${:.3}.",
                min_profit_usd
            ));
            self.request_render();
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

        self.checks_executed = self.checks_executed.saturating_add(1);
        let detect_ts = Utc::now();
        self.execute_arb(opportunity, &mi, size, detect_ts).await;

        self.request_render();
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
            self.last_balance
                .or_else(|| {
                    if self.session_start_balance > 0.0 {
                        Some(self.session_start_balance)
                    } else {
                        None
                    }
                })
                .unwrap_or(0.0)
        };

        let balance_size = balance / (yes_ask + no_ask);
        let mut size = self.effective_max_trade_size();

        if max_liq < size {
            self.log_action_fast(&format!(
                "⚠️ Liquidity limit: only {:.1} shares available at top of book (requested {:.1}).",
                max_liq, size
            ));
            size = max_liq;
        }

        if balance_size < size {
            let requested_before_balance = size;
            let pair_cost = yes_ask + no_ask;
            self.log_action_fast(&format!(
                "⚠️ Balance limit: wallet ${balance:.2} can fund up to {:.1} paired shares at ${pair_cost:.3}/share (requested {:.1} -> capped to {:.1}).",
                balance_size, requested_before_balance, balance_size
            ));
            size = balance_size;
        }

        let active_fee_rate = if self.cached_fee_rate_bps > 0 { self.config.clob_fee_rate } else { 0.0 };
        let fee_yes = active_fee_rate * (yes_ask * (1.0 - yes_ask)).powf(self.config.clob_fee_exponent);
        let fee_no = active_fee_rate * (no_ask * (1.0 - no_ask)).powf(self.config.clob_fee_exponent);
        let max_fee = fee_yes.max(fee_no);

        let min_sellable = Self::env_f64("MIN_PAIRED_SHARES", 5.0).max(5.0);
        let min_order_size = ((min_sellable + 0.5) / (1.0 - max_fee)).ceil();

        size = size.floor();

        if size < min_order_size {
            self.log_action_fast(&format!("⏭ Size {} below minimum ({}). Skipping.", size, min_order_size));
            return 0.0;
        }

        size
    }

    // ─── Arbitrage Execution ───────────────────────────────────────────────────

    async fn execute_arb(
        &mut self,
        opp: ArbOpportunity,
        mi: &MarketInfo,
        total_size: f64,
        detect_ts: chrono::DateTime<Utc>,
    ) {
        let active_fee_rate = if self.cached_fee_rate_bps > 0 { self.config.clob_fee_rate } else { 0.0 };
        let fee_yes = active_fee_rate * (opp.yes_price * (1.0 - opp.yes_price)).powf(self.config.clob_fee_exponent);
        let fee_no = active_fee_rate * (opp.no_price * (1.0 - opp.no_price)).powf(self.config.clob_fee_exponent);
        let max_fee = fee_yes.max(fee_no);
        let min_sellable = Self::env_f64("MIN_PAIRED_SHARES", 5.0).max(5.0);
        let min_order_size = ((min_sellable + 0.5) / (1.0 - max_fee)).ceil();
        let total_size = total_size.floor();
        if total_size < min_order_size {
            self.log_action_fast(&format!(
                "🧾 ORDER SEND: NO — total size {:.0} is below minimum paired size {:.0}.",
                total_size, min_order_size
            ));
            return;
        }
        let planned_batches = Self::plan_taker_batches(total_size);
        if planned_batches.is_empty() {
            self.log_action_fast("🧾 ORDER SEND: NO — no valid taker batch plan.");
            return;
        }
        let batches = planned_batches.len();
        let approx_batch_size = planned_batches
            .iter()
            .copied()
            .reduce(f64::min)
            .unwrap_or(total_size);

        self.log_action_fast("⚡️ Executing Arbitrage Orders...");

        let mut user_balance = self.last_balance.unwrap_or(0.0);
        if user_balance == 0.0 && self.config.mock_currency {
            user_balance = 1000.0;
        }
        self.log_action_fast(&format!("💰 Balance: ${:.2} USDC", user_balance));

        self.log_action_fast(&format!(
            "🎯 Size: {} ({}x{}) | UP@{} DOWN@{}",
            total_size, batches, approx_batch_size, opp.yes_price, opp.no_price
        ));
        self.log_action_fast(&format!(
            "🧾 ORDER SEND: YES — submitting {} batch(es) now.",
            batches
        ));

        if self.config.shadow_engine_enabled && !self.config.shadow_engine_send_orders {
            self.log_action_fast(
                "🧾 ORDER SEND: NO — shadow mode active (`SHADOW_ENGINE_SEND_ORDERS=false`).",
            );
            self.log_action_fast("🕶️ Shadow engine active: execution modeled, order send disabled.");
            return;
        }

        let mut total_profit = 0.0_f64;
        let mut attempted_batches = 0usize;
        let mut successful_batches = 0usize;
        let mut failed_batches = 0;
        let mut partial_fill_failures = 0usize;
        let mut abort_remaining = false;

        for (batch, this_size) in planned_batches.into_iter().enumerate() {
            if abort_remaining {
                break;
            }
            if this_size <= 0.0 {
                continue;
            }
            attempted_batches += 1;

            self.log_action_fast(&format!(
                "📤 Batch {}/{}: taker pair submit (harder-fill-first policy) | {} shares",
                batch + 1, batches, this_size
            ));

            let batch_id = format!("{}:{}:{}", mi.condition_id, Utc::now().timestamp_millis(), batch);
            match self
                .execute_batch(&opp, mi, this_size, total_size, detect_ts, batch_id.clone())
                .await
            {
                Ok(profit) => {
                    total_profit += profit;
                    successful_batches += 1;
                }
                Err(e) => {
                    let err_msg = Self::format_error_chain(&e);
                    if err_msg.contains("Both legs missed") {
                        self.log_action_fast(&format!(
                            "🚫 Batch {}/{}: both legs unfilled after timeout — no position taken",
                            batch + 1, batches
                        ));
                    } else if err_msg.contains("partial fill unresolved") {
                        partial_fill_failures += 1;
                        let reason = Self::truncate_for_action(&err_msg, 180);
                        self.log_action_fast(&format!(
                            "⚠️ Batch {}/{}: partial fill unresolved — {}",
                            batch + 1,
                            batches,
                            reason
                        ));
                        abort_remaining = true;
                    } else if err_msg.contains("marked ambiguous timeout") {
                        self.log_action_fast(&format!(
                            "⚠️ Batch {}/{}: ambiguous timeout — entry locked for deterministic recovery.",
                            batch + 1,
                            batches
                        ));
                        self.errors
                            .push(format!("Batch {} ambiguous timeout: {}", batch + 1, err_msg));
                        abort_remaining = true;
                    } else if err_msg.contains("stale opportunity dropped") {
                        let stale_reason = err_msg
                            .split("stale opportunity dropped:")
                            .nth(1)
                            .map(str::trim)
                            .unwrap_or("reason unavailable");
                        self.log_action_fast(&format!(
                            "⏭ Batch {}/{}: stale before submit ({}) — aborting remaining child orders.",
                            batch + 1, batches, stale_reason
                        ));
                        abort_remaining = true;
                    } else if err_msg.contains("post taker batch timed out")
                        || err_msg.contains("post taker batch failed")
                    {
                        let transport_reason = err_msg
                            .split("post taker batch")
                            .nth(1)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .unwrap_or("reason unavailable");
                        self.log_action_fast(&format!(
                            "⏭ Batch {}/{}: submit transport failed ({}) — aborting remaining child orders.",
                            batch + 1, batches, transport_reason
                        ));
                        self.errors
                            .push(format!("Batch {} transport failure: {}", batch + 1, err_msg));
                        abort_remaining = true;
                    } else {
                        self.log_action_fast(&format!(
                            "⚠️ Batch {}/{} failed: {}",
                            batch + 1,
                            batches,
                            Self::truncate_for_action(&err_msg, 180)
                        ));
                        warn!("Batch {} failed: {}", batch + 1, err_msg);
                        self.errors.push(format!("Batch {}: {}", batch + 1, err_msg));
                    }
                    failed_batches += 1;
                }
            }
        }

        if failed_batches > 0 {
            self.log_action_fast(&format!(
                "❌ {}/{} batches failed to execute UP@{} DOWN@{}.",
                failed_batches, batches, opp.yes_price, opp.no_price
            ));
            if failed_batches == batches {
                if partial_fill_failures > 0 {
                    self.log_action_fast(&format!(
                        "❌ All {} batches failed to close safely ({} with partial fills).",
                        batches, partial_fill_failures
                    ));
                } else {
                    self.log_action_fast(&format!(
                        "❌ All {} batches failed to fill.",
                        batches
                    ));
                }
            }
        }
        self.log_action_fast(&format!(
            "🧾 ORDER SEND RESULT: attempted {} batch(es), successful {}, failed {}.",
            attempted_batches,
            successful_batches,
            failed_batches
        ));

        let (opportunity_success, recorded_pnl) = self.finalize_opportunity_outcome(
            attempted_batches,
            successful_batches,
            total_profit,
        );
        if attempted_batches > 0 {
            self.log_action_fast(&format!(
                "🧾 OPPORTUNITY RESULT: {} (1 {} event) | batches: attempted {}, successful {}, failed {} | net PnL ${:+.3}",
                if opportunity_success { "SUCCESS" } else { "FAILED" },
                if opportunity_success { "success" } else { "failure" },
                attempted_batches,
                successful_batches,
                failed_batches,
                recorded_pnl
            ));
            let pair_cost = opp.yes_price + opp.no_price;
            let raw_edge = 1.0 - pair_cost;
            let hedge_fail = self
                .adaptive_hedge_failure_ratio()
                .map(|r| format!("{:.0}%", r * 100.0))
                .unwrap_or_else(|| "n/a".to_string());
            self.log_action_fast(&format!(
                "🧪 Outcome context: pair ${pair_cost:.3}/share (raw edge ${raw_edge:.3}), size {:.1}, ws age up/down {}ms/{}ms, adaptive min profit ${:.3}, adaptive max size {:.1}, hedge fail {}",
                total_size,
                self.ws_yes_age_ms.unwrap_or(9999),
                self.ws_no_age_ms.unwrap_or(9999),
                self.effective_min_profit_usd(),
                self.effective_max_trade_size(),
                hedge_fail
            ));
        }
    }

    fn finalize_opportunity_outcome(
        &mut self,
        attempted_batches: usize,
        successful_batches: usize,
        total_profit: f64,
    ) -> (bool, f64) {
        if attempted_batches == 0 {
            return (false, 0.0);
        }

        let opportunity_success = successful_batches > 0;
        let recorded_pnl = if opportunity_success { total_profit } else { 0.0 };
        self.stats
            .record_execution(opportunity_success, recorded_pnl);
        self.update_adaptive_stability_controls(opportunity_success, total_profit);

        if opportunity_success {
            self.consecutive_failures = 0;
            self.record_execution_result(total_profit);

            if self.daily_pnl <= -self.config.max_daily_loss_usd {
                let cooldown = Duration::from_millis(self.config.circuit_breaker_cooldown_ms);
                self.circuit_breaker_until = Some(Instant::now() + cooldown);
                warn!("Daily loss limit hit: ${:.4}", self.daily_pnl);
            }
        } else {
            self.consecutive_failures += 1;
            if self.consecutive_failures >= self.config.max_consecutive_failures {
                let cooldown = Duration::from_millis(self.config.circuit_breaker_cooldown_ms);
                self.circuit_breaker_until = Some(Instant::now() + cooldown);
                warn!(
                    "Circuit breaker tripped after {} failed opportunities",
                    self.consecutive_failures
                );
            }
        }

        (opportunity_success, recorded_pnl)
    }

    async fn execute_batch(
        &mut self,
        opp: &ArbOpportunity,
        mi: &MarketInfo,
        size: f64,
        opportunity_size: f64,
        detect_ts: chrono::DateTime<Utc>,
        batch_id: String,
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

        let (yes_book, no_book) = self
            .pre_submit_revalidate(mi, opp, size, opportunity_size, detect_ts)
            .await?;

        let taker_tif = Self::taker_time_in_force();
        let taker_tif_label = Self::taker_tif_label(taker_tif);

        // Sign both legs concurrently (bounded by SDK timeout)
        let sign_start_ts = Utc::now();
        let (yes_result, no_result) = tokio::join!(
            self.run_sdk_call(
                "sign YES order",
                self.client.sign_order(
                    &opp.yes_token_id,
                    opp.yes_price,
                    size,
                    Side::Buy,
                    taker_tif,
                    mi.neg_risk,
                    self.cached_fee_rate_bps,
                )
            ),
            self.run_sdk_call(
                "sign NO order",
                self.client.sign_order(
                    &opp.no_token_id,
                    opp.no_price,
                    size,
                    Side::Buy,
                    taker_tif,
                    mi.neg_risk,
                    self.cached_fee_rate_bps,
                )
            )
        );
        let sign_end_ts = Utc::now();

        let yes_order = yes_result?;
        let no_order = no_result?;

        // Post harder-to-fill leg first using local fillability score from visible depth.
        let yes_fillability = yes_book.ask_liquidity_at(opp.yes_price);
        let no_fillability = no_book.ask_liquidity_at(opp.no_price);
        let yes_posted_first = if yes_fillability.is_finite() && no_fillability.is_finite() {
            yes_fillability <= no_fillability
        } else {
            opp.yes_price >= opp.no_price
        };
        let (first, second) = if yes_posted_first {
            (yes_order, no_order)
        } else {
            (no_order, yes_order)
        };

        // Post both as immediate-taker batch (bounded timeout + retry on transient transport/API faults).
        let post_start_ts = Utc::now();
        self.post_attempt_count = self.post_attempt_count.saturating_add(1);
        let post_timeout_ms = Self::env_u64("SDK_POST_TIMEOUT_MS", SDK_POST_TIMEOUT_MS);
        let post_max_retries = Self::env_u64("POST_BATCH_MAX_RETRIES", POST_RETRY_MAX_RETRIES);
        let mut retry_idx = 0u64;
        let mut first_order = Some(first);
        let mut second_order = Some(second);
        let responses = loop {
            let order_a = first_order
                .take()
                .ok_or_else(|| anyhow!("post taker batch missing first order payload"))?;
            let order_b = second_order
                .take()
                .ok_or_else(|| anyhow!("post taker batch missing second order payload"))?;

            match tokio::time::timeout(
                Duration::from_millis(post_timeout_ms),
                self.client
                    .post_orders(vec![order_a, order_b], taker_tif_label),
            )
            .await
            {
                Ok(Ok(responses)) => break responses,
                Ok(Err(err)) => {
                    let chained = Self::format_error_chain(&err);
                    let retryable = Self::is_retryable_error(&chained);
                    if retryable && retry_idx < post_max_retries {
                        let delay_ms = Self::retry_backoff_ms(retry_idx);
                        warn!(
                            "post taker batch attempt {} failed (retryable): {} — retrying in {}ms",
                            retry_idx + 1,
                            chained,
                            delay_ms
                        );
                        retry_idx += 1;

                        let (yes_retry, no_retry) = tokio::join!(
                            self.run_sdk_call(
                                "re-sign YES order",
                                self.client.sign_order(
                                    &opp.yes_token_id,
                                    opp.yes_price,
                                    size,
                                    Side::Buy,
                                    taker_tif,
                                    mi.neg_risk,
                                    self.cached_fee_rate_bps,
                                )
                            ),
                            self.run_sdk_call(
                                "re-sign NO order",
                                self.client.sign_order(
                                    &opp.no_token_id,
                                    opp.no_price,
                                    size,
                                    Side::Buy,
                                    taker_tif,
                                    mi.neg_risk,
                                    self.cached_fee_rate_bps,
                                )
                            ),
                        );
                        let (yes_retry, no_retry) = (yes_retry?, no_retry?);
                        let (first_retry, second_retry) = if yes_posted_first {
                            (yes_retry, no_retry)
                        } else {
                            (no_retry, yes_retry)
                        };
                        first_order = Some(first_retry);
                        second_order = Some(second_retry);
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        continue;
                    }
                    return Err(err)
                        .with_context(|| format!("post taker batch failed after {} attempt(s)", retry_idx + 1));
                }
                Err(_) => {
                    // Timeout is an ambiguous execution state: the exchange may have accepted
                    // and matched one or both legs even though we didn't receive the response.
                    // Blind retries can multiply one-sided exposure, so we skip retries here.
                    warn!(
                        "post taker batch attempt {} timed out ({}ms); skipping retries to avoid duplicate exposure",
                        retry_idx + 1,
                        post_timeout_ms
                    );
                    let timeout_detected_at = Utc::now();
                    self.register_ambiguous_timeout_batch(
                        &batch_id,
                        mi,
                        size,
                        detect_ts,
                        sign_start_ts,
                        sign_end_ts,
                        post_start_ts,
                        timeout_detected_at,
                        opp,
                    );
                    self.start_timeout_recovery_lock(&batch_id);
                    self.log_action(&format!(
                        "⛔ Timeout recovery lock engaged for batch {} ({}s window)",
                        batch_id,
                        self.config.timeout_recovery_lock_secs
                    ))
                    .await;
                    if let Err(e) = self
                        .run_network_call(
                            "cancel all orders after ambiguous post timeout",
                            0,
                            Self::env_u64("SDK_CANCEL_TIMEOUT_MS", SDK_CANCEL_TIMEOUT_MS),
                            || self.client.cancel_all_orders(),
                        )
                        .await
                    {
                        warn!(
                            "cancel all after post timeout failed: {}",
                            Self::format_error_chain(&e)
                        );
                    }
                    self.run_timeout_recovery_flow(mi, opp, &batch_id).await;
                    anyhow::bail!(
                        "post taker batch timed out after {}ms ({} attempt(s)); marked ambiguous timeout and recovery lock engaged",
                        post_timeout_ms,
                        retry_idx + 1
                    );
                }
            }
        };
        let post_end_ts = Utc::now();

        if responses.is_empty() {
            anyhow::bail!("No responses from post_orders");
        }
        let posted_ids = Self::map_posted_order_ids(yes_posted_first, &responses);
        self.register_posted_batch(
            &batch_id,
            mi,
            size,
            detect_ts,
            sign_start_ts,
            sign_end_ts,
            post_start_ts,
            post_end_ts,
            &posted_ids,
            opp,
        );
        let detect_to_submit_ms = (post_end_ts - detect_ts).num_milliseconds().max(0) as u64;
        Self::push_latency_value(&mut self.detect_to_submit_latency_ms, detect_to_submit_ms);

        // Wait for fills
        let timeout_ms = self
            .config
            .gtc_taker_timeout_ms
            .min(Self::env_u64("ORDER_STATUS_TIMEOUT_MS", ORDER_STATUS_TIMEOUT_MS));
        let (yes_filled, no_filled) = self
            .wait_for_fills_event_driven(&posted_ids, size, timeout_ms, &batch_id)
            .await;

        // Cancel any remaining open orders
        for resp in &responses {
            if !resp.order_id.is_empty() {
                let matched = resp.size_matched.as_deref().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                if matched < size * 0.99 {
                    if let Err(e) = self
                        .run_network_call(
                            "cancel residual order",
                            Self::env_u64("CANCEL_ORDER_MAX_RETRIES", CANCEL_RETRY_MAX_RETRIES),
                            Self::env_u64("SDK_CANCEL_TIMEOUT_MS", SDK_CANCEL_TIMEOUT_MS),
                            || self.client.cancel_order(&resp.order_id),
                        )
                        .await
                    {
                        debug!(
                            "Residual cancel failed for {}: {}",
                            resp.order_id,
                            Self::format_error_chain(&e)
                        );
                    }
                }
            }
        }

        let cost = yes_filled * opp.yes_price + no_filled * opp.no_price;

        if yes_filled >= size * 0.95 && no_filled >= size * 0.95 {
            self.post_paired_count = self.post_paired_count.saturating_add(1);
            // Both legs filled — merge
            let mergeable = actual_payout_size.min(yes_filled).min(no_filled);
            self.position.yes_size += yes_filled;
            self.position.no_size += no_filled;
            self.position.yes_cost += yes_filled * opp.yes_price;
            self.position.no_cost += no_filled * opp.no_price;
            self.note_position_activity();
            self.active_assets.insert(mi.tokens.yes.clone());
            self.active_assets.insert(mi.tokens.no.clone());

            // Fire merge in background
            let fire_merge_amount = self.mergeable_available();
            self.fire_merge(fire_merge_amount, mi).await;
            if let Some(batch) = self.execution_batches.get_mut(&batch_id) {
                batch.state = ExecutionState::MergePending;
                batch.realized_pnl_usd = Some(mergeable * 1.0 - cost - self.gas_cache.fee_per_merge_usd);
            }

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
                "✅ ARB fill: Buy UP {yes_filled:.2} @ {:.2} + DOWN {no_filled:.2} @ {:.2} | entry ${cost:.2}",
                opp.yes_price,
                opp.no_price
            );
            self.log_action(&fill_msg).await;

            let expected_payout = mergeable;
            let gross_profit = expected_payout - cost;
            let roi_pct = if cost > 0.0 { (profit / cost) * 100.0 } else { 0.0 };
            self.log_action(&format!(
                "📦 Exit estimate: ${expected_payout:.2} payout (merge) | gross ${gross_profit:+.2} | net ${profit:+.2} ({roi_pct:+.2}%)"
            ))
            .await;
            if let Some(batch) = self.execution_batches.get_mut(&batch_id) {
                batch.state = ExecutionState::Closed;
            }

            Ok(profit)
        } else if yes_filled > 0.05 || no_filled > 0.05 {
            // Partial fill — hedge the missing leg
            self.hedge_event_count = self.hedge_event_count.saturating_add(1);
            if let Some(batch) = self.execution_batches.get_mut(&batch_id) {
                batch.state = ExecutionState::HedgePending;
            }
            self.handle_partial_fill(
                yes_filled, no_filled, opp, mi, self.cached_fee_rate_bps, cost,
            )
            .await
        } else {
            if let Some(batch) = self.execution_batches.get_mut(&batch_id) {
                batch.state = ExecutionState::Failed;
            }
            anyhow::bail!("Both legs missed (yes={yes_filled:.2} no={no_filled:.2})")
        }
    }

    /// Event-driven fill wait: User WS events primary, REST polling fallback on staleness.
    async fn wait_for_fills_event_driven(
        &mut self,
        posted_ids: &PostedOrderIds,
        target_size: f64,
        timeout_ms: u64,
        batch_id: &str,
    ) -> (f64, f64) {
        let start = Instant::now();
        let poll_interval_ms = self.config.ws_fill_fallback_poll_ms.max(50);
        let mut last_fallback_poll =
            Instant::now() - Duration::from_millis(self.config.ws_fill_fallback_poll_ms.max(1));
        let mut yes_filled = 0.0_f64;
        let mut no_filled = 0.0_f64;

        while start.elapsed().as_millis() < timeout_ms as u128 {
            self.process_user_events().await;

            if let Some(yes_leg) = self.order_tracker.get(&posted_ids.yes_order_id) {
                yes_filled = yes_leg.matched_size;
            }
            if let Some(no_leg) = self.order_tracker.get(&posted_ids.no_order_id) {
                no_filled = no_leg.matched_size;
            }

            let ws_stale = self
                .ws_fill_last_event_at
                .map(|t| t.elapsed() > Duration::from_millis(self.config.ws_fill_fallback_poll_ms))
                .unwrap_or(true);
            if (!self.config.ws_fill_primary || ws_stale)
                && last_fallback_poll.elapsed() >= Duration::from_millis(poll_interval_ms)
            {
                self.stale_poll_fallback_count = self.stale_poll_fallback_count.saturating_add(1);
                let (yes_polled, no_polled) = self.poll_fills_once(posted_ids).await;
                if let Some(v) = yes_polled {
                    self.update_order_fill_state(&posted_ids.yes_order_id, v, Utc::now(), false);
                    yes_filled = yes_filled.max(v);
                }
                if let Some(v) = no_polled {
                    self.update_order_fill_state(&posted_ids.no_order_id, v, Utc::now(), false);
                    no_filled = no_filled.max(v);
                }
                last_fallback_poll = Instant::now();
            }

            if Self::fills_sufficient(yes_filled, no_filled, target_size) {
                if let Some(batch) = self.execution_batches.get_mut(batch_id) {
                    batch.state = ExecutionState::Paired;
                    let now = Utc::now();
                    batch.paired_complete_ts = Some(now);
                    if let Some(first_fill_ts) = batch.first_fill_event_ts {
                        let first_fill_ms = (first_fill_ts - batch.detect_ts).num_milliseconds().max(0) as u64;
                        Self::push_latency_value(&mut self.first_fill_latency_ms, first_fill_ms);
                    }
                    let pair_ms = (now - batch.detect_ts).num_milliseconds().max(0) as u64;
                    Self::push_latency_value(&mut self.pair_complete_latency_ms, pair_ms);
                }
                break;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        (yes_filled, no_filled)
    }

    async fn poll_fills_once(&self, posted_ids: &PostedOrderIds) -> (Option<f64>, Option<f64>) {
        let yes_future = async {
            if posted_ids.yes_order_id.is_empty() {
                return None;
            }
            match self
                .run_network_call(
                    "poll YES order status",
                    Self::env_u64("ORDER_LOOKUP_MAX_RETRIES", CANCEL_RETRY_MAX_RETRIES),
                    Self::env_u64("SDK_ORDER_LOOKUP_TIMEOUT_MS", SDK_ORDER_LOOKUP_TIMEOUT_MS),
                    || self.client.get_order(&posted_ids.yes_order_id),
                )
                .await
            {
                Ok(maybe_order) => maybe_order.map(|x| x.matched_f64()),
                Err(e) => {
                    debug!("poll YES order status failed: {}", Self::format_error_chain(&e));
                    None
                }
            }
        };
        let no_future = async {
            if posted_ids.no_order_id.is_empty() {
                return None;
            }
            match self
                .run_network_call(
                    "poll NO order status",
                    Self::env_u64("ORDER_LOOKUP_MAX_RETRIES", CANCEL_RETRY_MAX_RETRIES),
                    Self::env_u64("SDK_ORDER_LOOKUP_TIMEOUT_MS", SDK_ORDER_LOOKUP_TIMEOUT_MS),
                    || self.client.get_order(&posted_ids.no_order_id),
                )
                .await
            {
                Ok(maybe_order) => maybe_order.map(|x| x.matched_f64()),
                Err(e) => {
                    debug!("poll NO order status failed: {}", Self::format_error_chain(&e));
                    None
                }
            }
        };
        tokio::join!(yes_future, no_future)
    }

    fn register_posted_batch(
        &mut self,
        batch_id: &str,
        mi: &MarketInfo,
        size: f64,
        detect_ts: chrono::DateTime<Utc>,
        sign_start_ts: chrono::DateTime<Utc>,
        sign_end_ts: chrono::DateTime<Utc>,
        post_start_ts: chrono::DateTime<Utc>,
        post_end_ts: chrono::DateTime<Utc>,
        posted_ids: &PostedOrderIds,
        opp: &ArbOpportunity,
    ) {
        let now = Utc::now();
        let yes_leg = OrderLegState {
            token_id: mi.tokens.yes.clone(),
            order_id: posted_ids.yes_order_id.clone(),
            target_size: size,
            matched_size: 0.0,
            last_update_ts: now,
        };
        let no_leg = OrderLegState {
            token_id: mi.tokens.no.clone(),
            order_id: posted_ids.no_order_id.clone(),
            target_size: size,
            matched_size: 0.0,
            last_update_ts: now,
        };
        if !posted_ids.yes_order_id.is_empty() {
            self.order_tracker
                .insert(posted_ids.yes_order_id.clone(), yes_leg.clone());
            self.order_to_batch
                .insert(posted_ids.yes_order_id.clone(), batch_id.to_string());
        }
        if !posted_ids.no_order_id.is_empty() {
            self.order_tracker
                .insert(posted_ids.no_order_id.clone(), no_leg.clone());
            self.order_to_batch
                .insert(posted_ids.no_order_id.clone(), batch_id.to_string());
        }

        self.execution_batches.insert(
            batch_id.to_string(),
            ExecutionBatchState {
                batch_id: batch_id.to_string(),
                condition_id: mi.condition_id.clone(),
                state: ExecutionState::Posted,
                yes_leg,
                no_leg,
                detect_ts,
                sign_start_ts: Some(sign_start_ts),
                sign_end_ts: Some(sign_end_ts),
                post_start_ts: Some(post_start_ts),
                post_end_ts: Some(post_end_ts),
                first_fill_event_ts: None,
                paired_complete_ts: None,
                expected_pnl_usd: Some(size - (opp.total_cost * size) - self.gas_cache.fee_per_merge_usd),
                realized_pnl_usd: None,
            },
        );
    }

    fn register_ambiguous_timeout_batch(
        &mut self,
        batch_id: &str,
        mi: &MarketInfo,
        size: f64,
        detect_ts: chrono::DateTime<Utc>,
        sign_start_ts: chrono::DateTime<Utc>,
        sign_end_ts: chrono::DateTime<Utc>,
        post_start_ts: chrono::DateTime<Utc>,
        post_end_ts: chrono::DateTime<Utc>,
        opp: &ArbOpportunity,
    ) {
        let now = Utc::now();
        let yes_leg = OrderLegState {
            token_id: mi.tokens.yes.clone(),
            order_id: String::new(),
            target_size: size,
            matched_size: 0.0,
            last_update_ts: now,
        };
        let no_leg = OrderLegState {
            token_id: mi.tokens.no.clone(),
            order_id: String::new(),
            target_size: size,
            matched_size: 0.0,
            last_update_ts: now,
        };
        self.execution_batches.insert(
            batch_id.to_string(),
            ExecutionBatchState {
                batch_id: batch_id.to_string(),
                condition_id: mi.condition_id.clone(),
                state: ExecutionState::AmbiguousTimeout,
                yes_leg,
                no_leg,
                detect_ts,
                sign_start_ts: Some(sign_start_ts),
                sign_end_ts: Some(sign_end_ts),
                post_start_ts: Some(post_start_ts),
                post_end_ts: Some(post_end_ts),
                first_fill_event_ts: None,
                paired_complete_ts: None,
                expected_pnl_usd: Some(size - (opp.total_cost * size) - self.gas_cache.fee_per_merge_usd),
                realized_pnl_usd: None,
            },
        );
    }

    async fn run_timeout_recovery_flow(&mut self, mi: &MarketInfo, opp: &ArbOpportunity, batch_id: &str) {
        let lock_secs = self.config.timeout_recovery_lock_secs.max(1);
        let poll_ms = self.config.timeout_recovery_poll_ms.max(50);
        let deadline = Instant::now() + Duration::from_secs(lock_secs);

        while Instant::now() < deadline {
            self.process_user_events().await;
            let _ = self
                .apply_rest_trade_deltas(mi, "timeout recovery")
                .await;
            tokio::time::sleep(Duration::from_millis(poll_ms)).await;
        }

        self.process_user_events().await;
        let _ = self
            .apply_rest_trade_deltas(mi, "timeout recovery finalization")
            .await;

        let (yes_ask, no_ask) = match self.fetch_books(mi).await {
            Some((yes_book, no_book)) => {
                let yes_mid = yes_book
                    .best_bid()
                    .zip(yes_book.best_ask())
                    .map(|(bid, ask)| (bid + ask) * 0.5)
                    .or_else(|| yes_book.best_ask())
                    .unwrap_or(opp.yes_price);
                let no_mid = no_book
                    .best_bid()
                    .zip(no_book.best_ask())
                    .map(|(bid, ask)| (bid + ask) * 0.5)
                    .or_else(|| no_book.best_ask())
                    .unwrap_or(opp.no_price);
                self.last_yes_mid_price = Some(yes_mid);
                self.last_no_mid_price = Some(no_mid);
                (
                    yes_book.best_ask().unwrap_or(opp.yes_price),
                    no_book.best_ask().unwrap_or(opp.no_price),
                )
            }
            None => (opp.yes_price, opp.no_price),
        };

        if self.position.has_imbalance() {
            self.handle_imbalance(mi, yes_ask, no_ask).await;
        }
        if self.mergeable_available() >= 1.0 {
            self.fire_merge(self.mergeable_available(), mi).await;
        }

        self.timeout_recovery_active = false;
        self.entry_lock_until = None;

        let resolved = self.in_neutrality_resume_band() && !self.reconcile_drift_lock_active();
        if resolved {
            self.unresolved_ambiguous_batches.remove(batch_id);
            self.log_action(&format!(
                "✅ Timeout recovery completed for batch {} — entry lock released",
                batch_id
            ))
            .await;
            if let Some(batch) = self.execution_batches.get_mut(batch_id) {
                batch.state = if self.position.has_imbalance() {
                    ExecutionState::Partial
                } else {
                    ExecutionState::Failed
                };
            }
        } else {
            self.log_action(&format!(
                "⚠️ Timeout recovery ended with unresolved inventory (imbalance {:.3}); keeping entry lock active",
                self.net_imbalance_shares()
            ))
            .await;
        }
    }

    fn update_order_fill_state(
        &mut self,
        order_id: &str,
        matched: f64,
        now: chrono::DateTime<Utc>,
        from_ws: bool,
    ) {
        let mut fill_notice: Option<(String, f64, f64, f64)> = None;

        if let Some(leg) = self.order_tracker.get_mut(order_id) {
            if matched >= leg.matched_size {
                leg.matched_size = matched;
                leg.last_update_ts = now;
            }
        }
        let Some(batch_id) = self.order_to_batch.get(order_id).cloned() else {
            return;
        };
        if let Some(batch) = self.execution_batches.get_mut(&batch_id) {
            if batch.yes_leg.order_id == order_id {
                let prev = batch.yes_leg.matched_size;
                batch.yes_leg.matched_size = batch.yes_leg.matched_size.max(matched);
                batch.yes_leg.last_update_ts = now;
                if batch.yes_leg.matched_size > prev + 0.000_1 {
                    fill_notice = Some((
                        "UP".to_string(),
                        batch.yes_leg.matched_size - prev,
                        batch.yes_leg.matched_size,
                        batch.yes_leg.target_size,
                    ));
                }
            }
            if batch.no_leg.order_id == order_id {
                let prev = batch.no_leg.matched_size;
                batch.no_leg.matched_size = batch.no_leg.matched_size.max(matched);
                batch.no_leg.last_update_ts = now;
                if batch.no_leg.matched_size > prev + 0.000_1 {
                    fill_notice = Some((
                        "DOWN".to_string(),
                        batch.no_leg.matched_size - prev,
                        batch.no_leg.matched_size,
                        batch.no_leg.target_size,
                    ));
                }
            }
            if batch.first_fill_event_ts.is_none()
                && (batch.yes_leg.matched_size > 0.0 || batch.no_leg.matched_size > 0.0)
            {
                batch.first_fill_event_ts = Some(now);
            }
            if batch.yes_leg.matched_size > 0.0 || batch.no_leg.matched_size > 0.0 {
                batch.state = ExecutionState::Partial;
            }
        }

        if let Some((leg_label, delta, total, target)) = fill_notice {
            let source = if from_ws { "WS" } else { "REST poll" };
            self.log_action_fast(&format!(
                "✅ Fill ({source}): {leg_label} +{delta:.3} (total {total:.3}/{target:.3})"
            ));
        }
        if from_ws {
            self.ws_fill_last_event_at = Some(Instant::now());
        }
    }

    fn apply_untracked_ws_trade_fill(&mut self, ev: &crate::types::WsTradeEvent) {
        let _ = self.apply_ws_trade_fill_to_position(ev, "Untracked WS fill");
    }

    fn apply_ws_trade_fill_to_position(
        &mut self,
        ev: &crate::types::WsTradeEvent,
        reason: &str,
    ) -> bool {
        let trade_size = ev.size.parse::<f64>().unwrap_or(0.0);
        if trade_size <= 0.0 {
            return false;
        }
        let trade_price = ev.price.parse::<f64>().unwrap_or(0.0);
        let side = ev.side.to_ascii_uppercase();

        let Some(mi) = self.market_info.as_ref() else {
            self.log_action_fast(&format!(
                "⚠️ {reason}: {side} {trade_size:.3} @ {trade_price:.3} (no active market cached)"
            ));
            return false;
        };

        if ev.market != mi.condition_id {
            self.log_action_fast(&format!(
                "⚠️ {reason} on non-active market: {side} {trade_size:.3} @ {trade_price:.3}"
            ));
            return false;
        }

        let (leg_label, size_ref, cost_ref) = if ev.asset_id == mi.tokens.yes {
            ("UP", &mut self.position.yes_size, &mut self.position.yes_cost)
        } else if ev.asset_id == mi.tokens.no {
            ("DOWN", &mut self.position.no_size, &mut self.position.no_cost)
        } else {
            self.log_action_fast(&format!(
                "⚠️ {reason} for unknown asset {}: {side} {trade_size:.3} @ {trade_price:.3}",
                ev.asset_id
            ));
            return false;
        };

        let applied_size =
            Self::apply_position_trade_delta(size_ref, cost_ref, &side, trade_size, trade_price);
        if applied_size > 0.0 {
            self.note_position_activity();
            self.active_assets.insert(ev.asset_id.clone());
            self.log_action_fast(&format!(
                "⚠️ {reason}: {side} {applied_size:.3} {leg_label} @ {trade_price:.3} — position adjusted"
            ));
            true
        } else {
            self.log_action_fast(&format!(
                "⚠️ {reason}: ignored {side} {trade_size:.3} {leg_label} @ {trade_price:.3} (no inventory to reduce)"
            ));
            false
        }
    }

    fn apply_position_trade_delta(
        size: &mut f64,
        cost: &mut f64,
        side: &str,
        trade_size: f64,
        trade_price: f64,
    ) -> f64 {
        if trade_size <= 0.0 {
            return 0.0;
        }
        match side {
            "BUY" => {
                *size += trade_size;
                *cost += trade_size * trade_price;
                trade_size
            }
            "SELL" => {
                let reducible = trade_size.min(*size);
                if reducible <= 0.0 {
                    return 0.0;
                }
                let avg_cost = if *size > 0.0 { *cost / *size } else { 0.0 };
                *size = (*size - reducible).max(0.0);
                *cost = (*cost - avg_cost * reducible).max(0.0);
                if *size <= 0.01 {
                    *size = 0.0;
                    *cost = 0.0;
                }
                reducible
            }
            _ => 0.0,
        }
    }

    fn mark_rest_trade_seen(&mut self, trade_id: &str) -> bool {
        if trade_id.is_empty() {
            return true;
        }
        if self.seen_rest_trade_ids.contains(trade_id) {
            return false;
        }
        let owned = trade_id.to_string();
        self.seen_rest_trade_ids.insert(owned.clone());
        self.rest_trade_id_queue.push_back(owned);
        while self.rest_trade_id_queue.len() > MAX_SEEN_WS_TRADE_IDS {
            if let Some(evicted) = self.rest_trade_id_queue.pop_front() {
                self.seen_rest_trade_ids.remove(&evicted);
            }
        }
        true
    }

    fn apply_trade_record_to_position(
        &mut self,
        trade: &crate::types::TradeRecord,
        mi: &MarketInfo,
    ) -> bool {
        let trade_size = trade.size.parse::<f64>().unwrap_or(0.0);
        let trade_price = trade.price.parse::<f64>().unwrap_or(0.0);
        if trade_size <= 0.0 || trade_price <= 0.0 {
            return false;
        }

        let side = trade.side.to_ascii_uppercase();
        let (size_ref, cost_ref, asset_id) = if trade.token_id == mi.tokens.yes {
            (
                &mut self.position.yes_size,
                &mut self.position.yes_cost,
                mi.tokens.yes.clone(),
            )
        } else if trade.token_id == mi.tokens.no {
            (
                &mut self.position.no_size,
                &mut self.position.no_cost,
                mi.tokens.no.clone(),
            )
        } else {
            return false;
        };

        let applied = Self::apply_position_trade_delta(size_ref, cost_ref, &side, trade_size, trade_price);
        if applied > 0.0 {
            self.active_assets.insert(asset_id);
            self.note_position_activity();
            true
        } else {
            false
        }
    }

    async fn apply_rest_trade_deltas(&mut self, mi: &MarketInfo, reason: &str) -> usize {
        let trades = match self.client.get_trades(Some(&mi.condition_id)).await {
            Ok(mut t) => {
                t.truncate(REST_RECOVERY_POLL_LOOKBACK_MAX_TRADES);
                t
            }
            Err(e) => {
                warn!(
                    "REST trade delta poll failed during {reason}: {}",
                    Self::format_error_chain(&e)
                );
                return 0;
            }
        };

        let mut applied = 0usize;
        for trade in trades {
            if !self.mark_rest_trade_seen(&trade.id) {
                continue;
            }
            if self.apply_trade_record_to_position(&trade, mi) {
                applied += 1;
            }
        }
        if applied > 0 {
            self.log_action(&format!(
                "⚠️ REST reconcile applied {applied} trade delta(s) during {reason}"
            ))
            .await;
        }
        applied
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
        self.log_action(&format!(
            "⚠️ Partial fill detected: UP {:.2} / DOWN {:.2} — attempting auto-rescue.",
            yes_filled, no_filled
        ))
        .await;

        let result = if yes_filled > no_filled {
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
        };

        self.record_hedge_outcome(result.is_ok());

        if let Err(e) = &result {
            // Rescue failed: retain observed partial inventory in memory so dashboard reflects reality.
            if yes_filled > 0.0 {
                self.position.yes_size += yes_filled;
                self.position.yes_cost += yes_filled * opp.yes_price;
                self.active_assets.insert(mi.tokens.yes.clone());
            }
            if no_filled > 0.0 {
                self.position.no_size += no_filled;
                self.position.no_cost += no_filled * opp.no_price;
                self.active_assets.insert(mi.tokens.no.clone());
            }
            if yes_filled > 0.0 || no_filled > 0.0 {
                self.note_position_activity();
            }

            let reason = Self::truncate_for_action(&Self::format_error_chain(e), 180);
            self.log_action(&format!(
                "⚠️ Auto-rescue failed; unpaired position remains (UP {:.2} / DOWN {:.2}) — {}",
                yes_filled, no_filled, reason
            ))
            .await;
        }

        result.with_context(|| {
            format!(
                "partial fill unresolved (yes={yes_filled:.3}, no={no_filled:.3})"
            )
        })
    }

    async fn execute_buy_hedge_ladder(
        &mut self,
        missing_token_id: &str,
        mi: &MarketInfo,
        fee_rate_bps: u64,
        missing_price: f64,
        hedge_target_net: f64,
        min_buy_size: f64,
        context_label: &str,
    ) -> Result<(f64, f64, f64, bool)> {
        let tick = mi.tick_size.max(0.001);
        let hedge_tif = Self::hedge_time_in_force();
        let hedge_tif_label = Self::tif_label(hedge_tif).to_string();
        let mut net_received_total = 0.0;
        let mut gross_matched_total = 0.0;
        let mut gross_cost_total = 0.0;
        let mut last_error: Option<anyhow::Error> = None;

        for (idx, px_mult) in [1.03_f64, 1.06_f64].iter().enumerate() {
            let remaining_target = (hedge_target_net - net_received_total).max(0.0);
            if remaining_target <= 0.01 {
                break;
            }

            let hedge_buy_price = Self::snap_price_to_tick(missing_price * px_mult, tick, true)
                .min(1.0 - tick);
            let Some(hedge_order_size) = self.gross_buy_size_for_target_net(
                remaining_target,
                hedge_buy_price,
                min_buy_size,
            ) else {
                last_error = Some(anyhow!(
                    "{} hedge target {:.2} cannot be represented with valid market BUY precision at {:.3}",
                    context_label,
                    remaining_target,
                    hedge_buy_price
                ));
                continue;
            };

            let hedge_order = match self
                .run_sdk_call(
                    "sign ladder hedge order",
                    self.client.sign_order(
                        missing_token_id,
                        hedge_buy_price,
                        hedge_order_size,
                        Side::Buy,
                        hedge_tif,
                        mi.neg_risk,
                        fee_rate_bps,
                    ),
                )
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    last_error = Some(e);
                    continue;
                }
            };

            let post = self
                .run_network_call(
                    "post ladder hedge order",
                    0,
                    Self::env_u64("SDK_POST_TIMEOUT_MS", SDK_POST_TIMEOUT_MS),
                    {
                        let client = Arc::clone(&self.client);
                        let mut maybe_order = Some(hedge_order);
                        let tif = hedge_tif_label.clone();
                        move || {
                            let client = Arc::clone(&client);
                            let tif = tif.clone();
                            let order = maybe_order.take();
                            async move {
                                let order = order
                                    .ok_or_else(|| anyhow!("ladder hedge order missing payload"))?;
                                client.post_order(order, &tif).await
                            }
                        }
                    },
                )
                .await;
            let resp = match post {
                Ok(v) => v,
                Err(e) => {
                    last_error = Some(e);
                    continue;
                }
            };

            let matched = resp
                .size_matched
                .as_deref()
                .unwrap_or("0")
                .parse::<f64>()
                .unwrap_or(0.0);
            if matched > 0.0 {
                self.mark_response_fill_applied(&resp.order_id);
                let net_received = self.fee_adjust_shares(matched, hedge_buy_price);
                gross_matched_total += matched;
                net_received_total += net_received;
                gross_cost_total += matched * hedge_buy_price;
                self.log_action(&format!(
                    "✅ {} ladder fill A{}: gross {:.2} @ {:.2} (net +{:.2})",
                    context_label,
                    idx + 1,
                    matched,
                    hedge_buy_price,
                    net_received
                ))
                .await;
            } else {
                self.log_action(&format!(
                    "⏭ {} ladder A{} unfilled @ {:.2}",
                    context_label,
                    idx + 1,
                    hedge_buy_price
                ))
                .await;
            }

            if idx == 0 && net_received_total + 0.01 < hedge_target_net {
                tokio::time::sleep(Duration::from_millis(120)).await;
            }
        }

        if net_received_total <= 0.0 {
            if let Some(err) = last_error {
                return Err(err).context(format!("{context_label} ladder failed"));
            }
            anyhow::bail!("{context_label} ladder posted but remained unfilled");
        }
        let resolved = net_received_total + 0.01 >= hedge_target_net;
        Ok((gross_matched_total, net_received_total, gross_cost_total, resolved))
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
        let min_sell_size = Self::env_f64("MIN_SELL_RESCUE_SHARES", 5.0).max(5.0);
        let min_buy_size = Self::env_f64("MIN_RESCUE_BUY_SHARES", 1.0).max(0.01);
        let order_size = ((size * 100.0).floor() / 100.0).max(0.0);
        if order_size < min_buy_size {
            anyhow::bail!(
                "rescue size {:.3} below minimum hedge-buyable {:.2} shares",
                order_size,
                min_buy_size
            );
        }

        // Polymarket taker fee can leave fewer tokens than raw matched size.
        // Clamp sell-back order size to estimated net-holdings after fee.
        let sellable_size = ((self.fee_adjust_shares(order_size, filled_price).min(order_size) * 100.0).floor()
            / 100.0)
            .max(0.0);
        let mut can_sell_back = sellable_size >= min_sell_size;
        if can_sell_back && !self.config.mock_currency {
            let rpc = crate::clob_client::POLYGON_RPCS[0];
            let holder = self.client.maker_address();
            match self.client.get_ctf_balance(filled_token_id, holder, rpc).await {
                Ok(on_chain) => {
                    if on_chain + 0.01 < sellable_size {
                        can_sell_back = false;
                        self.log_action(&format!(
                            "⏭ Rescue sell-back deferred: on-chain balance {:.2} < sellable {:.2} (relayer delay); forcing BUY hedge.",
                            on_chain, sellable_size
                        ))
                        .await;
                    }
                }
                Err(e) => {
                    warn!(
                        "Rescue sell-back balance precheck failed (continuing): {}",
                        Self::format_error_chain(&e)
                    );
                }
            }
        }

        // Estimate hedge and sell-back using live depth when available.
        // Hedge BUY size must be grossed up so post-fee received shares match
        // the missing leg inventory.
        let hedge_target_net = sellable_size.min(order_size).max(0.0);
        let hedge_quote_size_for_estimate = self
            .gross_buy_size_for_target_net(hedge_target_net, missing_price, min_buy_size)
            .unwrap_or(order_size.max(min_buy_size));
        let mut expected_missing_cost = missing_price * hedge_quote_size_for_estimate;
        let mut expected_sell_proceeds = filled_price * sellable_size * 0.95;
        if let Some((yes_book, no_book)) = self.fetch_books(mi).await {
            if missing_token_id == mi.tokens.yes {
                expected_missing_cost =
                    Self::estimate_buy_cost_from_book(&yes_book, hedge_quote_size_for_estimate)
                    .unwrap_or(expected_missing_cost);
            } else {
                expected_missing_cost =
                    Self::estimate_buy_cost_from_book(&no_book, hedge_quote_size_for_estimate)
                    .unwrap_or(expected_missing_cost);
            }
            if filled_token_id == mi.tokens.yes && sellable_size > 0.0 {
                expected_sell_proceeds = Self::estimate_sell_proceeds_from_book(&yes_book, sellable_size)
                    .unwrap_or(expected_sell_proceeds);
            } else if sellable_size > 0.0 {
                expected_sell_proceeds = Self::estimate_sell_proceeds_from_book(&no_book, sellable_size)
                    .unwrap_or(expected_sell_proceeds);
            }
        }

        // Hedge P&L: buy missing leg and merge.
        let actual_size_missing =
            self.fee_adjust_shares(hedge_quote_size_for_estimate, missing_price);
        let actual_size_filled = hedge_target_net;
        let mergeable = actual_size_missing.min(actual_size_filled);
        let hedge_pnl = mergeable * 1.0
            - filled_cost
            - expected_missing_cost
            - self.gas_cache.fee_per_merge_usd;

        // Sell-back P&L from expected executable bid depth.
        let sell_cost_basis = if order_size > 0.0 {
            filled_cost * (sellable_size / order_size)
        } else {
            0.0
        };
        let sell_pnl = expected_sell_proceeds - sell_cost_basis;

        let sold_back = can_sell_back && sell_pnl >= hedge_pnl;
        if !can_sell_back {
            self.log_action(&format!(
                "⏭ Rescue sell-back skipped: sellable {:.3} < min {:.1} shares; forcing BUY hedge.",
                sellable_size, min_sell_size
            ))
            .await;
        }
        let tick = mi.tick_size.max(0.001);

        let result = if sold_back {
            // Sell back filled leg
            let sell_back_price = Self::snap_price_to_tick(filled_price * 0.99, tick, false)
                .max(tick);
            let hedge_tif = Self::hedge_time_in_force();
            let hedge_tif_label = Self::tif_label(hedge_tif);
            let sell_order = self
                .run_sdk_call(
                    "sign sell-back order",
                    self.client.sign_order(
                        filled_token_id,
                        sell_back_price,
                        sellable_size,
                        Side::Sell,
                        hedge_tif,
                        mi.neg_risk,
                        fee_rate_bps,
                    ),
                )
                .await?;
            let resp = self
                .run_network_call("post sell-back order", 0, Self::env_u64("SDK_POST_TIMEOUT_MS", SDK_POST_TIMEOUT_MS), {
                    let client = Arc::clone(&self.client);
                    let mut maybe_order = Some(sell_order);
                    let tif = hedge_tif_label.to_string();
                    move || {
                        let client = Arc::clone(&client);
                        let tif = tif.clone();
                        let order = maybe_order.take();
                        async move {
                            let order =
                                order.ok_or_else(|| anyhow!("sell-back order missing payload"))?;
                            client.post_order(order, &tif).await
                        }
                    }
                })
                .await;
            let resp = match resp {
                Ok(resp) => resp,
                Err(e) => {
                    if Self::is_balance_allowance_rejection(&e) {
                        self.log_action(
                            "⚠️ Rescue sell-back rejected (balance/allowance). Keeping normal smart-hedge mode.",
                        )
                        .await;
                    }
                    return Err(e);
                }
            };
            let matched = resp.size_matched.as_deref().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
            if matched > 0.0 {
                self.mark_response_fill_applied(&resp.order_id);
            }
            if matched <= 0.0 {
                anyhow::bail!("rescue sell-back posted but unfilled");
            }
            let proceeds = matched * sell_back_price;
            self.log_action(&format!(
                "✅ Rescue sell-back fill: {matched:.2} @ {:.2}",
                sell_back_price
            ))
            .await;
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
            let affordable_target_net = self.affordable_net_buy_shares(missing_price);
            let hedge_target_net = hedge_target_net.min(affordable_target_net);
            if hedge_target_net + 0.01 < min_buy_size {
                anyhow::bail!(
                    "rescue hedge skipped: affordable net {:.2} < min {:.2} (balance ${:.2})",
                    hedge_target_net,
                    min_buy_size,
                    self.last_balance.unwrap_or(0.0)
                );
            }
            let (gross_hedge_matched, hedge_received, gross_hedge_cost, resolved) = self
                .execute_buy_hedge_ladder(
                    missing_token_id,
                    mi,
                    fee_rate_bps,
                    missing_price,
                    hedge_target_net,
                    min_buy_size,
                    "Rescue",
                )
                .await?;
            let paired_size = hedge_received.min(hedge_target_net);
            self.log_action(&format!(
                "✅ Rescue hedge fill: gross {:.2} (net +{hedge_received:.2})",
                gross_hedge_matched,
            ))
            .await;

            if filled_is_yes {
                self.position.yes_size += paired_size;
                self.position.no_size += paired_size;
            } else {
                self.position.no_size += paired_size;
                self.position.yes_size += paired_size;
            }
            self.note_position_activity();
            self.active_assets.insert(mi.tokens.yes.clone());
            self.active_assets.insert(mi.tokens.no.clone());
            let fire_amount = self.mergeable_available();
            self.fire_merge(fire_amount, mi).await;
            self.stats.record_hedge();

            let actual_matched_missing = hedge_received;
            let actual_matched_filled = hedge_target_net;
            let actual_mergeable = actual_matched_missing.min(actual_matched_filled);
            let actual_hedge_pnl =
                actual_mergeable * 1.0 - filled_cost - gross_hedge_cost - self.gas_cache.fee_per_merge_usd;
            self.trade_logger
                .log_hedge(
                    &mi.condition_id,
                    Some(&mi.question),
                    actual_mergeable,
                    filled_cost + gross_hedge_cost,
                    actual_hedge_pnl,
                    false,
                    true,
                    None,
                )
                .await;
            if !resolved {
                anyhow::bail!(
                    "rescue ladder unresolved: net {:.2}/{:.2} filled",
                    hedge_received,
                    hedge_target_net
                );
            }
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
        let (missing_token, missing_label, missing_price, filled_token, filled_label, filled_price, filled_size_total, filled_cost_total) =
            if yes_excess > 0.0 {
                (
                    &mi.tokens.no,
                    "DOWN",
                    no_ask,
                    &mi.tokens.yes,
                    "UP",
                    yes_ask,
                    self.position.yes_size,
                    self.position.yes_cost,
                )
            } else {
                (
                    &mi.tokens.yes,
                    "UP",
                    yes_ask,
                    &mi.tokens.no,
                    "DOWN",
                    no_ask,
                    self.position.no_size,
                    self.position.no_cost,
                )
            };

        let min_sell_size = Self::env_f64("MIN_IMBALANCE_SELL_SHARES", 5.0).max(0.01);
        let min_buy_size = Self::env_f64("MIN_IMBALANCE_BUY_SHARES", 1.0).max(0.01);
        let mut size = yes_excess.abs().min(self.effective_max_trade_size());
        size = (size * 100.0).floor() / 100.0;
        if size < min_buy_size {
            self.hedge_cooldown_until = Some(Instant::now() + Duration::from_secs(IMBALANCE_DUST_RECHECK_SECS));
            self.log_action(&format!(
                "⏭ Imbalance {:.3} below min hedge-buy {:.2}; keeping naked {} dust and retrying in {}s.",
                size,
                min_buy_size,
                if yes_excess > 0.0 { "UP" } else { "DOWN" },
                IMBALANCE_DUST_RECHECK_SECS
            ))
            .await;
            return;
        }
        size = size.min(filled_size_total);
        if size <= 0.0 {
            return;
        }

        let avg_filled_cost = if filled_size_total > 0.0 {
            filled_cost_total / filled_size_total
        } else {
            0.0
        };
        let filled_cost_basis = avg_filled_cost * size;

        let hedge_target_net = size;
        let hedge_quote_size_for_estimate = self
            .gross_buy_size_for_target_net(hedge_target_net, missing_price, min_buy_size)
            .unwrap_or(size.max(min_buy_size));
        let mut expected_missing_cost = missing_price * hedge_quote_size_for_estimate;
        let mut expected_sell_proceeds = filled_price * size * 0.95;
        if let Some((yes_book, no_book)) = self.fetch_books(mi).await {
            if missing_token == &mi.tokens.yes {
                expected_missing_cost =
                    Self::estimate_buy_cost_from_book(&yes_book, hedge_quote_size_for_estimate)
                    .unwrap_or(expected_missing_cost);
            } else {
                expected_missing_cost =
                    Self::estimate_buy_cost_from_book(&no_book, hedge_quote_size_for_estimate)
                    .unwrap_or(expected_missing_cost);
            }
            if filled_token == &mi.tokens.yes {
                expected_sell_proceeds = Self::estimate_sell_proceeds_from_book(&yes_book, size)
                    .unwrap_or(expected_sell_proceeds);
            } else {
                expected_sell_proceeds = Self::estimate_sell_proceeds_from_book(&no_book, size)
                    .unwrap_or(expected_sell_proceeds);
            }
        }

        let actual_size_missing =
            self.fee_adjust_shares(hedge_quote_size_for_estimate, missing_price);
        let actual_size_filled = hedge_target_net;
        let mergeable = actual_size_missing.min(actual_size_filled);
        let hedge_pnl = mergeable * 1.0
            - filled_cost_basis
            - expected_missing_cost
            - self.gas_cache.fee_per_merge_usd;
        let sell_pnl = expected_sell_proceeds - filled_cost_basis;
        let sell_size = ((self.fee_adjust_shares(size, filled_price).min(size) * 100.0).floor() / 100.0).max(0.0);
        let mut can_sell_back = sell_size >= min_sell_size;
        let mut sell_back_blocked_by_relayer = false;
        if can_sell_back && !self.config.mock_currency {
            let rpc = crate::clob_client::POLYGON_RPCS[0];
            let holder = self.client.maker_address();
            match self.client.get_ctf_balance(filled_token, holder, rpc).await {
                Ok(on_chain) => {
                    if on_chain + 0.01 < sell_size {
                        can_sell_back = false;
                        sell_back_blocked_by_relayer = true;
                        self.log_action(&format!(
                            "⏭ Imbalance sell-back deferred: on-chain balance {:.2} < sellable {:.2} (relayer delay).",
                            on_chain, sell_size
                        ))
                        .await;
                    }
                }
                Err(e) => {
                    warn!(
                        "Imbalance sell-back balance precheck failed (continuing): {}",
                        Self::format_error_chain(&e)
                    );
                }
            }
        }

        if sell_back_blocked_by_relayer {
            let relayer_grace_secs = Self::env_u64("IMBALANCE_RELAYER_GRACE_SECS", 12).max(1);
            let relayer_retry_secs = Self::env_u64("IMBALANCE_RELAYER_RETRY_SECS", 6).max(1);
            let recent_activity = self
                .last_position_activity_at
                .map(|t| t.elapsed() <= Duration::from_secs(relayer_grace_secs))
                .unwrap_or(false);
            if recent_activity {
                self.hedge_cooldown_until =
                    Some(Instant::now() + Duration::from_secs(relayer_retry_secs));
                self.log_action(&format!(
                    "⏳ Imbalance hedge deferred for {}s (waiting relayer settlement; avoiding forced BUY hedge).",
                    relayer_retry_secs
                ))
                .await;
                return;
            }
        }

        // Hard loss guard: avoid forcing a hedge/sell-back when projected
        // outcomes are too negative. Near expiry, bypass to reduce hard carry.
        let max_expected_loss = Self::env_f64("IMBALANCE_MAX_EXPECTED_LOSS_USD", 1.25).max(0.0);
        let guard_disable_below_secs =
            Self::env_u64("IMBALANCE_LOSS_GUARD_DISABLE_BELOW_SECS", 45) as i64;
        let secs_to_expiry = (mi.end_date - Utc::now()).num_seconds();
        let best_projected_recovery_pnl = if can_sell_back {
            sell_pnl.max(hedge_pnl)
        } else {
            hedge_pnl
        };
        if secs_to_expiry > guard_disable_below_secs
            && best_projected_recovery_pnl < -max_expected_loss
        {
            let retry_secs = Self::env_u64("IMBALANCE_LOSS_GUARD_RETRY_SECS", 8).max(1);
            self.hedge_cooldown_until = Some(Instant::now() + Duration::from_secs(retry_secs));
            self.log_action(&format!(
                "🛡️ Imbalance action paused: best projected recovery PnL ${:+.2} below loss guard -${:.2}; retry in {}s.",
                best_projected_recovery_pnl, max_expected_loss, retry_secs
            ))
            .await;
            return;
        }

        let sell_back = can_sell_back && sell_pnl >= hedge_pnl;

        if !can_sell_back && !sell_back_blocked_by_relayer {
            self.log_action(&format!(
                "⏭ Imbalance sell-back skipped: sellable {:.3} < min {:.1} shares; using BUY hedge path.",
                sell_size, min_sell_size
            ))
            .await;
        }

        self.log_action(&format!(
            "📊 Smart hedge: {} better (sell PnL: ${:+.2} vs hedge PnL: ${:+.2})",
            if sell_back {
                format!("SELL {filled_label}")
            } else {
                format!("BUY {missing_label}")
            },
            sell_pnl,
            hedge_pnl
        ))
        .await;

        let tick = mi.tick_size.max(0.001);
        let fee_bps = self.cached_fee_rate_bps;
        let hedge_tif = Self::hedge_time_in_force();
        let hedge_tif_label = Self::tif_label(hedge_tif);
        if sell_back {
            let sell_back_price = Self::snap_price_to_tick(filled_price * 0.99, tick, false).max(tick);
            match self
                .run_sdk_call(
                    "sign imbalance sell-back order",
                    self.client.sign_order(
                        filled_token,
                        sell_back_price,
                        sell_size,
                        Side::Sell,
                        hedge_tif,
                        mi.neg_risk,
                        fee_bps,
                    ),
                )
                .await
            {
                Ok(order) => match self
                    .run_network_call(
                        "post imbalance sell-back order",
                        0,
                        Self::env_u64("SDK_POST_TIMEOUT_MS", SDK_POST_TIMEOUT_MS),
                        {
                            let client = Arc::clone(&self.client);
                            let mut maybe_order = Some(order);
                            let tif = hedge_tif_label.to_string();
                            move || {
                                let client = Arc::clone(&client);
                                let tif = tif.clone();
                                let order = maybe_order.take();
                                async move {
                                    let order = order
                                        .ok_or_else(|| anyhow!("imbalance sell-back order missing payload"))?;
                                    client.post_order(order, &tif).await
                                }
                            }
                        },
                    )
                    .await
                {
                    Ok(resp) => {
                        let matched = resp
                            .size_matched
                            .as_deref()
                            .unwrap_or("0")
                            .parse::<f64>()
                            .unwrap_or(0.0);
                        if matched > 0.0 {
                            self.mark_response_fill_applied(&resp.order_id);
                            let reduced = if yes_excess > 0.0 {
                                Self::apply_position_trade_delta(
                                    &mut self.position.yes_size,
                                    &mut self.position.yes_cost,
                                    "SELL",
                                    matched,
                                    sell_back_price,
                                )
                            } else {
                                Self::apply_position_trade_delta(
                                    &mut self.position.no_size,
                                    &mut self.position.no_cost,
                                    "SELL",
                                    matched,
                                    sell_back_price,
                                )
                            };
                            if reduced > 0.0 {
                                self.note_position_activity();
                                let avg_cost = if size > 0.0 {
                                    filled_cost_basis / size
                                } else {
                                    0.0
                                };
                                let realized_cost = avg_cost * reduced;
                                let proceeds = reduced * sell_back_price;
                                self.hedge_sellback_pnl += proceeds - realized_cost;
                                self.sync_daily_pnl_from_components();
                            }
                            self.consecutive_hedge_failures = 0;
                            self.hedge_cooldown_until = None;
                            self.record_hedge_outcome(true);
                            self.log_action(&format!(
                                "✅ Imbalance sell-back fill: -{reduced:.2} {filled_label} @ {:.2}",
                                sell_back_price
                            ))
                            .await;
                        } else {
                            let cooldown_secs = self.on_hedge_failure();
                            self.log_action(&format!(
                                "⚠️ Imbalance sell-back posted but unfilled — cooldown {}s",
                                cooldown_secs
                            ))
                            .await;
                        }
                    }
                    Err(e) => {
                        warn!("Imbalance sell-back failed: {}", Self::format_error_chain(&e));
                        if Self::is_balance_allowance_rejection(&e) {
                            self.hedge_cooldown_until = None;
                            self.record_hedge_outcome(false);
                            self.log_action(
                                "⚠️ Imbalance sell-back rejected (balance/allowance). Keeping smart-hedge mode.",
                            )
                            .await;
                        } else {
                            let cooldown_secs = self.on_hedge_failure();
                            let reason = Self::truncate_for_action(&Self::format_error_chain(&e), 160);
                            self.log_action(&format!(
                                "⚠️ Imbalance sell-back failed ({}) — cooldown {}s",
                                reason, cooldown_secs
                            ))
                            .await;
                        }
                    }
                },
                Err(e) => {
                    warn!("Imbalance sell-back sign failed: {}", Self::format_error_chain(&e));
                    self.record_hedge_outcome(false);
                    let reason = Self::truncate_for_action(&Self::format_error_chain(&e), 160);
                    self.log_action(&format!("⚠️ Imbalance sell-back sign failed ({reason})"))
                        .await;
                }
            }
        } else {
            let affordable_target_net = self.affordable_net_buy_shares(missing_price);
            let hedge_target_net = hedge_target_net.min(affordable_target_net);
            if hedge_target_net + 0.01 < min_buy_size {
                self.hedge_cooldown_until =
                    Some(Instant::now() + Duration::from_secs(IMBALANCE_DUST_RECHECK_SECS));
                self.log_action(&format!(
                    "⏭ Imbalance BUY hedge skipped: affordable net {:.2} < min {:.2} (balance ${:.2}); retry in {}s.",
                    hedge_target_net,
                    min_buy_size,
                    self.last_balance.unwrap_or(0.0),
                    IMBALANCE_DUST_RECHECK_SECS
                ))
                .await;
                return;
            }
            if hedge_target_net + 0.01 < size {
                self.log_action(&format!(
                    "⚖️ Imbalance BUY hedge size capped by balance: target {:.2} → {:.2} net shares.",
                    size,
                    hedge_target_net
                ))
                .await;
            }

            let (gross_hedge_matched, hedge_received, gross_hedge_cost, resolved) = match self
                .execute_buy_hedge_ladder(
                    missing_token,
                    mi,
                    fee_bps,
                    missing_price,
                    hedge_target_net,
                    min_buy_size,
                    "Imbalance",
                )
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!("Imbalance hedge failed: {}", Self::format_error_chain(&e));
                    let cooldown_secs = self.on_hedge_failure();
                    let reason = Self::truncate_for_action(&Self::format_error_chain(&e), 160);
                    self.log_action(&format!(
                        "⚠️ Imbalance hedge failed ({}) — cooldown {}s",
                        reason, cooldown_secs
                    ))
                    .await;
                    return;
                }
            };

            if hedge_received > 0.0 {
                let effective_price = if hedge_received > 0.0 {
                    gross_hedge_cost / hedge_received
                } else {
                    missing_price
                };
                let added = if yes_excess > 0.0 {
                    self.active_assets.insert(mi.tokens.no.clone());
                    Self::apply_position_trade_delta(
                        &mut self.position.no_size,
                        &mut self.position.no_cost,
                        "BUY",
                        hedge_received,
                        effective_price,
                    )
                } else {
                    self.active_assets.insert(mi.tokens.yes.clone());
                    Self::apply_position_trade_delta(
                        &mut self.position.yes_size,
                        &mut self.position.yes_cost,
                        "BUY",
                        hedge_received,
                        effective_price,
                    )
                };
                if added > 0.0 {
                    self.note_position_activity();
                }
                self.consecutive_hedge_failures = 0;
                self.hedge_cooldown_until = None;
                if resolved {
                    self.record_hedge_outcome(true);
                }
                self.log_action(&format!(
                    "✅ Imbalance hedge fill: +{added:.2} {} (gross {:.2})",
                    missing_label, gross_hedge_matched
                ))
                .await;
                if !resolved {
                    let cooldown_secs = self.on_hedge_failure();
                    self.log_action(&format!(
                        "⚠️ Imbalance ladder unresolved: net {:.2}/{:.2} — cooldown {}s",
                        hedge_received, hedge_target_net, cooldown_secs
                    ))
                    .await;
                }
            } else {
                let cooldown_secs = self.on_hedge_failure();
                self.log_action(&format!(
                    "⚠️ Imbalance hedge posted but unfilled — cooldown {}s",
                    cooldown_secs
                ))
                .await;
            }
        }
    }

    fn on_hedge_failure(&mut self) -> u64 {
        self.consecutive_hedge_failures += 1;
        self.record_hedge_outcome(false);
        let cooldown_secs = (30 * self.consecutive_hedge_failures as u64).min(MAX_HEDGE_COOLDOWN_SECS);
        self.hedge_cooldown_until = Some(Instant::now() + Duration::from_secs(cooldown_secs));
        warn!(
            "Hedge failed #{} — cooldown {cooldown_secs}s",
            self.consecutive_hedge_failures
        );
        cooldown_secs
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

        // Cap to currently available (excluding already reserved pending merges).
        let capped = amount.min(self.mergeable_available());
        if capped < 0.01 {
            return;
        }

        // ── Relayer delay guard ───────────────────────────────────────────────
        // Polymarket's fill relayer can take a few seconds to settle CTF tokens
        // on-chain.  If we call mergePositions before the balance appears, the
        // transaction reverts.  Check actual on-chain balances first; if zero,
        // defer and retry in MERGE_RETRY_SECS without touching in-memory state.
        if !self.config.mock_currency {
            let rpc = crate::clob_client::POLYGON_RPCS[0];
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

        let merge_id = format!("{}:{}:{:.2}", mi.condition_id, Utc::now().timestamp_millis(), capped);
        let condition_id = mi.condition_id.clone();
        let question = mi.question.clone();
        let neg_risk = mi.neg_risk;
        self.pending_merge_reserved_size += capped;
        self.session_locked_value += capped; // value reserved until confirmation/failure is processed.
        self.pending_merges.insert(
            merge_id.clone(),
            PendingMergeState {
                merge_id: merge_id.clone(),
                condition_id: condition_id.clone(),
                size: capped,
                tx_hash: None,
                status: PendingMergeStatus::Submitted,
                created_at: Utc::now(),
            },
        );

        if self.config.mock_currency {
            // Mock mode — just log, no on-chain call
            let msg = MergeResultMsg {
                merge_id,
                condition_id,
                question,
                size: capped,
                tx_hash: None,
                success: true,
                error: None,
            };
            let _ = self.merge_result_tx.send(msg);
            return;
        }

        info!("Merging {capped:.2} YES+NO → USDC (on-chain, fire-and-forget)");

        let client = Arc::clone(&self.client);
        let tx = self.merge_result_tx.clone();

        tokio::spawn(async move {
            let mut last_err = None;
            for rpc in &crate::clob_client::POLYGON_RPCS {
                match client
                    .merge_positions(&condition_id, capped, neg_risk, rpc)
                    .await
                {
                    Ok(tx_hash) => {
                        let result = MergeResultMsg {
                            merge_id,
                            condition_id,
                            question,
                            size: capped,
                            tx_hash: Some(format!("{tx_hash:?}")),
                            success: true,
                            error: None,
                        };
                        let _ = tx.send(result);
                        return;
                    }
                    Err(e) => {
                        warn!("Merge failed on {rpc}: {e}");
                        last_err = Some(e);
                    }
                }
            }

            let result = MergeResultMsg {
                merge_id,
                condition_id,
                question,
                size: capped,
                tx_hash: None,
                success: false,
                error: last_err.map(|e| e.to_string()),
            };
            let _ = tx.send(result);
        });
    }

    async fn process_merge_results(&mut self) {
        while let Ok(msg) = self.merge_result_rx.try_recv() {
            let released_size = self
                .pending_merges
                .remove(&msg.merge_id)
                .map(|p| p.size)
                .unwrap_or(msg.size);
            self.pending_merge_reserved_size =
                (self.pending_merge_reserved_size - released_size).max(0.0);
            self.session_locked_value = (self.session_locked_value - released_size).max(0.0);

            if msg.success {
                self.position.yes_size = (self.position.yes_size - released_size).max(0.0);
                self.position.no_size = (self.position.no_size - released_size).max(0.0);
                self.note_position_activity();
                self.log_action(&format!(
                    "✅ Merge confirmed: {:.2} shares → USDC | tx={}",
                    released_size,
                    msg.tx_hash.as_deref().unwrap_or("n/a")
                ))
                .await;
                self.trade_logger
                    .log_merge(&msg.condition_id, Some(&msg.question), released_size, true, None)
                    .await;
                for batch in self.execution_batches.values_mut() {
                    if batch.condition_id == msg.condition_id && batch.state == ExecutionState::MergePending {
                        batch.state = ExecutionState::Closed;
                    }
                }
            } else {
                self.merge_mismatch_count = self.merge_mismatch_count.saturating_add(1);
                let err_msg = msg.error.unwrap_or_else(|| "unknown merge failure".to_string());
                self.log_action(&format!(
                    "⚠️ Merge failed: {:.2} shares for {} — {}",
                    released_size, msg.condition_id, err_msg
                ))
                .await;
                self.trade_logger
                    .log_merge(
                        &msg.condition_id,
                        Some(&msg.question),
                        released_size,
                        false,
                        Some(err_msg.clone()),
                    )
                    .await;
                for batch in self.execution_batches.values_mut() {
                    if batch.condition_id == msg.condition_id && batch.state == ExecutionState::MergePending {
                        batch.state = ExecutionState::Failed;
                    }
                }
            }
        }
    }

    // ─── Redemption ────────────────────────────────────────────────────────────

    pub async fn run_claim_cycle(monitor: &Arc<Mutex<Self>>) {
        let cycle_start = Instant::now();
        let now = Instant::now();

        let (client, is_redeeming_flag, should_scan) = {
            let mut m = match monitor.try_lock() {
                Ok(g) => g,
                Err(_) => return,
            };

            if m.config.mock_currency {
                m.record_claim_latency(cycle_start.elapsed());
                return;
            }
            let is_redeeming_flag = Arc::clone(&m.is_redeeming);
            if is_redeeming_flag.load(Ordering::Relaxed) {
                m.record_claim_latency(cycle_start.elapsed());
                return;
            }

            let due = m
                .last_claim_poll
                .map(|t| t.elapsed().as_secs() >= CLAIM_POLL_INTERVAL_SECS)
                .unwrap_or(true);
            if !due && !m.force_claim_scan {
                m.record_claim_latency(cycle_start.elapsed());
                return;
            }
            m.last_claim_poll = Some(now);
            m.force_claim_scan = false;
            (Arc::clone(&m.client), is_redeeming_flag, true)
        };

        if !should_scan {
            return;
        }

        let redeemables = match client.get_redeemable_conditions().await {
            Ok(v) => v,
            Err(e) => {
                warn!("Failed to fetch redeemable positions: {e}");
                if let Ok(mut m) = monitor.try_lock() {
                    m.record_claim_latency(cycle_start.elapsed());
                }
                return;
            }
        };

        let total_claimable = redeemables.iter().map(|x| x.2).sum::<f64>();
        let candidates = {
            let mut m = match monitor.try_lock() {
                Ok(g) => g,
                Err(_) => return,
            };
            m.pending_claim_value = total_claimable;
            m.request_render();
            if redeemables.is_empty() {
                m.record_claim_latency(cycle_start.elapsed());
                return;
            }

            let now = Instant::now();
            let mut picked = Vec::new();
            for (cid, question, amount) in redeemables {
                if !m.should_attempt_claim(&cid, amount, now) {
                    continue;
                }
                picked.push((cid, question, amount));
            }

            if picked.is_empty() {
                m.record_claim_latency(cycle_start.elapsed());
                return;
            }

            is_redeeming_flag.store(true, Ordering::Relaxed);
            m.log_action(&format!(
                "💸 Claimable detected: ${:.2} total — processing {} condition(s)",
                total_claimable,
                picked.len()
            ))
            .await;
            picked
        };

        for (cid, question, amount) in candidates {
            if let Ok(mut m) = monitor.try_lock() {
                m.log_action(&format!(
                    "💸 Claim attempt: {} (${:.2})",
                    question, amount
                ))
                .await;
            }

            let mut success_tx: Option<String> = None;
            let mut last_error = String::from("unknown redeem error");

            for rpc in &crate::clob_client::POLYGON_RPCS {
                match client.redeem_positions(&cid, rpc).await {
                    Ok(tx) => {
                        success_tx = Some(format!("{tx:?}"));
                        break;
                    }
                    Err(e) => {
                        last_error = format!("{e}");
                        warn!("Redemption failed on {rpc}: {e}");
                    }
                }
            }

            if let Ok(mut m) = monitor.try_lock() {
                if let Some(tx_hash) = success_tx {
                    m.claim_retry_after.remove(&cid);
                    m.claim_success_suppression_until.insert(
                        cid.clone(),
                        Instant::now() + Duration::from_secs(CLAIM_SUCCESS_SUPPRESSION_SECS),
                    );
                    // Gross claim cash-in; not net PnL against unknown entry basis.
                    m.redemption_pnl += amount;
                    m.sync_daily_pnl_from_components();
                    m.log_action(&format!(
                        "✅ Redemption confirmed: {} (${:.2}) tx={}",
                        question, amount, tx_hash
                    ))
                    .await;
                    m.trade_logger
                        .log_redemption(&cid, Some(&question), amount, amount, true, None)
                        .await;
                } else {
                    m.claim_retry_after.insert(
                        cid.clone(),
                        Instant::now() + Duration::from_secs(CLAIM_FAILURE_BACKOFF_SECS),
                    );
                    m.log_action(&format!(
                        "⚠️ Redemption failed: {} (${:.2}) — retry in {}s",
                        question, amount, CLAIM_FAILURE_BACKOFF_SECS
                    ))
                    .await;
                    m.trade_logger
                        .log_redemption(
                            &cid,
                            Some(&question),
                            amount,
                            amount,
                            false,
                            Some(last_error),
                        )
                        .await;
                }
            }
        }

        is_redeeming_flag.store(false, Ordering::Relaxed);
        if let Ok(mut m) = monitor.try_lock() {
            m.record_claim_latency(cycle_start.elapsed());
        }
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

        // Cancel resting strategy orders
        if let Some(ref mut maker) = self.maker {
            let _ = maker.cancel_all(&self.client).await;
        }
        if let Some(ref mut strategy) = self.post_only {
            let _ = strategy.cancel_all(&self.client).await;
        }

        // Clear active market + WS while we search for the next market.
        self.market_info = None;
        self.ws_client = None;
        self.ws_notify = None;
        self.last_tick_signature = None;
        self.ws_yes_age_ms = None;
        self.ws_no_age_ms = None;
        self.last_market_discovery_attempt = Some(Instant::now());

        // Position state is market-specific (token ids change every 5m market).
        // Reset stale in-memory inventory now so expired-market carry cannot be
        // hedged using the next market's prices during discovery/rollover gaps.
        let stale_yes = self.position.yes_size;
        let stale_no = self.position.no_size;
        if stale_yes > 0.01 || stale_no > 0.01 {
            self.log_action(&format!(
                "🧹 Rollover reset: clearing expired-market in-mem position UP {:.2} / DOWN {:.2}",
                stale_yes, stale_no
            ))
            .await;
        }
        self.position = Position::default();
        self.pending_merge_reserved_size = 0.0;
        self.hedge_cooldown_until = None;
        self.consecutive_hedge_failures = 0;
        self.reconcile_drift_streak = 0;
        self.timeout_recovery_active = false;
        self.unresolved_ambiguous_batches.clear();
        if matches!(
            self.entry_lock_reason,
            Some(
                EntryLockReason::Imbalance
                    | EntryLockReason::TimeoutRecovery
                    | EntryLockReason::AmbiguousFill
                    | EntryLockReason::ReconcileDrift
            )
        ) {
            self.entry_lock_reason = None;
            self.entry_lock_until = None;
        }

        self.order_tracker.clear();
        self.order_to_batch.clear();
        self.execution_batches.clear();

        // Find next market (non-fatal if unavailable during a short gap)
        match self.find_active_market().await {
            Ok(true) => {
                self.clear_discovery_degraded_state();
            }
            Ok(false) => {
                self.clear_discovery_degraded_state();
                let retry_secs = self.discovery_retry_secs();
                self.log_action(&format!(
                    "⏳ No active market yet. Waiting for next window (retry {}s)...",
                    retry_secs
                ))
                .await;
                return;
            }
            Err(e) => {
                warn!("Failed to find next market: {e}");
                if self.discovery_failure_since.is_none() {
                    self.discovery_failure_since = Some(Instant::now());
                }
                let retry_secs = self.discovery_retry_secs();
                let reason = Self::truncate_for_action(&Self::format_error_chain(&e), 220);
                self.log_action(&format!(
                    "⚠️ Failed to discover next market (retry {}s): {}",
                    retry_secs, reason
                ))
                .await;
                self.maybe_log_discovery_degraded().await;
                return;
            }
        }

        if self.market_info.is_none() {
            return;
        }

        // Seed position from current market trades only (safe after reset).
        self.recover_positions_from_trades().await;

        // Resubscribe WS (Hard Reconnect)
        self.connect_ws_for_active_market(true).await;

        let new_q = self.market_info.as_ref().map(|m| m.question.clone()).unwrap_or_else(|| "unknown".to_string());
        self.log_action(&format!("✅ Rolled over to: {new_q}")).await;
        info!("Rolled over to: {new_q}");
        self.request_render();
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

    // ─── Resting Strategy Fill Check ──────────────────────────────────────────

    pub async fn check_maker_fills(&mut self) {
        if !self.config.uses_resting_orders() {
            return;
        }
        let mi = match self.market_info.clone() {
            Some(m) => m,
            None => return,
        };

        let fills = match self.config.strategy_mode {
            StrategyMode::Maker => {
                if let Some(ref mut maker) = self.maker {
                    maker.check_fills(&self.client, &mi.condition_id).await.ok()
                } else {
                    None
                }
            }
            StrategyMode::PostOnly => {
                if let Some(ref mut strategy) = self.post_only {
                    strategy.check_fills(&self.client, &mi.condition_id).await.ok()
                } else {
                    None
                }
            }
            StrategyMode::Taker => None,
        };

        if let Some((yes_filled, no_filled)) = fills {
            if yes_filled > 0.01 || no_filled > 0.01 {
                self.position.yes_size += yes_filled;
                self.position.no_size += no_filled;
                self.note_position_activity();
                self.active_assets.insert(mi.tokens.yes.clone());
                self.active_assets.insert(mi.tokens.no.clone());

                let strategy_label = if self.config.is_post_only_mode() {
                    "Post-only"
                } else {
                    "Maker"
                };
                info!(
                    "{strategy_label} fills: YES={yes_filled:.2} NO={no_filled:.2}"
                );
                self.log_action(&format!(
                    "✅ {strategy_label} fill: UP {yes_filled:.2} | DOWN {no_filled:.2}"
                ))
                .await;

                // Safety guard: once a resting quote starts filling, clear remaining
                // resting orders so inventory cannot cascade while hedge/reconcile runs.
                let mut cancelled_resting_quotes = false;
                match self.config.strategy_mode {
                    StrategyMode::Maker => {
                        if let Some(ref mut maker) = self.maker {
                            if maker.has_open_orders() {
                                let _ = maker.cancel_all(&self.client).await;
                                cancelled_resting_quotes = true;
                            }
                        }
                    }
                    StrategyMode::PostOnly => {
                        if let Some(ref mut strategy) = self.post_only {
                            if strategy.has_open_orders() {
                                let _ = strategy.cancel_all(&self.client).await;
                                cancelled_resting_quotes = true;
                            }
                        }
                    }
                    StrategyMode::Taker => {}
                }
                if cancelled_resting_quotes {
                    self.log_action("🛑 Resting quotes cancelled after fill; waiting for neutral inventory.")
                        .await;
                }

                let fire_amount = self.mergeable_available();
                if fire_amount >= 1.0 {
                    self.fire_merge(fire_amount, &mi).await;
                }
            }
        }
    }

    // ─── User WS Events Processing ────────────────────────────────────────────

    async fn process_user_events(&mut self) {
        let mut ws_client_opt = self.ws_client.take();
        if let Some(ref mut ws) = ws_client_opt {
            let mut processed_user = 0usize;
            while processed_user < MAX_WS_USER_EVENTS_PER_CYCLE {
                let Ok(msg) = ws.user_events_rx.try_recv() else {
                    break;
                };
                processed_user += 1;
                // Peek the event_type
                if let Some(event_type) = msg.get("event_type").and_then(|v| v.as_str()) {
                    match event_type {
                        "order" => {
                            if let Ok(ev) = serde_json::from_value::<crate::types::WsOrderEvent>(msg) {
                                debug!("WS Order Event: {} | {} | {} | matched: {}", ev.id, ev.side, ev.status, ev.size_matched);
                                let matched = ev.size_matched.parse::<f64>().unwrap_or(0.0);
                                self.update_order_fill_state(&ev.id, matched, Utc::now(), true);
                            }
                        }
                        "trade" => {
                            if let Ok(ev) = serde_json::from_value::<crate::types::WsTradeEvent>(msg) {
                                if !self.mark_ws_trade_seen(&ev.id) {
                                    debug!("WS Trade Event duplicate ignored: {}", ev.id);
                                    continue;
                                }
                                info!("WS Trade Event: Executed {} {} shares of {} at {}", ev.side, ev.size, ev.asset_id, ev.price);
                                self.active_assets.insert(ev.asset_id.clone());
                                let trade_size = ev.size.parse::<f64>().unwrap_or(0.0);
                                let response_already_applied =
                                    self.response_fill_already_applied(&ev.taker_order_id);
                                let tracked_batch_state = self
                                    .order_to_batch
                                    .get(&ev.taker_order_id)
                                    .and_then(|batch_id| self.execution_batches.get(batch_id))
                                    .map(|batch| batch.state.clone());
                                let mut tracked_fill = false;
                                if trade_size > 0.0 {
                                    if let Some(leg) = self.order_tracker.get(&ev.taker_order_id).cloned() {
                                        // Trade events are per-match deltas; accumulate and cap at target.
                                        let matched_hint =
                                            (leg.matched_size + trade_size).min(leg.target_size.max(leg.matched_size));
                                        self.update_order_fill_state(
                                            &ev.taker_order_id,
                                            matched_hint,
                                            Utc::now(),
                                            true,
                                        );
                                        tracked_fill = true;
                                    }
                                }
                                if !tracked_fill {
                                    if !response_already_applied {
                                        if self.config.uses_resting_orders() {
                                            // For maker/post-only, WS trade events can be ambiguous
                                            // (maker/taker ids may not map cleanly to our local trackers).
                                            // Avoid directional self-hedge cascades from misattribution and
                                            // let order polling + reconcile own the inventory truth.
                                            debug!(
                                                "Ignoring untracked WS trade in resting mode: {} {} {} @ {}",
                                                ev.side, ev.size, ev.asset_id, ev.price
                                            );
                                        } else {
                                            self.apply_untracked_ws_trade_fill(&ev);
                                        }
                                    }
                                } else if matches!(
                                    tracked_batch_state,
                                    Some(ExecutionState::Failed | ExecutionState::Closed | ExecutionState::SoldBack)
                                ) && !response_already_applied {
                                    let _ = self.apply_ws_trade_fill_to_position(&ev, "Late tracked WS fill");
                                }
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

            if processed_user == MAX_WS_USER_EVENTS_PER_CYCLE {
                debug!(
                    "WS user event drain capped at {} events this cycle (remaining backlog deferred)",
                    MAX_WS_USER_EVENTS_PER_CYCLE
                );
            }

            let mut processed_market = 0usize;
            while processed_market < MAX_WS_MARKET_EVENTS_PER_CYCLE {
                let Ok(msg) = ws.market_events_rx.try_recv() else {
                    break;
                };
                processed_market += 1;
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
                                self.force_claim_scan = true;
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
            if processed_market == MAX_WS_MARKET_EVENTS_PER_CYCLE {
                debug!(
                    "WS market event drain capped at {} events this cycle (remaining backlog deferred)",
                    MAX_WS_MARKET_EVENTS_PER_CYCLE
                );
            }
        }
        self.ws_client = ws_client_opt;
    }

    // ─── WS Health Check ──────────────────────────────────────────────────────

    pub async fn check_ws_health(&mut self) {
        self.maybe_recover_after_runtime_pause().await;
        if !self.config.ws_enabled {
            return;
        }

        if self.market_info.is_none() {
            self.try_find_active_market_if_due().await;
            return;
        }
        let mi = self.market_info.clone().expect("market checked above");

        if let Some(ref ws) = self.ws_client {
            let yes_age = ws.get_book_age_ms(&mi.tokens.yes).await;
            let no_age = ws.get_book_age_ms(&mi.tokens.no).await;
            self.ws_yes_age_ms = yes_age;
            self.ws_no_age_ms = no_age;
        }

        // Align with TS behavior: reconnect on explicit reconnect request or missing WS client.
        // Staleness is handled by REST fallback in the hot path and should not force reconnect loops.
        if self.ws_client.is_none() || self.ws_reconnect_requested {
            warn!("WS connection missing/reconnect requested — reconnecting");
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
                    self.ws_reconnect_requested = false;
                    self.ws_stale_warned = false;
                    self.ws_stale_consecutive = 0;
                    self.ws_connected_at = Some(Instant::now());
                    self.last_ws_age_sample = None;
                    info!("WS reconnected");
                    self.log_action("📡 WebSocket orderbook feed reconnected").await;
                }
                Err(e) => {
                    warn!("WS reconnect failed: {e}");
                }
            }
        }
    }

    pub async fn run_balance_refresh_cycle(monitor: &Arc<Mutex<Self>>) {
        let cycle_start = Instant::now();
        let (is_mock, client) = {
            let m = match monitor.try_lock() {
                Ok(g) => g,
                Err(_) => return,
            };
            (m.config.mock_currency, Arc::clone(&m.client))
        };

        let fetch = if is_mock {
            Ok(1000.0)
        } else {
            client.get_balance().await
        };

        let mut m = match monitor.try_lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        match fetch {
            Ok(balance) => {
                m.last_balance = Some(balance);
                m.last_balance_refresh = Instant::now();
                m.sync_daily_pnl_from_balance();
                m.request_render();
            }
            Err(e) => {
                warn!("Balance refresh cycle failed: {e}");
            }
        }
        m.record_balance_latency(cycle_start.elapsed());
    }

    pub async fn run_gas_refresh_cycle(monitor: &Arc<Mutex<Self>>) {
        let (client, enabled) = {
            let m = match monitor.try_lock() {
                Ok(g) => g,
                Err(_) => return,
            };
            (Arc::clone(&m.client), !m.config.mock_currency)
        };
        if !enabled {
            return;
        }

        let (gas_gwei, pol_price) = tokio::join!(client.get_gas_price_gwei(), client.get_pol_price_usd());
        let merge_gas = 300_000_u64;
        let fee_per_merge_usd = (gas_gwei * 1e-9 * merge_gas as f64) * pol_price;

        let mut m = match monitor.try_lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        m.gas_cache = GasCache {
            gas_price_gwei: gas_gwei,
            pol_price_usd: pol_price,
            fee_per_merge_usd,
            updated_at: Instant::now(),
        };
        debug!(
            "Gas cache cycle: {gas_gwei:.1} Gwei, POL=${pol_price:.4}, merge≈${fee_per_merge_usd:.5}"
        );
    }

    pub async fn run_merge_reconcile_cycle(monitor: &Arc<Mutex<Self>>) {
        let (client, market_opt, mock_mode, holder) = {
            let mut m = match monitor.try_lock() {
                Ok(g) => g,
                Err(_) => return,
            };
            m.process_merge_results().await;
            (
                Arc::clone(&m.client),
                m.market_info.clone(),
                m.config.mock_currency,
                m.client.maker_address(),
            )
        };
        if mock_mode {
            return;
        }
        let Some(mi) = market_opt else {
            return;
        };
        // Read across configured RPCs and take the max observed per leg to reduce stale/laggy-node zeros.
        let mut yes_on_chain = 0.0_f64;
        let mut no_on_chain = 0.0_f64;
        for rpc in &crate::clob_client::POLYGON_RPCS {
            let (yes_res, no_res) = tokio::join!(
                client.get_ctf_balance(&mi.tokens.yes, holder, rpc),
                client.get_ctf_balance(&mi.tokens.no, holder, rpc),
            );
            if let Ok(v) = yes_res {
                yes_on_chain = yes_on_chain.max(v);
            }
            if let Ok(v) = no_res {
                no_on_chain = no_on_chain.max(v);
            }
        }

        let mut m = match monitor.try_lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        let yes_drift = (yes_on_chain - m.position.yes_size).abs();
        let no_drift = (no_on_chain - m.position.no_size).abs();
        let max_reservable = m.position.mergeable_amount();
        if m.pending_merge_reserved_size > max_reservable + 0.01 {
            m.merge_mismatch_count = m.merge_mismatch_count.saturating_add(1);
            m.pending_merge_reserved_size = max_reservable.max(0.0);
        }
        let on_chain_near_zero = yes_on_chain <= 0.01 && no_on_chain <= 0.01;
        let in_mem_has_position = m.position.yes_size > 0.01 || m.position.no_size > 0.01;
        let in_mem_is_dust = m.position.yes_size <= RECONCILE_DUST_MAX_SHARES
            && m.position.no_size <= RECONCILE_DUST_MAX_SHARES;
        let dust_drift = yes_drift.max(no_drift);
        let recent_position_activity = m
            .last_position_activity_at
            .map(|t| t.elapsed() < Duration::from_secs(RECONCILE_ZERO_SYNC_GRACE_SECS))
            .unwrap_or(false);
        let defer_zero_sync = on_chain_near_zero && in_mem_has_position && recent_position_activity;
        let dust_zero_sync_candidate = on_chain_near_zero
            && in_mem_has_position
            && in_mem_is_dust
            && dust_drift >= RECONCILE_DUST_DRIFT_MIN_SHARES;

        if dust_zero_sync_candidate {
            m.reconcile_drift_streak = m.reconcile_drift_streak.saturating_add(1);
            m.merge_mismatch_count = m.merge_mismatch_count.saturating_add(1);
            if m.reconcile_drift_streak == 1 || m.reconcile_drift_streak % 6 == 0 {
                let in_mem_yes = m.position.yes_size;
                let in_mem_no = m.position.no_size;
                m.log_action_fast(&format!(
                    "⚠️ Reconcile dust drift: on-chain zero but in-mem YES/NO {:.3}/{:.3}",
                    in_mem_yes, in_mem_no
                ));
            }
            if defer_zero_sync {
                if m.reconcile_drift_streak == 1 || m.reconcile_drift_streak % 6 == 0 {
                    m.log_action_fast(&format!(
                        "⏳ Reconcile deferring dust zero-sync for {}s after recent fills (possible settlement lag)",
                        RECONCILE_ZERO_SYNC_GRACE_SECS
                    ));
                }
            } else if m.reconcile_drift_streak >= RECONCILE_DUST_SYNC_STREAK {
                m.position.yes_size = 0.0;
                m.position.no_size = 0.0;
                m.position.yes_cost = 0.0;
                m.position.no_cost = 0.0;
                m.pending_merge_reserved_size = 0.0;
                m.log_action_fast(
                    "🧭 Cleared stale dust position from reconcile (on-chain balances are zero)"
                );
                m.note_position_activity();
                m.reconcile_drift_streak = 0;
            }
        } else if yes_drift > RECONCILE_LARGE_DRIFT_SHARES || no_drift > RECONCILE_LARGE_DRIFT_SHARES {
            m.reconcile_drift_streak = m.reconcile_drift_streak.saturating_add(1);
            m.merge_mismatch_count = m.merge_mismatch_count.saturating_add(1);
            let in_mem_yes = m.position.yes_size;
            let in_mem_no = m.position.no_size;
            m.log_action_fast(&format!(
                "⚠️ Reconcile drift detected: on-chain YES/NO {:.2}/{:.2} vs in-mem {:.2}/{:.2}",
                yes_on_chain, no_on_chain, in_mem_yes, in_mem_no
            ));
            let required_streak = if on_chain_near_zero && in_mem_has_position {
                RECONCILE_ZERO_SYNC_STREAK
            } else {
                RECONCILE_NONZERO_SYNC_STREAK
            };
            if defer_zero_sync && (m.reconcile_drift_streak == 1 || m.reconcile_drift_streak % 6 == 0) {
                m.log_action_fast(&format!(
                    "⏳ Reconcile deferring zero-sync for {}s after recent fills (possible settlement lag)",
                    RECONCILE_ZERO_SYNC_GRACE_SECS
                ));
            }

            // If drift persists, trust on-chain balances and force-sync in-memory state.
            if m.reconcile_drift_streak >= required_streak && !defer_zero_sync {
                let prev_yes_size = m.position.yes_size;
                let prev_no_size = m.position.no_size;
                let prev_yes_cost = m.position.yes_cost;
                let prev_no_cost = m.position.no_cost;

                m.position.yes_size = yes_on_chain.max(0.0);
                m.position.no_size = no_on_chain.max(0.0);
                m.position.yes_cost = if m.position.yes_size <= 0.01 {
                    0.0
                } else if prev_yes_size > 0.0 {
                    prev_yes_cost * (m.position.yes_size / prev_yes_size)
                } else {
                    prev_yes_cost
                };
                m.position.no_cost = if m.position.no_size <= 0.01 {
                    0.0
                } else if prev_no_size > 0.0 {
                    prev_no_cost * (m.position.no_size / prev_no_size)
                } else {
                    prev_no_cost
                };
                m.pending_merge_reserved_size = m
                    .pending_merge_reserved_size
                    .min(m.position.mergeable_amount())
                    .max(0.0);
                let synced_yes = m.position.yes_size;
                let synced_no = m.position.no_size;
                m.log_action_fast(&format!(
                    "🧭 Position reconciled to on-chain YES/NO {:.2}/{:.2} after persistent drift",
                    synced_yes, synced_no
                ));
                m.note_position_activity();
                m.reconcile_drift_streak = 0;
            }
        } else {
            m.reconcile_drift_streak = 0;
        }
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    fn note_position_activity(&mut self) {
        self.last_position_activity_at = Some(Instant::now());
    }

    fn mergeable_available(&self) -> f64 {
        (self.position.mergeable_amount() - self.pending_merge_reserved_size).max(0.0)
    }

    fn entry_lock_reason_label(reason: EntryLockReason) -> &'static str {
        match reason {
            EntryLockReason::Imbalance => "Imbalance",
            EntryLockReason::TimeoutRecovery => "TimeoutRecovery",
            EntryLockReason::AmbiguousFill => "AmbiguousFill",
            EntryLockReason::ReconcileDrift => "ReconcileDrift",
            EntryLockReason::HedgeFailureRisk => "HedgeFailureRisk",
        }
    }

    fn net_imbalance_shares(&self) -> f64 {
        (self.position.yes_size - self.position.no_size).abs()
    }

    fn in_neutrality_resume_band(&self) -> bool {
        self.net_imbalance_shares() <= self.config.neutrality_resume_net_shares.max(0.0)
    }

    fn reconcile_drift_lock_active(&self) -> bool {
        self.reconcile_drift_streak >= ENTRY_LOCK_RECONCILE_DRIFT_STREAK
    }

    fn hedge_failure_lock_active(&self) -> bool {
        let Some(last_outcome) = self.last_hedge_outcome_at else {
            return false;
        };
        if last_outcome.elapsed().as_secs() > HEDGE_FAILURE_LOCK_TTL_SECS {
            return false;
        }
        if self.recent_hedge_outcomes.len() < HEDGE_FAILURE_LOCK_MIN_SAMPLES {
            return false;
        }
        self.adaptive_hedge_failure_ratio().unwrap_or(0.0) >= HEDGE_FAILURE_LOCK_RATIO
    }

    fn effective_max_trade_size(&self) -> f64 {
        let configured_cap = self.config.max_trade_size.max(0.0);
        let floor = ADAPTIVE_MIN_SIZE_FLOOR.min(configured_cap);
        self.adaptive_max_trade_size.max(floor).min(configured_cap)
    }

    fn effective_min_profit_usd(&self) -> f64 {
        self.adaptive_min_profit_usd.max(0.0)
    }

    fn affordable_net_buy_shares(&self, price: f64) -> f64 {
        if price <= 0.0 {
            return 0.0;
        }
        let balance = self.last_balance.unwrap_or(0.0).max(0.0);
        if balance <= 0.0 {
            return 0.0;
        }
        let spendable = (balance * 0.98).max(0.0);
        let gross_affordable = ((spendable / price) * 100.0).floor() / 100.0;
        self.fee_adjust_shares(gross_affordable, price).max(0.0)
    }

    fn sync_daily_pnl_from_components(&mut self) {
        // Headline PnL should track wallet reality, not model components.
        self.sync_daily_pnl_from_balance();
    }

    fn sync_daily_pnl_from_balance(&mut self) {
        let balance = self.last_balance.unwrap_or(0.0);
        self.daily_pnl = balance - self.session_start_balance;
    }

    fn record_execution_result(&mut self, pnl: f64) {
        self.execution_success_count = self.execution_success_count.saturating_add(1);
        if pnl > 0.0 {
            self.economic_success_count = self.economic_success_count.saturating_add(1);
        }
        self.execution_pnl += pnl;
        self.sync_daily_pnl_from_components();
    }

    fn update_adaptive_stability_controls(&mut self, completed: bool, pnl: f64) {
        self.recent_opportunity_outcomes
            .push_back(OpportunityOutcome { completed, pnl });
        while self.recent_opportunity_outcomes.len() > ADAPTIVE_WINDOW_SIZE {
            self.recent_opportunity_outcomes.pop_front();
        }
        self.recompute_adaptive_controls();
    }

    fn record_hedge_outcome(&mut self, success: bool) {
        self.recent_hedge_outcomes.push_back(success);
        self.last_hedge_outcome_at = Some(Instant::now());
        while self.recent_hedge_outcomes.len() > ADAPTIVE_HEDGE_WINDOW_SIZE {
            self.recent_hedge_outcomes.pop_front();
        }
        self.recompute_adaptive_controls();
    }

    fn adaptive_hedge_failure_ratio(&self) -> Option<f64> {
        if self.recent_hedge_outcomes.is_empty() {
            return None;
        }
        let failures = self.recent_hedge_outcomes.iter().filter(|ok| !**ok).count() as f64;
        Some(failures / self.recent_hedge_outcomes.len() as f64)
    }

    fn recompute_adaptive_controls(&mut self) {
        let sample_count = self.recent_opportunity_outcomes.len();
        let hedge_sample_count = self.recent_hedge_outcomes.len();
        if sample_count < ADAPTIVE_MIN_WINDOW_SAMPLES
            && hedge_sample_count < ADAPTIVE_MIN_WINDOW_SAMPLES
        {
            return;
        }

        let completed_count = self
            .recent_opportunity_outcomes
            .iter()
            .filter(|o| o.completed)
            .count();
        let completion_ratio = if sample_count > 0 {
            completed_count as f64 / sample_count as f64
        } else {
            ADAPTIVE_TARGET_COMPLETION_RATIO
        };

        let (economic_success_count, completed_pnl_sum) = self
            .recent_opportunity_outcomes
            .iter()
            .filter(|o| o.completed)
            .fold((0usize, 0.0_f64), |(wins, pnl_sum), o| {
                let is_profit = if o.pnl > 0.0 { 1 } else { 0 };
                (wins + is_profit, pnl_sum + o.pnl)
            });
        let economic_ratio = if completed_count > 0 {
            economic_success_count as f64 / completed_count as f64
        } else {
            ADAPTIVE_TARGET_ECONOMIC_RATIO
        };
        let avg_completed_pnl = if completed_count > 0 {
            completed_pnl_sum / completed_count as f64
        } else {
            0.0
        };
        let hedge_failure_ratio = self.adaptive_hedge_failure_ratio().unwrap_or(0.0);

        let completion_penalty = ((ADAPTIVE_TARGET_COMPLETION_RATIO - completion_ratio)
            / ADAPTIVE_TARGET_COMPLETION_RATIO)
            .clamp(0.0, 1.0);
        let economic_penalty =
            ((ADAPTIVE_TARGET_ECONOMIC_RATIO - economic_ratio) / ADAPTIVE_TARGET_ECONOMIC_RATIO)
                .clamp(0.0, 1.0);
        let pnl_penalty = if avg_completed_pnl < 0.0 {
            (-avg_completed_pnl / ADAPTIVE_NEG_PNL_SCALE_USD).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let hedge_penalty = hedge_failure_ratio.clamp(0.0, 1.0);

        let mut target_pressure =
            (completion_penalty * 0.40 + economic_penalty * 0.25 + pnl_penalty * 0.20
                + hedge_penalty * 0.15)
                .clamp(0.0, 1.0);
        if sample_count == 0 && hedge_sample_count >= ADAPTIVE_MIN_WINDOW_SAMPLES {
            target_pressure = target_pressure.max((hedge_failure_ratio * 0.90).clamp(0.0, 1.0));
        }
        if hedge_sample_count >= HEDGE_FAILURE_LOCK_MIN_SAMPLES
            && hedge_failure_ratio >= HEDGE_FAILURE_LOCK_RATIO
        {
            target_pressure = target_pressure.max(0.80);
        }

        self.adaptive_pressure = (self.adaptive_pressure * (1.0 - ADAPTIVE_PRESSURE_SMOOTHING)
            + target_pressure * ADAPTIVE_PRESSURE_SMOOTHING)
            .clamp(0.0, 1.0);

        let prev_min_profit = self.adaptive_min_profit_usd;
        let prev_max_size = self.adaptive_max_trade_size;

        let base_min_profit = self.config.min_net_profit_usd.max(0.0);
        let min_profit_cap = ADAPTIVE_MIN_PROFIT_CAP.max(base_min_profit);
        let min_profit_span = (min_profit_cap - base_min_profit).max(0.0);
        let next_min_profit = base_min_profit + min_profit_span * self.adaptive_pressure;

        let size_floor = ADAPTIVE_MIN_SIZE_FLOOR.min(self.config.max_trade_size).max(0.0);
        let size_span = (self.config.max_trade_size - size_floor).max(0.0);
        let next_max_size = (self.config.max_trade_size - size_span * self.adaptive_pressure).max(size_floor);

        self.adaptive_min_profit_usd = next_min_profit;
        self.adaptive_max_trade_size = next_max_size;

        let min_profit_changed =
            (self.adaptive_min_profit_usd - prev_min_profit).abs() >= ADAPTIVE_LOG_MIN_PROFIT_DELTA;
        let max_size_changed =
            (self.adaptive_max_trade_size - prev_max_size).abs() >= ADAPTIVE_LOG_MIN_SIZE_DELTA;
        if min_profit_changed || max_size_changed {
            self.log_action_fast(&format!(
                "🧠 Adaptive learn: pressure {:.0}% | exec {:.0}% | econ {:.0}% | hedge fail {:.0}% | avg pnl ${:+.3} → min profit ${:.3}, max size {:.1}",
                self.adaptive_pressure * 100.0,
                completion_ratio * 100.0,
                economic_ratio * 100.0,
                hedge_failure_ratio * 100.0,
                avg_completed_pnl,
                self.adaptive_min_profit_usd,
                self.adaptive_max_trade_size
            ));
        }
    }

    fn update_entry_lock_state(&mut self) {
        if !self.config.strict_neutral_mode {
            self.entry_lock_reason = None;
            self.entry_lock_until = None;
            self.timeout_recovery_active = false;
            return;
        }

        let now = Instant::now();
        if self.timeout_recovery_active {
            if let Some(until) = self.entry_lock_until {
                if now < until {
                    self.entry_lock_reason = Some(EntryLockReason::TimeoutRecovery);
                    return;
                }
            }
            self.timeout_recovery_active = false;
            self.entry_lock_until = None;
        }

        if !self.unresolved_ambiguous_batches.is_empty()
            && self.in_neutrality_resume_band()
            && !self.reconcile_drift_lock_active()
        {
            self.unresolved_ambiguous_batches.clear();
        }

        if !self.unresolved_ambiguous_batches.is_empty() {
            self.entry_lock_reason = Some(EntryLockReason::AmbiguousFill);
            return;
        }

        if !self.in_neutrality_resume_band() {
            self.entry_lock_reason = Some(EntryLockReason::Imbalance);
            return;
        }

        if self.reconcile_drift_lock_active() {
            self.entry_lock_reason = Some(EntryLockReason::ReconcileDrift);
            return;
        }

        if self.hedge_failure_lock_active() {
            self.entry_lock_reason = Some(EntryLockReason::HedgeFailureRisk);
            return;
        }

        self.entry_lock_reason = None;
        self.entry_lock_until = None;
    }

    fn entry_lock_remaining_secs(&self) -> Option<u64> {
        if self.entry_lock_reason != Some(EntryLockReason::TimeoutRecovery) {
            return None;
        }
        self.entry_lock_until.map(|until| {
            until
                .saturating_duration_since(Instant::now())
                .as_secs()
        })
    }

    async fn log_entry_lock_if_needed(&mut self) {
        self.update_entry_lock_state();
        let Some(reason) = self.entry_lock_reason else {
            self.last_entry_lock_log_reason = None;
            self.last_entry_lock_log_at = None;
            return;
        };

        let now = Instant::now();
        let reason_changed = self.last_entry_lock_log_reason != Some(reason);
        let stale_log = self
            .last_entry_lock_log_at
            .map(|t| t.elapsed().as_secs() >= ENTRY_LOCK_LOG_INTERVAL_SECS)
            .unwrap_or(true);
        if !reason_changed && !stale_log {
            return;
        }

        let reason_label = Self::entry_lock_reason_label(reason);
        let msg = if let Some(remaining) = self.entry_lock_remaining_secs() {
            format!(
                "⛔ Entry locked: {} (resume in ~{}s) | imbalance {:.3}, ambiguous {}, drift streak {}",
                reason_label,
                remaining,
                self.net_imbalance_shares(),
                self.unresolved_ambiguous_batches.len(),
                self.reconcile_drift_streak
            )
        } else {
            format!(
                "⛔ Entry locked: {} | imbalance {:.3}, ambiguous {}, drift streak {}",
                reason_label,
                self.net_imbalance_shares(),
                self.unresolved_ambiguous_batches.len(),
                self.reconcile_drift_streak
            )
        };
        self.log_action(&msg).await;
        self.last_entry_lock_log_reason = Some(reason);
        self.last_entry_lock_log_at = Some(now);
    }

    fn start_timeout_recovery_lock(&mut self, batch_id: &str) {
        self.timeout_recovery_active = true;
        self.entry_lock_until =
            Some(Instant::now() + Duration::from_secs(self.config.timeout_recovery_lock_secs.max(1)));
        self.entry_lock_reason = Some(EntryLockReason::TimeoutRecovery);
        self.unresolved_ambiguous_batches.insert(batch_id.to_string());
    }

    fn estimate_buy_cost_from_book(book: &OrderBook, size: f64) -> Option<f64> {
        if size <= 0.0 {
            return Some(0.0);
        }
        let mut remaining = size;
        let mut total_cost = 0.0;
        for level in &book.asks {
            let px = level.price_f64();
            let lvl_size = level.size_f64();
            if px <= 0.0 || lvl_size <= 0.0 {
                continue;
            }
            let take = remaining.min(lvl_size);
            total_cost += take * px;
            remaining -= take;
            if remaining <= 1e-9 {
                break;
            }
        }
        if remaining > 1e-6 {
            None
        } else {
            Some(total_cost)
        }
    }

    fn estimate_sell_proceeds_from_book(book: &OrderBook, size: f64) -> Option<f64> {
        if size <= 0.0 {
            return Some(0.0);
        }
        let mut remaining = size;
        let mut proceeds = 0.0;
        for level in &book.bids {
            let px = level.price_f64();
            let lvl_size = level.size_f64();
            if px <= 0.0 || lvl_size <= 0.0 {
                continue;
            }
            let take = remaining.min(lvl_size);
            proceeds += take * px;
            remaining -= take;
            if remaining <= 1e-9 {
                break;
            }
        }
        if remaining > 1e-6 {
            None
        } else {
            Some(proceeds)
        }
    }

    fn env_u64(key: &str, default: u64) -> u64 {
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(default)
    }

    fn env_f64(key: &str, default: f64) -> f64 {
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(default)
    }

    /// Polymarket market-BUY validation can reject orders when quote notional has
    /// unsupported precision. Pick the largest <= `max_size` that keeps notional
    /// aligned to 2 decimal places (USDC cents), while respecting `min_size`.
    fn quantize_market_buy_size(max_size: f64, price: f64, min_size: f64) -> Option<f64> {
        if !max_size.is_finite() || !price.is_finite() || max_size <= 0.0 || price <= 0.0 {
            return None;
        }
        let max_cents = (max_size * 100.0).floor() as i64;
        let min_cents = (min_size * 100.0).ceil() as i64;
        if max_cents < min_cents || min_cents <= 0 {
            return None;
        }

        let step = Self::market_buy_step_cents(price)?;
        let quantized_cents = (max_cents / step) * step;
        if quantized_cents < min_cents {
            return None;
        }
        Some(quantized_cents as f64 / 100.0)
    }

    /// Pick the smallest >= `min_size` that satisfies market-BUY quote precision.
    fn quantize_market_buy_size_up(
        min_size: f64,
        price: f64,
        min_order_size: f64,
        max_size: f64,
    ) -> Option<f64> {
        if !min_size.is_finite()
            || !max_size.is_finite()
            || !price.is_finite()
            || min_size <= 0.0
            || max_size <= 0.0
            || price <= 0.0
        {
            return None;
        }
        let floor_min = min_size.max(min_order_size);
        let min_cents = (floor_min * 100.0).ceil() as i64;
        let max_cents = (max_size * 100.0).floor() as i64;
        if max_cents < min_cents || min_cents <= 0 {
            return None;
        }
        let step = Self::market_buy_step_cents(price)?;
        let quantized_cents = ((min_cents + step - 1) / step) * step;
        if quantized_cents > max_cents {
            return None;
        }
        Some(quantized_cents as f64 / 100.0)
    }

    fn market_buy_step_cents(price: f64) -> Option<i64> {
        if !price.is_finite() || price <= 0.0 {
            return None;
        }
        let price_cents = ((price * 100.0).round() as i64).max(1) as u64;
        let step = (100_u64 / Self::gcd_u64(price_cents, 100_u64)) as i64;
        if step <= 0 {
            return None;
        }
        Some(step)
    }

    fn gcd_u64(mut a: u64, mut b: u64) -> u64 {
        while b != 0 {
            let t = b;
            b = a % b;
            a = t;
        }
        a.max(1)
    }

    fn snap_price_to_tick(price: f64, tick_size: f64, round_up: bool) -> f64 {
        let tick = tick_size.max(0.0001);
        let ratio = (price / tick).max(0.0);
        let snapped = if round_up {
            ratio.ceil() * tick
        } else {
            ratio.floor() * tick
        };
        let bounded = snapped.clamp(tick, 1.0 - tick);
        // Keep a stable decimal representation for SDK validation checks.
        ((bounded * 10_000.0).round()) / 10_000.0
    }

    fn truncate_for_action(message: &str, max_chars: usize) -> String {
        if message.chars().count() <= max_chars {
            return message.to_string();
        }
        let mut out = message.chars().take(max_chars).collect::<String>();
        out.push_str("...");
        out
    }

    fn is_balance_allowance_rejection(error: &anyhow::Error) -> bool {
        let msg = Self::format_error_chain(error).to_ascii_lowercase();
        msg.contains("not enough balance")
            || msg.contains("insufficient balance")
            || msg.contains("allowance")
    }

    fn taker_time_in_force() -> TimeInForce {
        match std::env::var("TAKER_ORDER_TYPE")
            .unwrap_or_else(|_| "FAK".to_string())
            .trim()
            .to_uppercase()
            .as_str()
        {
            "FOK" => TimeInForce::Fok,
            _ => TimeInForce::Fak,
        }
    }

    fn taker_tif_label(tif: TimeInForce) -> &'static str {
        Self::tif_label(tif)
    }

    fn hedge_time_in_force() -> TimeInForce {
        match std::env::var("HEDGE_ORDER_TYPE")
            .unwrap_or_else(|_| "FAK".to_string())
            .trim()
            .to_uppercase()
            .as_str()
        {
            "FOK" => TimeInForce::Fok,
            "GTC" => TimeInForce::Gtc,
            "GTD" => TimeInForce::Gtd,
            _ => TimeInForce::Fak,
        }
    }

    fn tif_label(tif: TimeInForce) -> &'static str {
        match tif {
            TimeInForce::Fok => "FOK",
            TimeInForce::Fak => "FAK",
            TimeInForce::Gtc => "GTC",
            TimeInForce::Gtd => "GTD",
        }
    }

    fn plan_taker_batches(total_size: f64) -> Vec<f64> {
        let total_size = total_size.floor();
        if total_size < 1.0 {
            return Vec::new();
        }
        let min_child = Self::env_f64("MIN_CHILD_ORDER_SIZE", 5.0).max(5.0).floor();
        let target_child = Self::env_f64("TARGET_CHILD_ORDER_SIZE", 5.0)
            .max(min_child)
            .floor();
        let max_batches = Self::env_u64("MAX_TAKER_BATCHES", 8).max(1) as usize;

        let mut batches = (total_size / target_child).ceil().max(1.0) as usize;
        batches = batches.clamp(1, max_batches);

        let mut batch_size = (total_size / batches as f64).floor().max(min_child);
        while batches > 1 && batch_size < min_child {
            batches -= 1;
            batch_size = (total_size / batches as f64).floor().max(min_child);
        }

        let mut planned = Vec::with_capacity(batches);
        for idx in 0..batches {
            let sz = Self::batch_order_size(idx, batches, total_size, batch_size).floor();
            if sz > 0.0 {
                planned.push(sz);
            }
        }

        if planned.len() >= 2 {
            let last = planned[planned.len() - 1];
            if last > 0.0 && last < min_child {
                let last_idx = planned.len() - 1;
                planned[last_idx - 1] += last;
                planned.pop();
            }
        }

        planned
    }

    async fn pre_submit_revalidate(
        &mut self,
        mi: &MarketInfo,
        opp: &ArbOpportunity,
        size: f64,
        opportunity_size: f64,
        detect_ts: chrono::DateTime<Utc>,
    ) -> Result<(OrderBook, OrderBook)> {
        let max_signal_age_ms =
            Self::env_u64("PRE_SUBMIT_SIGNAL_MAX_AGE_MS", PRE_SUBMIT_SIGNAL_MAX_AGE_MS);
        let signal_age_ms = (Utc::now() - detect_ts).num_milliseconds().max(0) as u64;
        if signal_age_ms > max_signal_age_ms {
            anyhow::bail!(
                "stale opportunity dropped: signal age {}ms exceeds {}ms",
                signal_age_ms,
                max_signal_age_ms
            );
        }

        let (yes_book, no_book) = self
            .fetch_books(mi)
            .await
            .ok_or_else(|| anyhow!("pre-submit revalidation failed: missing orderbooks"))?;

        let yes_best = yes_book
            .best_ask()
            .ok_or_else(|| anyhow!("pre-submit revalidation failed: YES best ask missing"))?;
        let no_best = no_book
            .best_ask()
            .ok_or_else(|| anyhow!("pre-submit revalidation failed: NO best ask missing"))?;

        let max_pair_drift = Self::env_f64("PRE_SUBMIT_PAIR_DRIFT_MAX", PRE_SUBMIT_PAIR_DRIFT_MAX)
            .max(0.0);
        let pair_drift = (yes_best + no_best) - opp.total_cost;
        if pair_drift > max_pair_drift {
            anyhow::bail!(
                "stale opportunity dropped: pair cost drifted by +{pair_drift:.4} (max +{max_pair_drift:.4})"
            );
        }

        let max_ask_slip = Self::env_f64("PRE_SUBMIT_ASK_SLIP_MAX", mi.tick_size.max(0.001))
            .max(0.0);
        if yes_best > opp.yes_price + max_ask_slip || no_best > opp.no_price + max_ask_slip {
            anyhow::bail!(
                "stale opportunity dropped: best ask moved beyond cap (YES {:.4}->{:.4}, NO {:.4}->{:.4})",
                opp.yes_price,
                yes_best,
                opp.no_price,
                no_best
            );
        }

        let min_liq_factor = Self::env_f64("PRE_SUBMIT_MIN_LIQ_FACTOR", PRE_SUBMIT_MIN_LIQ_FACTOR)
            .clamp(0.1, 1.0);
        let yes_liq = yes_book.ask_liquidity_at(opp.yes_price);
        let no_liq = no_book.ask_liquidity_at(opp.no_price);
        let needed = size * min_liq_factor;
        if yes_liq < needed || no_liq < needed {
            anyhow::bail!(
                "stale opportunity dropped: depth decayed (YES {:.2}, NO {:.2}, need {:.2})",
                yes_liq,
                no_liq,
                needed
            );
        }

        let active_fee_rate = if self.cached_fee_rate_bps > 0 {
            self.config.clob_fee_rate
        } else {
            0.0
        };
        let fee_yes = active_fee_rate
            * (yes_best * (1.0 - yes_best)).powf(self.config.clob_fee_exponent);
        let fee_no = active_fee_rate
            * (no_best * (1.0 - no_best)).powf(self.config.clob_fee_exponent);
        let effective_threshold = 1.0 - fee_yes.max(fee_no);
        let edge_per_share = effective_threshold - (yes_best + no_best);
        let profit_ref_size = opportunity_size.max(size).max(1.0);
        let min_edge_per_share = self.effective_min_profit_usd() / profit_ref_size;
        if edge_per_share < min_edge_per_share {
            anyhow::bail!(
                "stale opportunity dropped: edge {:.4}/share below min {:.4}/share after revalidation",
                edge_per_share,
                min_edge_per_share
            );
        }

        Ok((yes_book, no_book))
    }

    fn format_error_chain(err: &anyhow::Error) -> String {
        err.chain()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
            .join(" | ")
    }

    fn is_retryable_error(message: &str) -> bool {
        let msg = message.to_ascii_lowercase();
        [
            "timed out",
            "timeout",
            "deadline",
            "connection reset",
            "connection refused",
            "connection closed",
            "broken pipe",
            "temporarily unavailable",
            "temporarily overloaded",
            "service unavailable",
            "bad gateway",
            "gateway timeout",
            "too many requests",
            "rate limit",
            "error sending request",
            "connect error",
            "transport",
            "network",
            "econnreset",
            "econnrefused",
            "ecanceled",
            "http status: 425",
            "http status: 429",
            "http status: 500",
            "http status: 502",
            "http status: 503",
            "http status: 504",
            "code: 425",
            "code: 429",
            "code: 500",
            "code: 502",
            "code: 503",
            "code: 504",
        ]
        .iter()
        .any(|needle| msg.contains(needle))
    }

    fn retry_backoff_ms(retry_idx: u64) -> u64 {
        let base = Self::env_u64("SDK_RETRY_BASE_DELAY_MS", SDK_RETRY_BASE_DELAY_MS).max(1);
        let max_delay = Self::env_u64("SDK_RETRY_MAX_DELAY_MS", SDK_RETRY_MAX_DELAY_MS).max(base);
        let jitter_max = Self::env_u64("SDK_RETRY_JITTER_MS", SDK_RETRY_JITTER_MS);
        let exp = base.saturating_mul(1u64 << retry_idx.min(6));
        let mut delay = exp.min(max_delay);
        if jitter_max > 0 {
            let jitter = rand::thread_rng().gen_range(0..=jitter_max);
            delay = delay.saturating_add(jitter);
        }
        delay
    }

    async fn run_network_call<T, Op, Fut>(
        &self,
        operation: &'static str,
        max_retries: u64,
        timeout_ms: u64,
        mut op: Op,
    ) -> Result<T>
    where
        Op: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut retry_idx = 0u64;

        loop {
            let call = tokio::time::timeout(Duration::from_millis(timeout_ms), op()).await;
            match call {
                Ok(Ok(value)) => return Ok(value),
                Ok(Err(err)) => {
                    let chained = Self::format_error_chain(&err);
                    let retryable = Self::is_retryable_error(&chained);
                    if retryable && retry_idx < max_retries {
                        let delay_ms = Self::retry_backoff_ms(retry_idx);
                        warn!(
                            "{operation} attempt {} failed (retryable): {} — retrying in {}ms",
                            retry_idx + 1,
                            chained,
                            delay_ms
                        );
                        retry_idx += 1;
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        continue;
                    }
                    return Err(err).with_context(|| {
                        format!(
                            "{operation} failed after {} attempt(s)",
                            retry_idx + 1
                        )
                    });
                }
                Err(_) => {
                    let timeout_err = anyhow!("{operation} timed out after {timeout_ms}ms");
                    if retry_idx < max_retries {
                        let delay_ms = Self::retry_backoff_ms(retry_idx);
                        warn!(
                            "{operation} attempt {} timed out ({}ms) — retrying in {}ms",
                            retry_idx + 1,
                            timeout_ms,
                            delay_ms
                        );
                        retry_idx += 1;
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        continue;
                    }
                    return Err(timeout_err).with_context(|| {
                        format!(
                            "{operation} failed after {} attempt(s)",
                            retry_idx + 1
                        )
                    });
                }
            }
        }
    }

    async fn run_sdk_call<T, F>(&self, operation: &'static str, fut: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        let timeout_ms = Self::env_u64("SDK_CALL_TIMEOUT_MS", SDK_CALL_TIMEOUT_MS);
        let timeout = Duration::from_millis(timeout_ms);
        tokio::time::timeout(timeout, fut)
            .await
            .with_context(|| format!("{operation} timed out after {timeout_ms}ms"))?
            .with_context(|| format!("{operation} failed"))
    }

    fn map_posted_order_ids(
        yes_posted_first: bool,
        responses: &[crate::types::OrderResponse],
    ) -> PostedOrderIds {
        let first = responses
            .first()
            .map(|r| r.order_id.clone())
            .unwrap_or_default();
        let second = responses
            .get(1)
            .map(|r| r.order_id.clone())
            .unwrap_or_default();

        if yes_posted_first {
            PostedOrderIds {
                yes_order_id: first,
                no_order_id: second,
            }
        } else {
            PostedOrderIds {
                yes_order_id: second,
                no_order_id: first,
            }
        }
    }

    fn mark_ws_trade_seen(&mut self, trade_id: &str) -> bool {
        if trade_id.is_empty() {
            return true;
        }
        if self.seen_ws_trade_ids.contains(trade_id) {
            return false;
        }
        let owned = trade_id.to_string();
        self.seen_ws_trade_ids.insert(owned.clone());
        self.ws_trade_id_queue.push_back(owned);
        while self.ws_trade_id_queue.len() > MAX_SEEN_WS_TRADE_IDS {
            if let Some(evicted) = self.ws_trade_id_queue.pop_front() {
                self.seen_ws_trade_ids.remove(&evicted);
            }
        }
        true
    }

    fn mark_response_fill_applied(&mut self, order_id: &str) {
        if order_id.is_empty() {
            return;
        }
        let owned = order_id.to_string();
        if !self.response_applied_order_ids.insert(owned.clone()) {
            return;
        }
        self.response_applied_order_queue.push_back(owned);
        while self.response_applied_order_queue.len() > MAX_RESPONSE_APPLIED_ORDER_IDS {
            if let Some(evicted) = self.response_applied_order_queue.pop_front() {
                self.response_applied_order_ids.remove(&evicted);
            }
        }
    }

    fn response_fill_already_applied(&self, order_id: &str) -> bool {
        !order_id.is_empty() && self.response_applied_order_ids.contains(order_id)
    }

    fn fills_sufficient(yes_filled: f64, no_filled: f64, target_size: f64) -> bool {
        let target_fill = target_size * 0.95;
        yes_filled >= target_fill && no_filled >= target_fill
    }

    fn session_successes(&self) -> u64 {
        self.stats
            .successes()
            .saturating_sub(self.session_successes_baseline)
    }

    fn session_failures(&self) -> u64 {
        self.stats
            .failures()
            .saturating_sub(self.session_failures_baseline)
    }

    fn batch_order_size(batch_idx: usize, total_batches: usize, total_size: f64, batch_size: f64) -> f64 {
        if total_batches == 0 {
            return 0.0;
        }
        if batch_idx + 1 == total_batches {
            let remaining = (total_size - (batch_idx as f64 * batch_size)).max(0.0);
            (remaining * 100.0).floor() / 100.0
        } else {
            batch_size
        }
    }

    async fn sample_ws_age_if_due(&mut self, mi: &MarketInfo) {
        let due = self
            .last_ws_age_sample
            .map(|t| t.elapsed() >= Duration::from_millis(WS_AGE_SAMPLE_INTERVAL_MS))
            .unwrap_or(true);
        if !due {
            return;
        }
        self.last_ws_age_sample = Some(Instant::now());

        let Some(ws) = self.ws_client.as_ref() else {
            self.ws_yes_age_ms = None;
            self.ws_no_age_ms = None;
            return;
        };

        let yes_age = ws.get_book_age_ms(&mi.tokens.yes).await;
        let no_age = ws.get_book_age_ms(&mi.tokens.no).await;
        self.ws_yes_age_ms = yes_age;
        self.ws_no_age_ms = no_age;

        // Mirror TS behavior: don't treat a fresh WS reconnect as stale immediately.
        // First snapshot/delta can arrive a bit late around rollover windows.
        let within_grace = self
            .ws_connected_at
            .map(|t| t.elapsed() < Duration::from_secs(WS_STALE_GRACE_SECS))
            .unwrap_or(false);
        if within_grace {
            self.ws_stale_consecutive = 0;
            self.ws_stale_warned = false;
            return;
        }

        let stale = Self::ws_ages_stale(yes_age, no_age);

        if stale {
            self.ws_stale_consecutive = self.ws_stale_consecutive.saturating_add(1);
            if self.ws_stale_consecutive >= WS_STALE_SAMPLE_LIMIT && !self.ws_stale_warned {
                self.ws_stale_warned = true;
                self.ws_reconnect_requested = true;
                self.log_action("⚠️ WS stale feed detected — using REST fallback")
                    .await;
            }
        } else {
            if self.ws_stale_warned {
                self.log_action("✅ WS feed fresh again — resuming WS pricing")
                    .await;
            }
            self.ws_stale_consecutive = 0;
            self.ws_stale_warned = false;
            self.ws_reconnect_requested = false;
        }
    }

    fn should_attempt_claim(&self, condition_id: &str, amount: f64, now: Instant) -> bool {
        if amount < CLAIM_MIN_AMOUNT_USDC {
            return false;
        }
        if self
            .claim_success_suppression_until
            .get(condition_id)
            .map(|until| now < *until)
            .unwrap_or(false)
        {
            return false;
        }
        if self
            .claim_retry_after
            .get(condition_id)
            .map(|until| now < *until)
            .unwrap_or(false)
        {
            return false;
        }
        true
    }

    fn ws_ages_stale(yes_age_ms: Option<u64>, no_age_ms: Option<u64>) -> bool {
        let threshold = crate::ws_client::BOOK_STALE_THRESHOLD_MS;
        yes_age_ms.map(|v| v > threshold).unwrap_or(true)
            || no_age_ms.map(|v| v > threshold).unwrap_or(true)
    }

    fn record_check_latency(&mut self, elapsed: Duration) {
        Self::push_latency_sample(&mut self.check_latency_ms, elapsed);
        self.maybe_log_latency_summary();
    }

    fn record_claim_latency(&mut self, elapsed: Duration) {
        Self::push_latency_sample(&mut self.claim_latency_ms, elapsed);
        self.maybe_log_latency_summary();
    }

    fn record_balance_latency(&mut self, elapsed: Duration) {
        Self::push_latency_sample(&mut self.balance_latency_ms, elapsed);
        self.maybe_log_latency_summary();
    }

    fn record_lock_wait_latency(&mut self, elapsed: Duration) {
        Self::push_latency_sample(&mut self.lock_wait_latency_ms, elapsed);
        self.maybe_log_latency_summary();
    }

    fn record_render_latency(&mut self, elapsed: Duration) {
        Self::push_latency_sample(&mut self.render_latency_ms, elapsed);
        self.maybe_log_latency_summary();
    }

    fn should_log_no_edge_skip(&mut self, net_spread: f64, resting_min_edge: f64) -> bool {
        let cooldown_ms = Self::env_u64("NO_EDGE_LOG_COOLDOWN_MS", 1_200).max(100);
        let key = format!("{net_spread:.3}|{resting_min_edge:.3}");
        let recent_duplicate = self
            .last_no_edge_skip_key
            .as_ref()
            .map(|prev| prev == &key)
            .unwrap_or(false);
        let cooldown_active = self
            .last_no_edge_skip_at
            .map(|t| t.elapsed() < Duration::from_millis(cooldown_ms))
            .unwrap_or(false);

        self.last_no_edge_skip_key = Some(key);
        self.last_no_edge_skip_at = Some(Instant::now());
        !(recent_duplicate && cooldown_active)
    }

    fn push_latency_sample(samples: &mut VecDeque<u64>, elapsed: Duration) {
        let ms = elapsed.as_millis().min(u128::from(u64::MAX)) as u64;
        Self::push_latency_value(samples, ms);
    }

    fn push_latency_value(samples: &mut VecDeque<u64>, ms: u64) {
        samples.push_back(ms);
        while samples.len() > PERF_WINDOW_SAMPLES {
            samples.pop_front();
        }
    }

    fn percentile(samples: &VecDeque<u64>, pct: f64) -> Option<u64> {
        if samples.is_empty() {
            return None;
        }
        let mut sorted: Vec<u64> = samples.iter().copied().collect();
        sorted.sort_unstable();
        let idx = ((sorted.len() - 1) as f64 * pct).round() as usize;
        sorted.get(idx).copied()
    }

    fn maybe_log_latency_summary(&mut self) {
        if self.perf_last_log.elapsed().as_secs() < PERF_LOG_INTERVAL_SECS {
            return;
        }
        self.perf_last_log = Instant::now();

        let check_p50 = Self::percentile(&self.check_latency_ms, 0.50).unwrap_or(0);
        let check_p95 = Self::percentile(&self.check_latency_ms, 0.95).unwrap_or(0);
        let claim_p50 = Self::percentile(&self.claim_latency_ms, 0.50).unwrap_or(0);
        let claim_p95 = Self::percentile(&self.claim_latency_ms, 0.95).unwrap_or(0);
        let balance_p50 = Self::percentile(&self.balance_latency_ms, 0.50).unwrap_or(0);
        let balance_p95 = Self::percentile(&self.balance_latency_ms, 0.95).unwrap_or(0);
        let lock_wait_p50 = Self::percentile(&self.lock_wait_latency_ms, 0.50).unwrap_or(0);
        let lock_wait_p95 = Self::percentile(&self.lock_wait_latency_ms, 0.95).unwrap_or(0);
        let render_p50 = Self::percentile(&self.render_latency_ms, 0.50).unwrap_or(0);
        let render_p95 = Self::percentile(&self.render_latency_ms, 0.95).unwrap_or(0);
        let detect_submit_p50 = Self::percentile(&self.detect_to_submit_latency_ms, 0.50).unwrap_or(0);
        let detect_submit_p95 = Self::percentile(&self.detect_to_submit_latency_ms, 0.95).unwrap_or(0);
        let first_fill_p50 = Self::percentile(&self.first_fill_latency_ms, 0.50).unwrap_or(0);
        let first_fill_p95 = Self::percentile(&self.first_fill_latency_ms, 0.95).unwrap_or(0);
        let pair_p50 = Self::percentile(&self.pair_complete_latency_ms, 0.50).unwrap_or(0);
        let pair_p95 = Self::percentile(&self.pair_complete_latency_ms, 0.95).unwrap_or(0);
        let action_q = self
            .session_logger
            .as_ref()
            .map(|l| l.queue_depth())
            .unwrap_or(0);
        let action_drop = self
            .session_logger
            .as_ref()
            .map(|l| l.dropped_count())
            .unwrap_or(0);
        let trade_q = self.trade_logger.queue_depth();
        let trade_drop = self.trade_logger.dropped_count();
        let qual_rate = if self.checks_total > 0 {
            (self.checks_fee_qualified as f64 / self.checks_total as f64) * 100.0
        } else {
            0.0
        };
        let opp_to_post_rate = if self.opportunity_detected_count > 0 {
            self.post_attempt_count as f64 / self.opportunity_detected_count as f64
        } else {
            0.0
        };
        let post_to_paired_rate = if self.post_attempt_count > 0 {
            self.post_paired_count as f64 / self.post_attempt_count as f64
        } else {
            0.0
        };
        let hedge_rate = if self.post_attempt_count > 0 {
            self.hedge_event_count as f64 / self.post_attempt_count as f64
        } else {
            0.0
        };
        let stale_poll_rate = if self.post_attempt_count > 0 {
            self.stale_poll_fallback_count as f64 / self.post_attempt_count as f64
        } else {
            0.0
        };

        info!(
            "Perf latency (ms) | check {}/{} n={} | detect->submit {}/{} n={} | first-fill {}/{} n={} | pair-complete {}/{} n={} | claim {}/{} n={} | balance {}/{} n={} | lock-wait {}/{} n={} | render {}/{} n={} | queue action/trade={}/{} dropped action/trade={}/{} | opp checks={} qual={} ({:.2}%) extreme={} size={} profit={} executed={} | ratios opp->post={:.3} post->paired={:.3} hedge={:.3} stale-poll={:.3} merge-mismatch={}",
            check_p50,
            check_p95,
            self.check_latency_ms.len(),
            detect_submit_p50,
            detect_submit_p95,
            self.detect_to_submit_latency_ms.len(),
            first_fill_p50,
            first_fill_p95,
            self.first_fill_latency_ms.len(),
            pair_p50,
            pair_p95,
            self.pair_complete_latency_ms.len(),
            claim_p50,
            claim_p95,
            self.claim_latency_ms.len(),
            balance_p50,
            balance_p95,
            self.balance_latency_ms.len(),
            lock_wait_p50,
            lock_wait_p95,
            self.lock_wait_latency_ms.len(),
            render_p50,
            render_p95,
            self.render_latency_ms.len(),
            action_q,
            trade_q,
            action_drop,
            trade_drop,
            self.checks_total,
            self.checks_fee_qualified,
            qual_rate,
            self.checks_skipped_extreme,
            self.checks_skipped_size,
            self.checks_skipped_profit,
            self.checks_executed,
            opp_to_post_rate,
            post_to_paired_rate,
            hedge_rate,
            stale_poll_rate,
            self.merge_mismatch_count
        );
    }

    pub fn record_check_latency_sample(&mut self, elapsed: Duration) {
        self.record_check_latency(elapsed);
    }

    pub fn record_lock_wait_latency_sample(&mut self, elapsed: Duration) {
        self.record_lock_wait_latency(elapsed);
    }

    pub async fn log_action(&mut self, msg: &str) {
        self.dashboard.log_action(msg);
        if let Some(ref logger) = self.session_logger {
            logger.try_log(msg);
        }
        self.request_render();
    }

    pub fn log_action_fast(&mut self, msg: &str) {
        self.dashboard.log_action(msg);
        if let Some(ref logger) = self.session_logger {
            logger.try_log(msg);
        }
        self.request_render();
    }

    fn request_render(&mut self) {
        self.render_requested = true;
    }

    pub fn render_if_requested(&mut self) {
        let force_render_ms = Self::env_u64("DASHBOARD_FORCE_RENDER_MS", DASHBOARD_FORCE_RENDER_MS);
        let force_due = self.last_render_at.elapsed() >= Duration::from_millis(force_render_ms);
        if !self.render_requested && !force_due {
            return;
        }
        let started = Instant::now();
        self.render_dashboard();
        self.render_requested = false;
        self.last_render_at = Instant::now();
        self.record_render_latency(started.elapsed());
    }

    fn fee_adjust_shares(&self, ordered_size: f64, price: f64) -> f64 {
        let active_fee_rate = if self.cached_fee_rate_bps > 0 { self.config.clob_fee_rate } else { 0.0 };
        let effective_fee = active_fee_rate * (price * (1.0 - price)).powf(self.config.clob_fee_exponent);
        let actual = ordered_size * (1.0 - effective_fee);
        (actual * 100.0).floor() / 100.0
    }

    /// Compute a market-BUY order size that aims to deliver `target_net_shares`
    /// after taker fee, while satisfying quote-precision constraints.
    fn gross_buy_size_for_target_net(
        &self,
        target_net_shares: f64,
        price: f64,
        min_order_size: f64,
    ) -> Option<f64> {
        if !target_net_shares.is_finite() || !price.is_finite() || target_net_shares <= 0.0 || price <= 0.0 {
            return None;
        }

        let active_fee_rate = if self.cached_fee_rate_bps > 0 {
            self.config.clob_fee_rate
        } else {
            0.0
        };
        let effective_fee =
            active_fee_rate * (price * (1.0 - price)).powf(self.config.clob_fee_exponent);
        let denom = (1.0 - effective_fee).max(0.000_001);
        let gross_needed = (target_net_shares / denom).max(min_order_size);

        let down = Self::quantize_market_buy_size(gross_needed, price, min_order_size);
        let up = Self::quantize_market_buy_size_up(
            gross_needed,
            price,
            min_order_size,
            gross_needed + 2.0,
        );

        let mut best: Option<(f64, f64)> = None; // (order_size, absolute net error)
        for candidate in [down, up].into_iter().flatten() {
            let est_net = self.fee_adjust_shares(candidate, price);
            let err = (est_net - target_net_shares).abs();
            match best {
                None => best = Some((candidate, err)),
                Some((best_size, best_err)) => {
                    let better = err + 1e-9 < best_err
                        || ((err - best_err).abs() <= 1e-9 && candidate < best_size);
                    if better {
                        best = Some((candidate, err));
                    }
                }
            }
        }

        best.map(|(size, _)| size)
    }

    /// Render the TUI dashboard with current state.
    fn render_dashboard(&mut self) {
        self.update_entry_lock_state();
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

        let pairs = self.position.yes_size.min(self.position.no_size);
        let carry_naked_yes = (self.position.yes_size - pairs).max(0.0);
        let carry_naked_no = (self.position.no_size - pairs).max(0.0);
        let yes_mark = self.last_yes_mid_price.unwrap_or_else(|| {
            if self.position.yes_size > 0.0 {
                self.position.yes_cost / self.position.yes_size
            } else {
                0.0
            }
        });
        let no_mark = self.last_no_mid_price.unwrap_or_else(|| {
            if self.position.no_size > 0.0 {
                self.position.no_cost / self.position.no_size
            } else {
                0.0
            }
        });
        let carry_worst_case_loss = (carry_naked_yes * yes_mark).max(carry_naked_no * no_mark);

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
            session_successes: self.session_successes(),
            session_failures: self.session_failures(),
            daily_pnl: self.daily_pnl,
            execution_pnl: self.execution_pnl,
            hedge_sellback_pnl: self.hedge_sellback_pnl,
            redemption_pnl: self.redemption_pnl,
            fees_gas_estimate: self.fees_gas_estimate,
            execution_success_count: self.execution_success_count,
            economic_success_count: self.economic_success_count,
            adaptive_pressure: self.adaptive_pressure,
            adaptive_window_samples: self.recent_opportunity_outcomes.len(),
            adaptive_hedge_samples: self.recent_hedge_outcomes.len(),
            adaptive_hedge_failure_ratio: self.adaptive_hedge_failure_ratio(),
            adaptive_min_profit_usd: self.adaptive_min_profit_usd,
            adaptive_max_trade_size: self.adaptive_max_trade_size,
            carry_naked_yes,
            carry_naked_no,
            carry_worst_case_loss,
            entry_lock_reason: self.entry_lock_reason.map(Self::entry_lock_reason_label),
            entry_lock_remaining_secs: self.entry_lock_remaining_secs(),
            circuit_breaker_active: cb_active,
            circuit_breaker_remaining_secs: cb_remaining,
            data_source: &self.last_data_source,
            ws_connected: self.ws_client.is_some(),
            pending_claim_usdc: Some(self.pending_claim_value),
            ws_yes_age_ms: self.ws_yes_age_ms,
            ws_no_age_ms: self.ws_no_age_ms,
        };

        self.dashboard.render(&state);
    }

    // ─── Shutdown ─────────────────────────────────────────────────────────────

    pub async fn shutdown(&mut self) {
        info!("Shutting down market monitor…");

        // Flush stats
        self.stats.flush().await;

        // Cancel resting strategy orders
        if let Some(ref mut maker) = self.maker {
            let _ = maker.cancel_all(&self.client).await;
        }
        if let Some(ref mut strategy) = self.post_only {
            let _ = strategy.cancel_all(&self.client).await;
        }

        let end_balance = self.last_balance.unwrap_or(0.0);
        let session_pnl_usd = end_balance - self.session_start_balance;
        let session_pnl_pct = if self.session_start_balance > 0.0 {
            (session_pnl_usd / self.session_start_balance) * 100.0
        } else {
            0.0
        };
        let adaptive_hedge_failure_ratio = self.adaptive_hedge_failure_ratio();
        let mut risk_flags = Vec::new();
        if self.position.has_imbalance() {
            risk_flags.push("position_imbalance".to_string());
        }
        if let Some(ratio) = adaptive_hedge_failure_ratio {
            if ratio >= 0.50 {
                risk_flags.push(format!("high_hedge_fail_ratio:{ratio:.2}"));
            }
        }
        if let Some(reason) = self.entry_lock_reason {
            risk_flags.push(format!(
                "entry_lock:{}",
                Self::entry_lock_reason_label(reason)
            ));
        }
        if self.discovery_failure_since.is_some() {
            risk_flags.push("discovery_degraded".to_string());
        }
        let outcome = if session_pnl_usd > 0.01 {
            "win"
        } else if session_pnl_usd < -0.01 {
            "loss"
        } else {
            "flat"
        }
        .to_string();

        let config_keys = [
            "STRATEGY_MODE",
            "MAX_TRADE_SIZE",
            "RESTING_MIN_EDGE_PER_SHARE",
            "MAKER_SPREAD_TICKS",
            "MIN_NET_PROFIT_USD",
            "IMBALANCE_MAX_EXPECTED_LOSS_USD",
            "IMBALANCE_LOSS_GUARD_RETRY_SECS",
            "IMBALANCE_LOSS_GUARD_DISABLE_BELOW_SECS",
            "IMBALANCE_RELAYER_GRACE_SECS",
            "IMBALANCE_RELAYER_RETRY_SECS",
        ];
        let config_snapshot = config_keys
            .iter()
            .map(|key| {
                (
                    (*key).to_string(),
                    std::env::var(key).unwrap_or_else(|_| "<unset>".to_string()),
                )
            })
            .collect::<Vec<_>>();

        let run_entry = RunMemoryEntry {
            timestamp: Utc::now(),
            market_slug: self.config.market_slugs.join(","),
            strategy_mode: self.config.strategy_mode.as_str().to_string(),
            session_start_balance: self.session_start_balance,
            session_end_balance: end_balance,
            session_pnl_usd,
            session_pnl_pct,
            execution_pnl: self.execution_pnl,
            hedge_sellback_pnl: self.hedge_sellback_pnl,
            redemption_cashin_usd: self.redemption_pnl,
            fees_gas_estimate: self.fees_gas_estimate,
            session_successes: self.session_successes(),
            session_failures: self.session_failures(),
            execution_success_count: self.execution_success_count,
            economic_success_count: self.economic_success_count,
            adaptive_pressure: self.adaptive_pressure,
            adaptive_min_profit_usd: self.adaptive_min_profit_usd,
            adaptive_max_trade_size: self.adaptive_max_trade_size,
            adaptive_hedge_failure_ratio,
            position_yes_size: self.position.yes_size,
            position_no_size: self.position.no_size,
            position_yes_cost: self.position.yes_cost,
            position_no_cost: self.position.no_cost,
            pending_claim_usdc: self.pending_claim_value,
            entry_lock_reason: self
                .entry_lock_reason
                .map(|r| Self::entry_lock_reason_label(r).to_string()),
            outcome,
            risk_flags,
            config_snapshot,
        };
        if let Err(e) = append_run_memory_entry(&run_entry).await {
            warn!("Failed to append run memory entry: {e}");
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
    use crate::types::{OrderResponse, PriceLevel};
    use tokio::sync::mpsc;

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
        cfg.strategy_mode = StrategyMode::Taker;
        cfg.maker_mode_enabled = false;
        cfg.maker_spread_ticks = 2;
        cfg.gtc_taker_timeout_ms = 5_000;
        cfg.ws_fill_primary = true;
        cfg.ws_fill_fallback_poll_ms = 300;
        cfg.strict_neutral_mode = true;
        cfg.neutrality_resume_net_shares = 0.25;
        cfg.timeout_recovery_lock_secs = 10;
        cfg.timeout_recovery_poll_ms = 200;
        cfg.discovery_degraded_secs = 60;
        cfg.adaptive_throttle_min_ms = 0;
        cfg.adaptive_throttle_burst_debounce_ms = 8;
        cfg.actionable_delta_min_ticks = 1;
        cfg.shadow_engine_enabled = false;
        cfg.shadow_engine_send_orders = false;
        cfg.merge_reconcile_interval_secs = 5;
        cfg.clob_fee_rate = 0.15;
        cfg.clob_fee_exponent = 2.0;
        Some(Arc::new(cfg))
    }

    async fn mock_monitor() -> Option<MarketMonitor> {
        let config = mock_config()?;

        let client = ClobClient::new(Arc::clone(&config)).await.ok()?;
        let (merge_result_tx, merge_result_rx) = mpsc::unbounded_channel();
        Some(MarketMonitor {
            config: Arc::clone(&config),
            client: Arc::new(client),
            ws_client: None,
            ws_notify: None,
            session_logger: None,
            trade_logger: TradeLogger::new().await.ok()?,
            stats: MarketStatsTracker::load().await.ok()?,
            maker: None,
            post_only: None,
            market_info: None,
            position: Position::default(),
            session_locked_value: 0.0,
            session_start_balance: 1000.0, // Mock balance
            session_successes_baseline: 0,
            session_failures_baseline: 0,
            daily_pnl: 0.0,
            execution_pnl: 0.0,
            hedge_sellback_pnl: 0.0,
            redemption_pnl: 0.0,
            fees_gas_estimate: 0.0,
            execution_success_count: 0,
            economic_success_count: 0,
            consecutive_failures: 0,
            circuit_breaker_until: None,
            entry_lock_until: None,
            entry_lock_reason: None,
            timeout_recovery_active: false,
            unresolved_ambiguous_batches: HashSet::new(),
            last_entry_lock_log_reason: None,
            last_entry_lock_log_at: None,
            hedge_cooldown_until: None,
            consecutive_hedge_failures: 0,
            active_assets: HashSet::new(),
            gas_cache: GasCache::default(),
            _gas_cache_refreshed: None,
            last_merge_attempt: None,
            last_claim_poll: None,
            last_market_discovery_attempt: None,
            discovery_failure_since: None,
            last_discovery_degraded_warn_at: None,
            last_discovery_self_heal_at: None,
            last_discovery_probe_at: None,
            force_claim_scan: false,
            last_runtime_tick: Instant::now(),
            last_resume_recovery_at: None,
            last_check: None,
            errors: Vec::new(),
            is_redeeming: Arc::new(AtomicBool::new(false)),
            pending_claim_value: 0.0,
            order_tracker: HashMap::new(),
            order_to_batch: HashMap::new(),
            execution_batches: HashMap::new(),
            ws_fill_last_event_at: None,
            last_actionable_snapshot: None,
            pending_merges: HashMap::new(),
            pending_merge_reserved_size: 0.0,
            merge_result_tx,
            merge_result_rx,
            dashboard: Dashboard::new(),
            last_balance: Some(1000.0),
            last_balance_refresh: Instant::now(),
            last_data_source: "REST".to_string(),
            cached_fee_rate_bps: 100, // 1% basis points
            last_tick_signature: None,
            ws_yes_age_ms: None,
            ws_no_age_ms: None,
            ws_reconnect_requested: false,
            ws_stale_warned: false,
            ws_stale_consecutive: 0,
            ws_connected_at: None,
            last_ws_age_sample: None,
            claim_retry_after: HashMap::new(),
            claim_success_suppression_until: HashMap::new(),
            check_latency_ms: VecDeque::new(),
            detect_to_submit_latency_ms: VecDeque::new(),
            first_fill_latency_ms: VecDeque::new(),
            pair_complete_latency_ms: VecDeque::new(),
            claim_latency_ms: VecDeque::new(),
            balance_latency_ms: VecDeque::new(),
            lock_wait_latency_ms: VecDeque::new(),
            render_latency_ms: VecDeque::new(),
            checks_total: 0,
            checks_fee_qualified: 0,
            checks_skipped_extreme: 0,
            checks_skipped_size: 0,
            checks_skipped_profit: 0,
            checks_executed: 0,
            opportunity_detected_count: 0,
            post_attempt_count: 0,
            post_paired_count: 0,
            hedge_event_count: 0,
            stale_poll_fallback_count: 0,
            merge_mismatch_count: 0,
            perf_last_log: Instant::now(),
            render_requested: true,
            last_render_at: Instant::now(),
            seen_ws_trade_ids: HashSet::new(),
            ws_trade_id_queue: VecDeque::with_capacity(MAX_SEEN_WS_TRADE_IDS + 1),
            response_applied_order_ids: HashSet::new(),
            response_applied_order_queue: VecDeque::with_capacity(MAX_RESPONSE_APPLIED_ORDER_IDS + 1),
            last_position_activity_at: None,
            reconcile_drift_streak: 0,
            adaptive_min_profit_usd: config.min_net_profit_usd,
            adaptive_max_trade_size: config.max_trade_size,
            adaptive_pressure: 0.0,
            recent_opportunity_outcomes: VecDeque::new(),
            recent_hedge_outcomes: VecDeque::new(),
            last_hedge_outcome_at: None,
            last_yes_mid_price: None,
            last_no_mid_price: None,
            last_no_edge_skip_key: None,
            last_no_edge_skip_at: None,
            seen_rest_trade_ids: HashSet::new(),
            rest_trade_id_queue: VecDeque::with_capacity(MAX_SEEN_WS_TRADE_IDS + 1),
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

    #[test]
    fn test_quantize_market_buy_size_aligns_quote_precision() {
        // At 0.53 price, 6.15 shares implies $3.2595 quote (4 dp) and is rejected.
        // Quantizer should snap down to an integer share amount so quote is 2 dp.
        let q = MarketMonitor::quantize_market_buy_size(6.15, 0.53, 5.0);
        assert_eq!(q, Some(6.0));

        // If max size cannot satisfy min size after quantization, return None.
        let q2 = MarketMonitor::quantize_market_buy_size(5.49, 0.53, 5.5);
        assert_eq!(q2, None);
    }

    #[test]
    fn test_quantize_market_buy_size_up_aligns_quote_precision() {
        // At 0.53, valid market-BUY sizes are integer shares.
        let q = MarketMonitor::quantize_market_buy_size_up(6.15, 0.53, 5.0, 7.2);
        assert_eq!(q, Some(7.0));
    }

    #[tokio::test]
    async fn test_gross_buy_size_for_target_net_is_fee_aware() {
        let Some(monitor) = mock_monitor().await else {
            return;
        };
        let target_net = 5.40;
        let Some(order_size) = monitor.gross_buy_size_for_target_net(target_net, 0.50, 5.0) else {
            panic!("expected a valid fee-aware order size");
        };
        let net_estimate = monitor.fee_adjust_shares(order_size, 0.50);
        // Fee-aware helper should not under-order versus target by more than 0.02 (size precision floor).
        assert!(net_estimate + 0.020_001 >= target_net);
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
        monitor.adaptive_max_trade_size = 5000.0;
        
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

    #[tokio::test]
    async fn test_claim_candidate_blocked_by_retry_and_suppression() {
        let Some(mut monitor) = mock_monitor().await else {
            return;
        };
        let now = Instant::now();
        let cid = "0xabc".to_string();

        monitor
            .claim_retry_after
            .insert(cid.clone(), now + Duration::from_secs(30));
        assert!(!monitor.should_attempt_claim(&cid, 1.0, now));

        monitor.claim_retry_after.clear();
        monitor
            .claim_success_suppression_until
            .insert(cid.clone(), now + Duration::from_secs(90));
        assert!(!monitor.should_attempt_claim(&cid, 1.0, now));

        monitor.claim_success_suppression_until.clear();
        assert!(monitor.should_attempt_claim(&cid, 1.0, now));
    }

    #[test]
    fn test_ws_stale_detection() {
        let threshold = crate::ws_client::BOOK_STALE_THRESHOLD_MS;
        assert!(MarketMonitor::ws_ages_stale(None, Some(10)));
        assert!(MarketMonitor::ws_ages_stale(Some(threshold + 1), Some(10)));
        assert!(MarketMonitor::ws_ages_stale(Some(10), Some(threshold + 1)));
        assert!(!MarketMonitor::ws_ages_stale(Some(10), Some(10)));
    }

    #[test]
    fn test_posted_order_ids_yes_first_mapping() {
        let responses = vec![
            OrderResponse {
                order_id: "yes-order".to_string(),
                status: None,
                size_matched: None,
                error_msg: None,
            },
            OrderResponse {
                order_id: "no-order".to_string(),
                status: None,
                size_matched: None,
                error_msg: None,
            },
        ];

        let ids = MarketMonitor::map_posted_order_ids(true, &responses);
        assert_eq!(ids.yes_order_id, "yes-order");
        assert_eq!(ids.no_order_id, "no-order");
    }

    #[test]
    fn test_posted_order_ids_no_first_mapping() {
        let responses = vec![
            OrderResponse {
                order_id: "no-order".to_string(),
                status: None,
                size_matched: None,
                error_msg: None,
            },
            OrderResponse {
                order_id: "yes-order".to_string(),
                status: None,
                size_matched: None,
                error_msg: None,
            },
        ];

        let ids = MarketMonitor::map_posted_order_ids(false, &responses);
        assert_eq!(ids.yes_order_id, "yes-order");
        assert_eq!(ids.no_order_id, "no-order");
    }

    #[test]
    fn test_fill_threshold_based_on_size_not_price() {
        // Target size 20 => 95% threshold is 19 shares on each leg.
        assert!(MarketMonitor::fills_sufficient(19.0, 19.1, 20.0));
        assert!(!MarketMonitor::fills_sufficient(18.99, 19.5, 20.0));
        assert!(!MarketMonitor::fills_sufficient(19.5, 18.99, 20.0));
    }

    #[test]
    fn test_batch_order_size_last_batch_remainder_not_doubled() {
        // 12 shares split into 2x6 should produce 6 in both batches.
        let b1 = MarketMonitor::batch_order_size(0, 2, 12.0, 6.0);
        let b2 = MarketMonitor::batch_order_size(1, 2, 12.0, 6.0);
        assert_eq!(b1, 6.0);
        assert_eq!(b2, 6.0);
    }

    #[tokio::test]
    async fn test_session_successes_uses_startup_baseline() {
        let Some(mut monitor) = mock_monitor().await else {
            return;
        };
        monitor.session_successes_baseline = 5;
        monitor.stats.stats.successes = 8;
        assert_eq!(monitor.session_successes(), 3);
    }

    #[tokio::test]
    async fn test_session_failures_uses_startup_baseline() {
        let Some(mut monitor) = mock_monitor().await else {
            return;
        };
        monitor.session_failures_baseline = 2;
        monitor.stats.stats.failures = 7;
        assert_eq!(monitor.session_failures(), 5);
    }

    #[tokio::test]
    async fn test_failed_opportunity_counts_once_even_with_multiple_failed_batches() {
        let Some(mut monitor) = mock_monitor().await else {
            return;
        };
        monitor.stats.stats.total_arb_executions = 10;
        monitor.stats.stats.failures = 3;
        monitor.stats.stats.total_pnl_usd = 9.5;

        let (success, recorded_pnl) = monitor.finalize_opportunity_outcome(3, 0, 1.2);

        assert!(!success);
        assert_eq!(recorded_pnl, 0.0);
        assert_eq!(monitor.stats.stats.total_arb_executions, 11);
        assert_eq!(monitor.stats.stats.failures, 4);
        assert_eq!(monitor.consecutive_failures, 1);
        assert_eq!(monitor.stats.stats.total_pnl_usd, 9.5);
    }

    #[tokio::test]
    async fn test_successful_opportunity_resets_failure_streak_and_records_pnl_once() {
        let Some(mut monitor) = mock_monitor().await else {
            return;
        };
        monitor.consecutive_failures = 4;
        monitor.stats.stats.total_arb_executions = 7;
        monitor.stats.stats.successes = 2;
        monitor.stats.stats.failures = 5;
        monitor.stats.stats.total_pnl_usd = 1.0;
        monitor.execution_pnl = 0.3;
        monitor.sync_daily_pnl_from_components();

        let (success, recorded_pnl) = monitor.finalize_opportunity_outcome(3, 2, 0.7);

        assert!(success);
        assert_eq!(recorded_pnl, 0.7);
        assert_eq!(monitor.consecutive_failures, 0);
        assert_eq!(monitor.stats.stats.total_arb_executions, 8);
        assert_eq!(monitor.stats.stats.successes, 3);
        assert_eq!(monitor.stats.stats.failures, 5);
        assert!((monitor.stats.stats.total_pnl_usd - 1.7).abs() < 1e-9);
        assert!((monitor.daily_pnl - 0.0).abs() < 1e-9);
        assert_eq!(monitor.execution_success_count, 1);
        assert_eq!(monitor.economic_success_count, 1);
    }

    #[tokio::test]
    async fn test_circuit_breaker_trips_once_per_failed_opportunity() {
        let Some(mut monitor) = mock_monitor().await else {
            return;
        };
        monitor.config = Arc::new(Config {
            max_consecutive_failures: 2,
            ..(*monitor.config).clone()
        });
        monitor.consecutive_failures = 1;
        monitor.stats.stats.total_arb_executions = 20;
        monitor.stats.stats.failures = 10;

        let (success, _) = monitor.finalize_opportunity_outcome(3, 0, 0.0);

        assert!(!success);
        assert_eq!(monitor.consecutive_failures, 2);
        assert_eq!(monitor.stats.stats.total_arb_executions, 21);
        assert_eq!(monitor.stats.stats.failures, 11);
        assert!(monitor.circuit_breaker_until.is_some());
    }

    #[tokio::test]
    async fn test_entry_lock_engages_on_imbalance() {
        let Some(mut monitor) = mock_monitor().await else {
            return;
        };
        monitor.position.yes_size = 3.0;
        monitor.position.no_size = 1.0;
        monitor.position.yes_cost = 1.2;
        monitor.position.no_cost = 0.4;

        monitor.update_entry_lock_state();
        assert_eq!(monitor.entry_lock_reason, Some(EntryLockReason::Imbalance));
    }

    #[tokio::test]
    async fn test_timeout_entry_lock_releases_after_window_when_neutral() {
        let Some(mut monitor) = mock_monitor().await else {
            return;
        };
        monitor.timeout_recovery_active = true;
        monitor.entry_lock_until = Some(Instant::now() - Duration::from_millis(5));
        monitor.unresolved_ambiguous_batches.clear();
        monitor.position = Position::default();
        monitor.reconcile_drift_streak = 0;

        monitor.update_entry_lock_state();
        assert_eq!(monitor.entry_lock_reason, None);
        assert!(!monitor.timeout_recovery_active);
    }

    #[tokio::test]
    async fn test_execution_success_counter_vs_economic_success_counter() {
        let Some(mut monitor) = mock_monitor().await else {
            return;
        };
        let (success, recorded_pnl) = monitor.finalize_opportunity_outcome(1, 1, -0.15);
        assert!(success);
        assert_eq!(recorded_pnl, -0.15);
        assert_eq!(monitor.execution_success_count, 1);
        assert_eq!(monitor.economic_success_count, 0);
    }

    #[tokio::test]
    async fn test_adaptive_learning_tightens_after_repeated_failures() {
        let Some(mut monitor) = mock_monitor().await else {
            return;
        };
        monitor.config = Arc::new(Config {
            min_net_profit_usd: 0.02,
            max_trade_size: 100.0,
            ..(*monitor.config).clone()
        });
        monitor.adaptive_min_profit_usd = 0.02;
        monitor.adaptive_max_trade_size = 100.0;

        for _ in 0..ADAPTIVE_WINDOW_SIZE {
            monitor.update_adaptive_stability_controls(false, 0.0);
        }
        for _ in 0..ADAPTIVE_HEDGE_WINDOW_SIZE {
            monitor.record_hedge_outcome(false);
        }

        assert!(monitor.adaptive_pressure > 0.2);
        assert!(monitor.adaptive_min_profit_usd > 0.02);
        assert!(monitor.adaptive_max_trade_size < 100.0);
    }

    #[tokio::test]
    async fn test_adaptive_learning_relaxes_after_winning_streak() {
        let Some(mut monitor) = mock_monitor().await else {
            return;
        };
        monitor.config = Arc::new(Config {
            min_net_profit_usd: 0.02,
            max_trade_size: 100.0,
            ..(*monitor.config).clone()
        });
        monitor.adaptive_min_profit_usd = 0.02;
        monitor.adaptive_max_trade_size = 100.0;

        for _ in 0..ADAPTIVE_WINDOW_SIZE {
            monitor.update_adaptive_stability_controls(false, 0.0);
        }
        for _ in 0..ADAPTIVE_HEDGE_WINDOW_SIZE {
            monitor.record_hedge_outcome(false);
        }
        let tightened_min_profit = monitor.adaptive_min_profit_usd;
        let tightened_max_size = monitor.adaptive_max_trade_size;

        for _ in 0..ADAPTIVE_WINDOW_SIZE {
            monitor.update_adaptive_stability_controls(true, 0.20);
        }
        for _ in 0..ADAPTIVE_HEDGE_WINDOW_SIZE {
            monitor.record_hedge_outcome(true);
        }

        assert!(monitor.adaptive_min_profit_usd < tightened_min_profit);
        assert!(monitor.adaptive_max_trade_size > tightened_max_size);
    }

    #[tokio::test]
    async fn test_daily_pnl_tracks_balance_delta() {
        let Some(mut monitor) = mock_monitor().await else {
            return;
        };
        monitor.session_start_balance = 100.0;
        monitor.last_balance = Some(112.34);
        monitor.sync_daily_pnl_from_components();

        assert!((monitor.daily_pnl - 12.34).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_hedge_failure_only_window_tightens_controls_hard() {
        let Some(mut monitor) = mock_monitor().await else {
            return;
        };
        monitor.config = Arc::new(Config {
            min_net_profit_usd: 0.02,
            max_trade_size: 100.0,
            ..(*monitor.config).clone()
        });
        monitor.adaptive_min_profit_usd = 0.02;
        monitor.adaptive_max_trade_size = 100.0;

        for _ in 0..ADAPTIVE_HEDGE_WINDOW_SIZE {
            monitor.record_hedge_outcome(false);
        }

        assert!(monitor.adaptive_pressure >= 0.75);
        assert!(monitor.adaptive_min_profit_usd >= 0.10);
        assert!(monitor.adaptive_max_trade_size <= 30.0);
    }

    #[tokio::test]
    async fn test_entry_lock_engages_on_hedge_failure_risk() {
        let Some(mut monitor) = mock_monitor().await else {
            return;
        };
        monitor.position = Position::default();
        monitor.unresolved_ambiguous_batches.clear();
        monitor.reconcile_drift_streak = 0;

        for _ in 0..HEDGE_FAILURE_LOCK_MIN_SAMPLES {
            monitor.record_hedge_outcome(false);
        }

        monitor.update_entry_lock_state();
        assert_eq!(monitor.entry_lock_reason, Some(EntryLockReason::HedgeFailureRisk));
    }
}
