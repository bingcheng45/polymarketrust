//! TUI Dashboard — static terminal display (replicates TypeScript bot's `render()`).
//!
//! Clears the screen and redraws the full dashboard on each render() call.
//! Uses raw ANSI escape codes — no external crate needed.

use chrono::{DateTime, Local, Utc};
use std::collections::VecDeque;
use std::io::{self, Write};
use std::time::Instant;
use tracing::info;

const MAX_PRICE_TICKS: usize = 5;
const MAX_ACTION_LOGS: usize = 10;
const SEPARATOR_WIDTH: usize = 64;

/// Snapshot of the data needed by render().
/// Passed in from MarketMonitor each tick.
pub struct DashboardState<'a> {
    // Header
    pub market_slug: &'a str,
    pub market_question: Option<&'a str>,
    pub market_end_date: Option<DateTime<Utc>>,
    pub mock_mode: bool,

    // Account
    pub wallet_address: &'a str,
    pub wallet_type: &'a str, // "Proxy Wallet" or "EOA"
    pub balance_usdc: Option<f64>,
    pub pending_claim_usdc: Option<f64>,

    // Position
    pub yes_size: f64,
    pub no_size: f64,
    pub yes_cost: f64,
    pub no_cost: f64,
    pub outcome_yes_label: &'a str,
    pub outcome_no_label: &'a str,

    // PnL
    pub session_start_balance: f64,
    pub session_successes: u64,
    pub session_failures: u64,
    pub daily_pnl: f64,
    pub execution_pnl: f64,
    pub hedge_sellback_pnl: f64,
    pub redemption_pnl: f64,
    pub fees_gas_estimate: f64,
    pub execution_success_count: u64,
    pub economic_success_count: u64,
    pub carry_naked_yes: f64,
    pub carry_naked_no: f64,
    pub carry_worst_case_loss: f64,
    pub entry_lock_reason: Option<&'a str>,
    pub entry_lock_remaining_secs: Option<u64>,

    // Circuit breaker
    pub circuit_breaker_active: bool,
    pub circuit_breaker_remaining_secs: i64,

    // Data source
    pub data_source: &'a str, // "WS" or "REST"
    pub ws_connected: bool,
    pub ws_yes_age_ms: Option<u64>,
    pub ws_no_age_ms: Option<u64>,
}

/// The dashboard state machine.
pub struct Dashboard {
    price_buffer: VecDeque<String>,
    action_logs: VecDeque<String>,
    session_start: Instant,
    enabled: bool,
}

impl Dashboard {
    pub fn new() -> Self {
        Self {
            price_buffer: VecDeque::with_capacity(MAX_PRICE_TICKS + 1),
            action_logs: VecDeque::with_capacity(MAX_ACTION_LOGS + 1),
            session_start: Instant::now(),
            enabled: true,
        }
    }

    /// Add a price tick line to the buffer.
    pub fn add_tick(&mut self, msg: String) {
        self.price_buffer.push_back(msg);
        if self.price_buffer.len() > MAX_PRICE_TICKS {
            self.price_buffer.pop_front();
        }
    }

    /// Add an action log entry (timestamped).
    pub fn log_action(&mut self, msg: &str) {
        let time = Local::now().format("%I:%M:%S %p");
        let formatted = format!("[{time}] {msg}");
        self.action_logs.push_back(formatted);
        if self.action_logs.len() > MAX_ACTION_LOGS {
            self.action_logs.pop_front();
        }
        info!("[ACTION] {msg}");
    }

    /// Clear the price buffer (e.g. on market rollover).
    pub fn clear_prices(&mut self) {
        self.price_buffer.clear();
    }

    /// Whether the dashboard is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Render the full dashboard to stdout.
    pub fn render(&self, state: &DashboardState<'_>) {
        if !self.enabled {
            return;
        }

        let mut out = String::with_capacity(2048);

        // Move cursor to top-left + clear screen from cursor down
        out.push_str("\x1b[H\x1b[J");

        // ─── Header ──────────────────────────────────────────────────────
        out.push_str(&"=".repeat(SEPARATOR_WIDTH));
        out.push('\n');

        let mode_flag = if state.mock_mode { " [MOCK MODE]" } else { "" };
        out.push_str(&format!(
            "📊 POLYMARKET BOT DASHBOARD{} — {}\n",
            mode_flag, state.market_slug
        ));

        if let Some(q) = state.market_question {
            out.push_str(&format!("   {q}\n"));
        }

        // Uptime + countdown
        let uptime_secs = self.session_start.elapsed().as_secs();
        let up_h = uptime_secs / 3600;
        let up_m = (uptime_secs % 3600) / 60;
        let up_s = uptime_secs % 60;
        let mut uptime_str = String::new();
        if up_h > 0 {
            uptime_str.push_str(&format!("{up_h}h "));
        }
        if up_h > 0 || up_m > 0 {
            uptime_str.push_str(&format!("{up_m}m "));
        }
        uptime_str.push_str(&format!("{up_s}s"));

        if let Some(end) = state.market_end_date {
            let remaining = (end - Utc::now()).num_seconds().max(0);
            let mins = remaining / 60;
            let secs = remaining % 60;
            out.push_str(&format!(
                "   ⏱ Uptime: {} | Ends in: {}m {:02}s\n",
                uptime_str, mins, secs
            ));
        } else {
            out.push_str(&format!("   ⏱ Uptime: {uptime_str}\n"));
        }
        out.push_str(&"=".repeat(SEPARATOR_WIDTH));
        out.push('\n');

        // ─── Account ─────────────────────────────────────────────────────
        out.push_str("💰 ACCOUNT (USDC)\n");
        out.push_str(&format!(
            "   Wallet:    {} [{}]\n",
            state.wallet_address, state.wallet_type
        ));

        let balance_str = match state.balance_usdc {
            Some(b) => format!("{b:.2}"),
            None => "---".to_string(),
        };
        out.push_str(&format!("   Balance:   {balance_str}\n"));

        // Capital
        let mut capital_line = "   Capital:  ".to_string();
        if let Some(pending) = state.pending_claim_usdc {
            if pending > 0.0 {
                capital_line.push_str(&format!(" ⏳ ${:.2} pending claim", pending));
            } else {
                capital_line.push_str(" none");
            }
        } else {
            capital_line.push_str(" none");
        }
        out.push_str(&capital_line);
        out.push('\n');

        // Position
        if state.yes_size > 0.0 || state.no_size > 0.0 {
            let pairs = state.yes_size.min(state.no_size);
            let excess_yes = state.yes_size - pairs;
            let excess_no = state.no_size - pairs;
            let total_cost = state.yes_cost + state.no_cost;

            let mut pos_line = format!(
                "   Position:  {:.3} {} (${:.2}) + {:.3} {} (${:.2})",
                state.yes_size,
                state.outcome_yes_label,
                state.yes_cost,
                state.no_size,
                state.outcome_no_label,
                state.no_cost,
            );
            if pairs > 0.01 {
                pos_line.push_str(&format!(
                    " = {:.3} pairs (${:.2} cost → ${:.0} payout)",
                    pairs, total_cost, pairs
                ));
            }
            if excess_yes > 0.01 {
                pos_line.push_str(&format!(
                    " | {:.3} {} naked",
                    excess_yes, state.outcome_yes_label
                ));
            }
            if excess_no > 0.01 {
                pos_line.push_str(&format!(
                    " | {:.3} {} naked",
                    excess_no, state.outcome_no_label
                ));
            }
            out.push_str(&pos_line);
            out.push('\n');
        } else {
            out.push_str("   Position:  none\n");
        }

        // Circuit breaker
        if state.circuit_breaker_active {
            out.push_str(&format!(
                "   🛑 CIRCUIT BREAKER: Active ({}s remaining)\n",
                state.circuit_breaker_remaining_secs
            ));
        }

        // Daily PnL and reliability snapshot
        {
            let pnl = state.daily_pnl;
            let pnl_pct = if state.session_start_balance > 0.0 {
                pnl / state.session_start_balance * 100.0
            } else {
                0.0
            };
            let (color, sign) = if pnl >= 0.0 {
                ("\x1b[32m", "+")
            } else {
                ("\x1b[31m", "")
            };
            let reset = "\x1b[0m";
            out.push_str(&format!(
                "   Daily PnL (balance delta): {color}{sign}${pnl:.3} ({sign}{pnl_pct:.1}%){reset} | successes: {} | failures: {}\n",
                state.session_successes, state.session_failures
            ));
            out.push_str(&format!(
                "   Engine estimates: exec ${:+.3} | hedge/sell-back ${:+.3} | redemption ${:+.3} | fees+gas ${:+.3}\n",
                state.execution_pnl,
                state.hedge_sellback_pnl,
                state.redemption_pnl,
                state.fees_gas_estimate
            ));
            out.push_str(&format!(
                "   Success split: execution {} | economic {}\n",
                state.execution_success_count, state.economic_success_count
            ));
            out.push_str(&format!(
                "   Carry Risk: naked Up {:.3} | naked Down {:.3} | worst-case directional loss ${:.2}\n",
                state.carry_naked_yes, state.carry_naked_no, state.carry_worst_case_loss
            ));
            if let Some(reason) = state.entry_lock_reason {
                if let Some(remaining) = state.entry_lock_remaining_secs {
                    out.push_str(&format!(
                        "   Entry Lock: {} ({}s remaining)\n",
                        reason, remaining
                    ));
                } else {
                    out.push_str(&format!("   Entry Lock: {}\n", reason));
                }
            }
        }

        out.push_str(&"-".repeat(SEPARATOR_WIDTH));
        out.push('\n');

        // ─── Price Feed ──────────────────────────────────────────────────
        out.push_str(&format!(
            "📈 LIVE MARKET DATA ({}) (Last {} ticks)\n",
            state.data_source, MAX_PRICE_TICKS
        ));
        out.push_str(&format!(
            "   WS status: {}\n",
            if state.ws_connected {
                "connected"
            } else {
                "disconnected"
            }
        ));
        if let (Some(yes_age), Some(no_age)) = (state.ws_yes_age_ms, state.ws_no_age_ms) {
            out.push_str(&format!(
                "   WS age: Up={}ms | Down={}ms\n",
                yes_age, no_age
            ));
        }
        if self.price_buffer.is_empty() {
            out.push_str("   (Waiting for ticks...)\n");
        } else {
            for line in &self.price_buffer {
                out.push_str("   ");
                out.push_str(line);
                out.push('\n');
            }
        }

        out.push_str(&"-".repeat(SEPARATOR_WIDTH));
        out.push('\n');

        // ─── Action Log ──────────────────────────────────────────────────
        out.push_str("⚡️ RECENT ACTIONS\n");
        if self.action_logs.is_empty() {
            out.push_str("   (No actions yet)\n");
        } else {
            for line in &self.action_logs {
                out.push_str("   ");
                out.push_str(line);
                out.push('\n');
            }
        }
        out.push_str(&"=".repeat(SEPARATOR_WIDTH));
        out.push('\n');

        // Flush once
        let stdout = io::stdout();
        let mut handle = stdout.lock();
        let _ = handle.write_all(out.as_bytes());
        let _ = handle.flush();
    }
}
