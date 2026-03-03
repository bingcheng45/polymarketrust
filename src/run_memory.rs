//! Persistent run-level memory for iterative tuning.
//!
//! Appends one JSON line per shutdown to `logs/run_memory.jsonl` so we can
//! compare wins/losses and parameter regimes across sessions.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::path::PathBuf;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Serialize)]
pub struct RunMemoryEntry {
    pub timestamp: DateTime<Utc>,
    pub market_slug: String,
    pub strategy_mode: String,
    pub session_start_balance: f64,
    pub session_end_balance: f64,
    pub session_pnl_usd: f64,
    pub session_pnl_pct: f64,
    pub execution_pnl: f64,
    pub hedge_sellback_pnl: f64,
    pub redemption_cashin_usd: f64,
    pub fees_gas_estimate: f64,
    pub session_successes: u64,
    pub session_failures: u64,
    pub execution_success_count: u64,
    pub economic_success_count: u64,
    pub adaptive_pressure: f64,
    pub adaptive_min_profit_usd: f64,
    pub adaptive_max_trade_size: f64,
    pub adaptive_hedge_failure_ratio: Option<f64>,
    pub position_yes_size: f64,
    pub position_no_size: f64,
    pub position_yes_cost: f64,
    pub position_no_cost: f64,
    pub pending_claim_usdc: f64,
    pub entry_lock_reason: Option<String>,
    pub outcome: String,
    pub risk_flags: Vec<String>,
    pub config_snapshot: Vec<(String, String)>,
}

pub async fn append_run_memory_entry(entry: &RunMemoryEntry) -> Result<()> {
    let path = PathBuf::from("logs/run_memory.jsonl");
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await?;

    let line = serde_json::to_string(entry)?;
    file.write_all(line.as_bytes()).await?;
    file.write_all(b"\n").await?;
    file.flush().await?;
    Ok(())
}
