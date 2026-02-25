//! Persistent market statistics (replicates market_stats.ts).
//!
//! Flushed to `logs/market_stats.json` on market rollover or graceful shutdown.

use crate::types::MarketStats;
use anyhow::Result;
use chrono::Utc;
use std::path::PathBuf;
use tokio::fs;
use tracing::{info, warn};

pub struct MarketStatsTracker {
    path: PathBuf,
    pub stats: MarketStats,
    arb_spreads: Vec<f64>,
    session_start: std::time::Instant,
}

impl MarketStatsTracker {
    pub async fn load() -> Result<Self> {
        let path = PathBuf::from("logs/market_stats.json");
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let stats = if path.exists() {
            match fs::read_to_string(&path).await {
                Ok(contents) => serde_json::from_str(&contents).unwrap_or_default(),
                Err(_) => MarketStats::default(),
            }
        } else {
            MarketStats::default()
        };

        Ok(Self {
            path,
            stats,
            arb_spreads: Vec::new(),
            session_start: std::time::Instant::now(),
        })
    }

    /// Record a detected arb opportunity (whether executed or not).
    pub fn record_opportunity(&mut self, spread: f64) {
        self.stats.total_arb_opportunities += 1;
        if spread > self.stats.best_spread {
            self.stats.best_spread = spread;
        }
        self.arb_spreads.push(spread);
        let n = self.arb_spreads.len() as f64;
        self.stats.avg_arb_spread = self.arb_spreads.iter().sum::<f64>() / n;
    }

    /// Record an executed arb trade.
    pub fn record_execution(&mut self, success: bool, pnl: f64) {
        self.stats.total_arb_executions += 1;
        if success {
            self.stats.successes += 1;
        } else {
            self.stats.failures += 1;
        }
        self.stats.total_pnl_usd += pnl;
    }

    pub fn record_hedge(&mut self) {
        self.stats.hedged += 1;
    }

    pub fn record_sellback(&mut self) {
        self.stats.sold_back += 1;
    }

    /// Flush stats to disk (called on market rollover or graceful shutdown).
    pub async fn flush(&mut self) {
        let elapsed_mins = self.session_start.elapsed().as_secs_f64() / 60.0;
        self.stats.total_minutes += elapsed_mins;
        self.stats.last_session = Utc::now().to_rfc3339();

        match serde_json::to_string_pretty(&self.stats) {
            Ok(json) => {
                if let Err(e) = fs::write(&self.path, json).await {
                    warn!("Failed to write market_stats.json: {e}");
                } else {
                    info!(
                        "Stats flushed — arbs: {}, successes: {}, PnL: ${:.4}",
                        self.stats.total_arb_executions,
                        self.stats.successes,
                        self.stats.total_pnl_usd
                    );
                }
            }
            Err(e) => warn!("Failed to serialize market stats: {e}"),
        }

        // Reset session timer after flush
        self.session_start = std::time::Instant::now();
    }
}
