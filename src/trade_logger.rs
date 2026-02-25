//! JSONL trade event logger (replicates trade_logger.ts).
//!
//! Each trade is appended as a single JSON line to `logs/trades.jsonl`.

use crate::types::{TradeLogEntry, TradeType};
use anyhow::Result;
use chrono::Utc;
use std::path::PathBuf;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tracing::warn;

pub struct TradeLogger {
    path: PathBuf,
}

impl TradeLogger {
    pub async fn new() -> Result<Self> {
        let path = PathBuf::from("logs/trades.jsonl");
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        Ok(Self { path })
    }

    pub async fn log(&self, entry: TradeLogEntry) {
        if let Err(e) = self.write_entry(&entry).await {
            warn!("TradeLogger write error: {e}");
        }
    }

    async fn write_entry(&self, entry: &TradeLogEntry) -> Result<()> {
        let line = serde_json::to_string(entry)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;
        file.write_all(line.as_bytes()).await?;
        file.write_all(b"\n").await?;
        Ok(())
    }

    pub async fn log_execution(
        &self,
        condition_id: &str,
        market: Option<&str>,
        yes_price: f64,
        no_price: f64,
        size: f64,
        cost_usd: f64,
        gas_fee_usd: f64,
        profit_usd: f64,
    ) {
        self.log(TradeLogEntry {
            timestamp: Utc::now(),
            trade_type: TradeType::Execution,
            condition_id: condition_id.to_string(),
            market: market.map(str::to_string),
            yes_price: Some(yes_price),
            no_price: Some(no_price),
            size: Some(size),
            cost_usd: Some(cost_usd),
            gas_fee_usd: Some(gas_fee_usd),
            profit_usd: Some(profit_usd),
            success: true,
            error: None,
        })
        .await;
    }

    pub async fn log_hedge(
        &self,
        condition_id: &str,
        market: Option<&str>,
        size: f64,
        cost_usd: f64,
        profit_usd: f64,
        sold_back: bool,
        success: bool,
        error: Option<String>,
    ) {
        self.log(TradeLogEntry {
            timestamp: Utc::now(),
            trade_type: if sold_back {
                TradeType::Sellback
            } else {
                TradeType::Hedge
            },
            condition_id: condition_id.to_string(),
            market: market.map(str::to_string),
            yes_price: None,
            no_price: None,
            size: Some(size),
            cost_usd: Some(cost_usd),
            gas_fee_usd: None,
            profit_usd: Some(profit_usd),
            success,
            error,
        })
        .await;
    }

    pub async fn log_merge(
        &self,
        condition_id: &str,
        market: Option<&str>,
        size: f64,
        success: bool,
        error: Option<String>,
    ) {
        self.log(TradeLogEntry {
            timestamp: Utc::now(),
            trade_type: TradeType::Merge,
            condition_id: condition_id.to_string(),
            market: market.map(str::to_string),
            yes_price: None,
            no_price: None,
            size: Some(size),
            cost_usd: None,
            gas_fee_usd: None,
            profit_usd: None,
            success,
            error,
        })
        .await;
    }

    pub async fn log_redemption(
        &self,
        condition_id: &str,
        market: Option<&str>,
        size: f64,
        profit_usd: f64,
        success: bool,
        error: Option<String>,
    ) {
        self.log(TradeLogEntry {
            timestamp: Utc::now(),
            trade_type: TradeType::Redemption,
            condition_id: condition_id.to_string(),
            market: market.map(str::to_string),
            yes_price: None,
            no_price: None,
            size: Some(size),
            cost_usd: None,
            gas_fee_usd: None,
            profit_usd: Some(profit_usd),
            success,
            error,
        })
        .await;
    }

    pub async fn log_maker_fill(
        &self,
        condition_id: &str,
        market: Option<&str>,
        size: f64,
        cost_usd: f64,
        success: bool,
        error: Option<String>,
    ) {
        self.log(TradeLogEntry {
            timestamp: Utc::now(),
            trade_type: TradeType::MakerFill,
            condition_id: condition_id.to_string(),
            market: market.map(str::to_string),
            yes_price: None,
            no_price: None,
            size: Some(size),
            cost_usd: Some(cost_usd),
            gas_fee_usd: None,
            profit_usd: None,
            success,
            error,
        })
        .await;
    }
}
