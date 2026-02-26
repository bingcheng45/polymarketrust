//! JSONL trade event logger (replicates trade_logger.ts).
//!
//! Each trade is appended as a single JSON line to `logs/trades.jsonl`.
//! Uses an async queue so log writes never block the hot trading path.

use crate::types::{TradeLogEntry, TradeType};
use anyhow::Result;
use chrono::Utc;
use std::path::PathBuf;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tracing::warn;

#[derive(Debug)]
enum LogMsg {
    Entry(TradeLogEntry),
}

pub struct TradeLogger {
    sender: mpsc::Sender<LogMsg>,
    #[allow(dead_code)]
    path: PathBuf,
}

impl TradeLogger {
    pub async fn new() -> Result<Self> {
        let path = PathBuf::from("logs/trades.jsonl");
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let (sender, mut rx) = mpsc::channel::<LogMsg>(1024);
        let path_clone = path.clone();

        tokio::spawn(async move {
            let mut file = match OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path_clone)
                .await
            {
                Ok(f) => f,
                Err(e) => {
                    warn!("TradeLogger: cannot open {}: {e}", path_clone.display());
                    return;
                }
            };

            while let Some(msg) = rx.recv().await {
                match msg {
                    LogMsg::Entry(entry) => {
                        if let Ok(line) = serde_json::to_string(&entry) {
                            let _ = file.write_all(line.as_bytes()).await;
                            let _ = file.write_all(b"\n").await;
                        }
                    }
                }
            }
            
            let _ = file.flush().await;
        });

        Ok(Self { sender, path })
    }

    pub async fn log(&self, entry: TradeLogEntry) {
        if let Err(e) = self.sender.send(LogMsg::Entry(entry)).await {
            warn!("TradeLogger cache full or receiver dropped: {e}");
        }
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
