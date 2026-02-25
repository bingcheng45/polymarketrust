//! Session file logger (replicates logger.ts / SessionLogger).
//!
//! Writes a human-readable session log to `logs/session_<timestamp>.txt`.
//! Uses an async queue so log writes never block the hot trading path.

use anyhow::Result;
use chrono::Utc;
use std::path::PathBuf;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tracing::warn;

#[derive(Debug)]
enum LogMsg {
    Line(String),
    Flush,
    Close,
}

pub struct SessionLogger {
    sender: mpsc::Sender<LogMsg>,
    path: PathBuf,
}

impl SessionLogger {
    pub async fn new(
        private_key_hint: &str,
        signature_type: u8,
        market_slug: &str,
        start_balance: f64,
        locked_value: f64,
        mock: bool,
    ) -> Result<Self> {
        let now = Utc::now();
        let filename = format!("logs/session_{}.txt", now.format("%Y-%m-%d_%H-%M-%S"));
        let path = PathBuf::from(&filename);

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
                    warn!("SessionLogger: cannot open {}: {e}", path_clone.display());
                    return;
                }
            };

            while let Some(msg) = rx.recv().await {
                match msg {
                    LogMsg::Line(line) => {
                        let _ = file.write_all(line.as_bytes()).await;
                        let _ = file.write_all(b"\n").await;
                    }
                    LogMsg::Flush => {
                        let _ = file.flush().await;
                    }
                    LogMsg::Close => {
                        let _ = file.flush().await;
                        break;
                    }
                }
            }
        });

        let logger = Self { sender, path };

        // Write session header
        let key_hint = if private_key_hint.len() >= 8 {
            format!("...{}", &private_key_hint[private_key_hint.len() - 8..])
        } else {
            "****".to_string()
        };
        let sig_name = match signature_type {
            0 => "EOA",
            1 => "Poly Proxy",
            2 => "Gnosis Safe",
            _ => "Unknown",
        };

        logger.write_raw("═══════════════════════════════════════════════════════════").await;
        logger.write_raw(&format!("  POLYMARKETRUST SESSION  {}", now.format("%Y-%m-%d %H:%M:%S UTC"))).await;
        logger.write_raw("═══════════════════════════════════════════════════════════").await;
        logger.write_raw(&format!("  Key         : {key_hint}")).await;
        logger.write_raw(&format!("  Sig type    : {sig_name}")).await;
        logger.write_raw(&format!("  Market      : {market_slug}")).await;
        logger.write_raw(&format!("  Balance     : ${start_balance:.4} USDC")).await;
        logger.write_raw(&format!("  Locked      : ${locked_value:.4} USDC")).await;
        logger.write_raw(&format!("  Mock mode   : {mock}")).await;
        logger.write_raw("───────────────────────────────────────────────────────────").await;

        Ok(logger)
    }

    /// Log an action with a timestamp prefix.
    pub async fn log(&self, msg: &str) {
        let ts = Utc::now().format("%H:%M:%S%.3f");
        self.write_raw(&format!("[{ts}] {msg}")).await;
    }

    /// Log an error.
    pub async fn error(&self, msg: &str) {
        let ts = Utc::now().format("%H:%M:%S%.3f");
        self.write_raw(&format!("[{ts}] ERROR: {msg}")).await;
    }

    pub async fn write_raw(&self, line: &str) {
        let _ = self.sender.send(LogMsg::Line(line.to_string())).await;
    }

    /// Flush the error summary and close the log file.
    pub async fn close(&self, errors: &[String]) {
        self.write_raw("───────────────────────────────────────────────────────────").await;
        if errors.is_empty() {
            self.write_raw("  No errors this session.").await;
        } else {
            self.write_raw(&format!("  Error summary ({} errors):", errors.len())).await;
            for e in errors {
                self.write_raw(&format!("    - {e}")).await;
            }
        }
        self.write_raw("═══════════════════════════════════════════════════════════").await;
        let _ = self.sender.send(LogMsg::Close).await;
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}
