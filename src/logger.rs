//! Session file logger (replicates logger.ts / SessionLogger).
//!
//! Writes a human-readable session log to `logs/session_<timestamp>.txt`.
//! Uses an async queue so log writes never block the hot trading path.

use anyhow::Result;
use chrono::{Local, Utc};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tracing::warn;

#[derive(Debug)]
enum LogMsg {
    Line(String),
    Close,
}

pub struct SessionLogger {
    sender: mpsc::Sender<LogMsg>,
    path: PathBuf,
    join_handle: Option<tokio::task::JoinHandle<()>>,
    dropped_lines: AtomicU64,
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

        let join_handle = tokio::spawn(async move {
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
                    LogMsg::Close => {
                        let _ = file.flush().await;
                        break;
                    }
                }
            }
        });

        let logger = Self {
            sender,
            path,
            join_handle: Some(join_handle),
            dropped_lines: AtomicU64::new(0),
        };

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
        self.try_log(msg);
    }

    /// Log an error.
    pub async fn error(&self, msg: &str) {
        self.try_error(msg);
    }

    pub async fn write_raw(&self, line: &str) {
        self.try_write_raw(line);
    }

    pub fn try_log(&self, msg: &str) {
        let ts = Local::now().format("%I:%M:%S %p");
        self.try_write_raw(&format!("[{ts}] {msg}"));
    }

    pub fn try_error(&self, msg: &str) {
        let ts = Local::now().format("%I:%M:%S %p");
        self.try_write_raw(&format!("[{ts}] ERROR: {msg}"));
    }

    pub fn try_write_raw(&self, line: &str) {
        match self.sender.try_send(LogMsg::Line(line.to_string())) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.dropped_lines.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Closed(_)) => {}
        }
    }

    pub fn dropped_count(&self) -> u64 {
        self.dropped_lines.load(Ordering::Relaxed)
    }

    pub fn queue_depth(&self) -> usize {
        self.sender
            .max_capacity()
            .saturating_sub(self.sender.capacity())
    }

    /// Flush the error summary and close the log file.
    pub async fn close(&mut self, errors: &[String]) {
        let now = chrono::Local::now().format("%d/%m/%Y, %l:%M:%S %p").to_string().to_lowercase();
        
        self.write_raw("===============================================================").await;
        if errors.is_empty() {
            self.write_raw("✅ ERRORS ENCOUNTERED: None").await;
            self.write_raw("===============================================================").await;
            self.write_raw(&format!("⏹ Session End: {}", now)).await;
            self.write_raw("===============================================================").await;
        } else {
            self.write_raw(&format!("🔴 ERRORS ENCOUNTERED ({} total)", errors.len())).await;
            self.write_raw("===============================================================").await;
            for e in errors {
                self.write_raw(e).await;
            }
            self.write_raw("===============================================================").await;
            self.write_raw(&format!("⏹ Session End: {}", now)).await;
            self.write_raw("===============================================================").await;
        }
        
        let _ = self.sender.send(LogMsg::Close).await;
        
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.await;
        }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}
