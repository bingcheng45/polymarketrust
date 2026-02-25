//! Real-time WebSocket orderbook client (replicates ws_client.ts).
//!
//! Connects to `wss://ws-subscriptions-clob.polymarket.com/ws/market`,
//! subscribes to token IDs, and maintains a local cache of the latest
//! full orderbook snapshot for each token.  Supports incremental
//! price-change updates and sends a PING heartbeat every 10 seconds
//! (matching TypeScript's HEARTBEAT_MS = 10_000) — the connection is
//! considered dead and reconnected if no PONG is received within 15 s.

use crate::types::{OrderBook, PriceLevel, WsBookEvent, WsPriceChangeEvent};
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::time::{interval, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
/// Books older than this are considered stale (WS-primary fetch falls back to REST).
const STALE_THRESHOLD_MS: u64 = 5_000;
/// Initial reconnect wait.
const RECONNECT_BASE_MS: u64 = 1_000;
/// Maximum reconnect wait.
const RECONNECT_MAX_MS: u64 = 30_000;
/// How often we send a PING frame (matches TypeScript HEARTBEAT_MS).
const PING_INTERVAL_SECS: u64 = 10;
/// If no message arrives within this many seconds of a PING we reconnect.
const PONG_TIMEOUT_SECS: u64 = 15;

struct BookEntry {
    book: OrderBook,
    updated_at: Instant,
}

pub struct WsClient {
    books: Arc<RwLock<HashMap<String, BookEntry>>>,
    subscribed: Arc<RwLock<Vec<String>>>,
}

impl WsClient {
    /// Create the client and immediately start the background WS loop.
    /// Returns the client plus an `mpsc::Receiver` that fires on every book update.
    pub async fn connect(
        token_ids: Vec<String>,
    ) -> Result<(Self, mpsc::Receiver<(String, OrderBook)>)> {
        let books: Arc<RwLock<HashMap<String, BookEntry>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let (update_tx, update_rx) = mpsc::channel::<(String, OrderBook)>(512);
        let subscribed = Arc::new(RwLock::new(token_ids));

        let books_clone = Arc::clone(&books);
        let tx_clone = update_tx.clone();
        let sub_clone = Arc::clone(&subscribed);

        tokio::spawn(async move {
            ws_loop(books_clone, tx_clone, sub_clone).await;
        });

        Ok((Self { books, subscribed }, update_rx))
    }

    /// Returns the cached book if it is fresh (< 5 s old).
    pub async fn get_order_book(&self, token_id: &str) -> Option<OrderBook> {
        let books = self.books.read().await;
        books.get(token_id).and_then(|e| {
            if e.updated_at.elapsed().as_millis() < STALE_THRESHOLD_MS as u128 {
                Some(e.book.clone())
            } else {
                None
            }
        })
    }

    /// Age of the cached book in milliseconds, or `None` if not present.
    pub async fn get_book_age_ms(&self, token_id: &str) -> Option<u64> {
        let books = self.books.read().await;
        books
            .get(token_id)
            .map(|e| e.updated_at.elapsed().as_millis() as u64)
    }

    /// Update the subscription list (called on market rollover).
    pub async fn resubscribe(&self, token_ids: Vec<String>) {
        let mut sub = self.subscribed.write().await;
        *sub = token_ids;
        // The running WS loop will pick up the new list on next reconnect.
        // For an immediate re-sub we rely on the health check triggering a reconnect.
    }

    /// True if the book for `token_id` was updated within the last 60 s.
    pub async fn is_healthy(&self, token_id: &str) -> bool {
        self.get_book_age_ms(token_id)
            .await
            .map(|age| age < 60_000)
            .unwrap_or(false)
    }
}

// ─── Background WS loop ──────────────────────────────────────────────────────

async fn ws_loop(
    books: Arc<RwLock<HashMap<String, BookEntry>>>,
    tx: mpsc::Sender<(String, OrderBook)>,
    subscribed: Arc<RwLock<Vec<String>>>,
) {
    let mut backoff_ms = RECONNECT_BASE_MS;

    loop {
        info!("WS: connecting to {WS_URL}");

        let conn = timeout(Duration::from_secs(10), connect_async(WS_URL)).await;

        match conn {
            Err(_) => warn!("WS: connect timed out, retry in {backoff_ms}ms"),
            Ok(Err(e)) => warn!("WS: connect error: {e}, retry in {backoff_ms}ms"),
            Ok(Ok((ws_stream, _))) => {
                info!("WS: connected");
                backoff_ms = RECONNECT_BASE_MS; // Reset on success

                let (mut write, mut read) = ws_stream.split();

                // Send subscription message
                let token_ids = subscribed.read().await.clone();
                let sub_msg = json!({ "assets_ids": token_ids, "type": "market" });
                if let Err(e) = write.send(Message::Text(sub_msg.to_string())).await {
                    warn!("WS: subscribe failed: {e}");
                    // fall through to reconnect
                } else {
                    info!("WS: subscribed to {} tokens", token_ids.len());

                    // Ping interval + pong deadline tracking
                    let mut ping_ticker = interval(Duration::from_secs(PING_INTERVAL_SECS));
                    ping_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    let mut last_pong = Instant::now();
                    let mut ping_sent = false;

                    'read: loop {
                        tokio::select! {
                            // ── Incoming message ──────────────────────────
                            msg = read.next() => {
                                match msg {
                                    None => {
                                        warn!("WS: stream ended");
                                        break 'read;
                                    }
                                    Some(Err(e)) => {
                                        warn!("WS: read error: {e}");
                                        break 'read;
                                    }
                                    Some(Ok(m)) => {
                                        match m {
                                            Message::Text(text) => {
                                                last_pong = Instant::now(); // Any message resets deadline
                                                ping_sent = false;
                                                handle_message(&text, &books, &tx).await;
                                            }
                                            Message::Pong(_) => {
                                                last_pong = Instant::now();
                                                ping_sent = false;
                                                debug!("WS: pong received");
                                            }
                                            Message::Ping(data) => {
                                                // Server-initiated ping — respond immediately
                                                let _ = write.send(Message::Pong(data)).await;
                                            }
                                            Message::Close(_) => {
                                                info!("WS: server closed");
                                                break 'read;
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }

                            // ── Heartbeat tick ────────────────────────────
                            _ = ping_ticker.tick() => {
                                // Check pong deadline first
                                if ping_sent && last_pong.elapsed().as_secs() > PONG_TIMEOUT_SECS {
                                    warn!("WS: pong timeout ({PONG_TIMEOUT_SECS}s), reconnecting");
                                    break 'read;
                                }

                                // Send ping
                                if write.send(Message::Ping(vec![])).await.is_err() {
                                    warn!("WS: ping send failed, reconnecting");
                                    break 'read;
                                }
                                ping_sent = true;
                                debug!("WS: ping sent");
                            }
                        }
                    }
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
    }
}

// ─── Message handlers ─────────────────────────────────────────────────────────

async fn handle_message(
    text: &str,
    books: &Arc<RwLock<HashMap<String, BookEntry>>>,
    tx: &mpsc::Sender<(String, OrderBook)>,
) {
    let value: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            debug!("WS: JSON parse error: {e}");
            return;
        }
    };

    let events = match value {
        Value::Array(arr) => arr,
        single => vec![single],
    };

    for event in events {
        let event_type = event
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        match event_type {
            "book" => {
                if let Ok(ev) = serde_json::from_value::<WsBookEvent>(event) {
                    let ts = ev
                        .timestamp
                        .as_deref()
                        .and_then(|s| s.parse::<u64>().ok());
                    let mut book = OrderBook {
                        bids: ev.bids,
                        asks: ev.asks,
                        timestamp: ts,
                    };
                    book.sort();

                    let token_id = ev.asset_id.clone();
                    {
                        let mut w = books.write().await;
                        w.insert(
                            token_id.clone(),
                            BookEntry {
                                book: book.clone(),
                                updated_at: Instant::now(),
                            },
                        );
                    }
                    let _ = tx.try_send((token_id, book));
                }
            }
            "price_change" => {
                if let Ok(ev) = serde_json::from_value::<WsPriceChangeEvent>(event) {
                    apply_price_change(&ev, books, tx).await;
                }
            }
            _ => {}
        }
    }
}

/// Apply an incremental price-change delta to the cached book.
async fn apply_price_change(
    ev: &WsPriceChangeEvent,
    books: &Arc<RwLock<HashMap<String, BookEntry>>>,
    tx: &mpsc::Sender<(String, OrderBook)>,
) {
    let mut books_w = books.write().await;
    let entry = books_w
        .entry(ev.asset_id.clone())
        .or_insert_with(|| BookEntry {
            book: OrderBook::default(),
            updated_at: Instant::now(),
        });

    let size: f64 = ev.size.parse().unwrap_or(0.0);
    let side_lower = ev.side.to_lowercase();
    let levels = if side_lower == "bid" {
        &mut entry.book.bids
    } else {
        &mut entry.book.asks
    };

    if size == 0.0 {
        // Level removed — price hit zero liquidity
        levels.retain(|l| l.price != ev.price);
    } else if let Some(level) = levels.iter_mut().find(|l| l.price == ev.price) {
        // Update existing level
        level.size = ev.size.clone();
    } else {
        // New price level
        levels.push(PriceLevel {
            price: ev.price.clone(),
            size: ev.size.clone(),
        });
    }

    entry.book.sort();
    entry.updated_at = Instant::now();

    let book = entry.book.clone();
    let token_id = ev.asset_id.clone();
    drop(books_w);

    let _ = tx.try_send((token_id, book));
}
