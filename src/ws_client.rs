//! Real-time WebSocket orderbook client (replicates ws_client.ts).
//!
//! Connects to `wss://ws-subscriptions-clob.polymarket.com/ws/market`,
//! subscribes to token IDs, and maintains a local cache of the latest
//! full orderbook snapshot for each token. Supports incremental price
//! change updates.

use crate::types::{OrderBook, PriceLevel, WsBookEvent, WsPriceChangeEvent};
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const STALE_THRESHOLD_MS: u64 = 5_000;
const RECONNECT_BASE_MS: u64 = 1_000;
const RECONNECT_MAX_MS: u64 = 30_000;

struct BookEntry {
    book: OrderBook,
    updated_at: Instant,
}

pub struct WsClient {
    books: Arc<RwLock<HashMap<String, BookEntry>>>,
    update_tx: mpsc::Sender<(String, OrderBook)>,
    subscribed: Arc<RwLock<Vec<String>>>,
}

impl WsClient {
    /// Create and connect the WebSocket client.
    /// `update_tx` receives `(token_id, book)` on every update.
    pub async fn connect(token_ids: Vec<String>) -> Result<(Self, mpsc::Receiver<(String, OrderBook)>)> {
        let books: Arc<RwLock<HashMap<String, BookEntry>>> = Arc::new(RwLock::new(HashMap::new()));
        let (update_tx, update_rx) = mpsc::channel::<(String, OrderBook)>(256);
        let subscribed = Arc::new(RwLock::new(token_ids.clone()));

        // Spawn background WS loop
        let books_clone = Arc::clone(&books);
        let tx_clone = update_tx.clone();
        let sub_clone = Arc::clone(&subscribed);

        tokio::spawn(async move {
            ws_loop(books_clone, tx_clone, sub_clone).await;
        });

        Ok((
            Self {
                books,
                update_tx,
                subscribed,
            },
            update_rx,
        ))
    }

    /// Returns the latest orderbook for a token if it is fresh (< 5s old).
    pub async fn get_order_book(&self, token_id: &str) -> Option<OrderBook> {
        let books = self.books.read().await;
        books.get(token_id).and_then(|entry| {
            if entry.updated_at.elapsed().as_millis() < STALE_THRESHOLD_MS as u128 {
                Some(entry.book.clone())
            } else {
                None
            }
        })
    }

    /// Returns the age of the cached book in milliseconds, or None if not present.
    pub async fn get_book_age_ms(&self, token_id: &str) -> Option<u64> {
        let books = self.books.read().await;
        books
            .get(token_id)
            .map(|e| e.updated_at.elapsed().as_millis() as u64)
    }

    /// Update the subscription list (e.g., on market rollover).
    pub async fn resubscribe(&self, token_ids: Vec<String>) {
        let mut sub = self.subscribed.write().await;
        *sub = token_ids;
    }

    /// Check whether the WS connection is healthy (books updated recently).
    pub async fn is_healthy(&self, token_id: &str) -> bool {
        self.get_book_age_ms(token_id)
            .await
            .map(|age| age < 60_000)
            .unwrap_or(false)
    }
}

// ─── Background WS Loop ───────────────────────────────────────────────────────

async fn ws_loop(
    books: Arc<RwLock<HashMap<String, BookEntry>>>,
    tx: mpsc::Sender<(String, OrderBook)>,
    subscribed: Arc<RwLock<Vec<String>>>,
) {
    let mut backoff_ms = RECONNECT_BASE_MS;

    loop {
        info!("WS: connecting to {WS_URL}");

        match timeout(Duration::from_secs(10), connect_async(WS_URL)).await {
            Err(_) => {
                warn!("WS: connection timed out, retrying in {backoff_ms}ms");
            }
            Ok(Err(e)) => {
                warn!("WS: connection failed: {e}, retrying in {backoff_ms}ms");
            }
            Ok(Ok((ws_stream, _))) => {
                info!("WS: connected");
                backoff_ms = RECONNECT_BASE_MS; // Reset on success

                let (mut write, mut read) = ws_stream.split();

                // Subscribe to current token list
                let token_ids = subscribed.read().await.clone();
                let sub_msg = json!({
                    "assets_ids": token_ids,
                    "type": "market"
                });
                if let Err(e) = write.send(Message::Text(sub_msg.to_string())).await {
                    warn!("WS: subscribe send failed: {e}");
                    continue;
                }
                info!("WS: subscribed to {} tokens", token_ids.len());

                // Read loop
                loop {
                    match timeout(Duration::from_secs(60), read.next()).await {
                        Err(_) => {
                            warn!("WS: read timeout (60s), reconnecting");
                            break;
                        }
                        Ok(None) => {
                            warn!("WS: stream ended, reconnecting");
                            break;
                        }
                        Ok(Some(Err(e))) => {
                            warn!("WS: read error: {e}, reconnecting");
                            break;
                        }
                        Ok(Some(Ok(msg))) => {
                            match msg {
                                Message::Text(text) => {
                                    handle_message(&text, &books, &tx).await;
                                }
                                Message::Ping(data) => {
                                    let _ = write.send(Message::Pong(data)).await;
                                }
                                Message::Close(_) => {
                                    info!("WS: server closed connection, reconnecting");
                                    break;
                                }
                                _ => {}
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

async fn handle_message(
    text: &str,
    books: &Arc<RwLock<HashMap<String, BookEntry>>>,
    tx: &mpsc::Sender<(String, OrderBook)>,
) {
    // Messages can be arrays
    let value: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            debug!("WS: parse error: {e}");
            return;
        }
    };

    let events = match value {
        Value::Array(arr) => arr,
        single => vec![single],
    };

    for event in events {
        let event_type = event.get("event_type").and_then(|v| v.as_str()).unwrap_or("");

        match event_type {
            "book" => {
                if let Ok(ev) = serde_json::from_value::<WsBookEvent>(event) {
                    let mut book = OrderBook {
                        bids: ev.bids,
                        asks: ev.asks,
                        timestamp: ev
                            .timestamp
                            .and_then(|t| t.parse::<u64>().ok()),
                    };
                    book.sort();

                    let token_id = ev.asset_id.clone();
                    {
                        let mut books_w = books.write().await;
                        books_w.insert(
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
    let entry = books_w.entry(ev.asset_id.clone()).or_insert_with(|| BookEntry {
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
        // Remove this price level
        levels.retain(|l| l.price != ev.price);
    } else {
        // Update or insert
        if let Some(level) = levels.iter_mut().find(|l| l.price == ev.price) {
            level.size = ev.size.clone();
        } else {
            levels.push(PriceLevel {
                price: ev.price.clone(),
                size: ev.size.clone(),
            });
        }
    }

    entry.book.sort();
    entry.updated_at = Instant::now();

    let book = entry.book.clone();
    let token_id = ev.asset_id.clone();

    drop(books_w); // Release lock before sending
    let _ = tx.try_send((token_id, book));
}
