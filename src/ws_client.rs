//! Real-time WebSocket orderbook client using `polymarket-client-sdk`.
//!
//! Maintains a local cache of the latest full orderbook snapshot for each token.
//! Uses the official SDK's built-in background connection manager, removing the 
//! need for manual ping/pong and reconnect logic.

use crate::types::{OrderBook, PriceLevel};
use anyhow::Result;
use futures_util::StreamExt;
use polymarket_client_sdk::auth::Credentials;
use polymarket_client_sdk::clob::ws::Client;
use polymarket_client_sdk::clob::ws::types::response::{BookUpdate, PriceChange, WsMessage};
use polymarket_client_sdk::error::Error as SdkError;
use polymarket_client_sdk::types::{Address, U256};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, Notify, RwLock};
use tracing::{info, warn};

/// Books older than this are considered stale (WS-primary fetch falls back to REST).
const STALE_THRESHOLD_MS: u64 = 5_000;

struct BookEntry {
    book: OrderBook,
    updated_at: Instant,
}

pub struct WsClient {
    books: Arc<RwLock<HashMap<String, BookEntry>>>,
    is_market_connected: Arc<AtomicBool>,
    is_user_connected: Arc<AtomicBool>,
    cmd_tx: mpsc::UnboundedSender<Vec<String>>,
    pub error_rx: mpsc::UnboundedReceiver<String>,
    pub user_events_rx: mpsc::UnboundedReceiver<Value>,
}

impl WsClient {
    /// Create the client and immediately start the background WS loops.
    /// Returns the client plus a `Notify` that fires on every book update.
    pub async fn connect(
        token_ids: Vec<String>,
        poly_api_key: String,
        poly_api_secret: String,
        poly_api_passphrase: String,
    ) -> Result<(Self, Arc<Notify>)> {
        let books: Arc<RwLock<HashMap<String, BookEntry>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let notify = Arc::new(Notify::new());
        let is_market_connected = Arc::new(AtomicBool::new(false));
        let is_user_connected = Arc::new(AtomicBool::new(false));
        
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (err_tx, err_rx) = mpsc::unbounded_channel();
        let (user_events_tx, user_events_rx) = mpsc::unbounded_channel();

        let books_clone = Arc::clone(&books);
        let notify_clone = notify.clone();
        let is_market_connected_clone = Arc::clone(&is_market_connected);
        
        let _ = cmd_tx.send(token_ids);

        let err_tx_market = err_tx.clone();
        tokio::spawn(async move {
            ws_market_loop(books_clone, notify_clone, cmd_rx, err_tx_market, is_market_connected_clone).await;
        });

        let is_user_connected_clone = Arc::clone(&is_user_connected);
        tokio::spawn(async move {
            ws_user_loop(poly_api_key, poly_api_secret, poly_api_passphrase, user_events_tx, err_tx, is_user_connected_clone).await;
        });

        Ok((Self { books, is_market_connected, is_user_connected, cmd_tx, error_rx: err_rx, user_events_rx }, notify))
    }

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

    pub async fn get_book_age_ms(&self, token_id: &str) -> Option<u64> {
        let books = self.books.read().await;
        books
            .get(token_id)
            .map(|e| e.updated_at.elapsed().as_millis() as u64)
    }

    pub async fn resubscribe(&self, token_ids: Vec<String>) {
        let _ = self.cmd_tx.send(token_ids);
    }

    pub async fn is_healthy(&self, _token_id: &str) -> bool {
        self.is_market_connected.load(Ordering::Relaxed) && self.is_user_connected.load(Ordering::Relaxed)
    }
}

async fn ws_market_loop(
    books: Arc<RwLock<HashMap<String, BookEntry>>>,
    notify: Arc<Notify>,
    mut cmd_rx: mpsc::UnboundedReceiver<Vec<String>>,
    err_tx: mpsc::UnboundedSender<String>,
    is_connected: Arc<AtomicBool>,
) {
    let client = Client::default();
    is_connected.store(true, Ordering::Relaxed); // Connection managed internally by SDK

    let mut active_tokens: Vec<U256> = Vec::new();
    let mut orderbook_stream: Option<Box<dyn futures_util::Stream<Item = std::result::Result<BookUpdate, SdkError>> + Unpin + Send>> = None;
    let mut prices_stream: Option<Box<dyn futures_util::Stream<Item = std::result::Result<PriceChange, SdkError>> + Unpin + Send>> = None;

    loop {
        tokio::select! {
            cmd_opt = cmd_rx.recv() => {
                match cmd_opt {
                    Some(tokens_str) => {
                        let tokens: Vec<U256> = tokens_str.iter()
                            .filter_map(|t| U256::from_str(t).ok())
                            .collect();
                        
                        if !active_tokens.is_empty() {
                            let _ = client.unsubscribe_orderbook(&active_tokens);
                            let _ = client.unsubscribe_prices(&active_tokens);
                        }
                        
                        active_tokens = tokens;
                        
                        if !active_tokens.is_empty() {
                            match client.subscribe_orderbook(active_tokens.clone()) {
                                Ok(stream) => { orderbook_stream = Some(Box::new(Box::pin(stream))); }
                                Err(e) => { let _ = err_tx.send(format!("WS SDK sub error: {}", e)); }
                            }
                            match client.subscribe_prices(active_tokens.clone()) {
                                Ok(stream) => { prices_stream = Some(Box::new(Box::pin(stream))); }
                                Err(e) => { let _ = err_tx.send(format!("WS SDK sub error: {}", e)); }
                            }
                            info!("WS SDK: seamlessly subscribed to {} tokens", active_tokens.len());
                        }
                    }
                    None => {
                        info!("WS: client dropped, exiting market background task");
                        return;
                    }
                }
            }
            
            Some(book_res) = async {
                match orderbook_stream.as_mut() {
                    Some(s) => s.next().await,
                    None => futures_util::future::pending().await,
                }
            } => {
                match book_res {
                    Ok(book) => {
                        handle_book_message(book, &books, &notify).await;
                    }
                    Err(e) => {
                        warn!("WS SDK Book error: {}", e);
                    }
                }
            }

            Some(price_res) = async {
                match prices_stream.as_mut() {
                    Some(s) => s.next().await,
                    None => futures_util::future::pending().await,
                }
            } => {
                match price_res {
                    Ok(price) => {
                        handle_price_change(price, &books, &notify).await;
                    }
                    Err(e) => {
                        warn!("WS SDK PriceChange error: {}", e);
                    }
                }
            }
        }
    }
}

async fn handle_book_message(
    book: BookUpdate,
    books: &Arc<RwLock<HashMap<String, BookEntry>>>,
    notify: &Arc<Notify>,
) {
    let mut order_book = OrderBook {
        bids: book.bids.iter().map(|b| PriceLevel {
            price: b.price.to_string(),
            size: b.size.to_string(),
        }).collect(),
        asks: book.asks.iter().map(|a| PriceLevel {
            price: a.price.to_string(),
            size: a.size.to_string(),
        }).collect(),
        timestamp: Some(book.timestamp as u64),
    };
    order_book.sort();

    let token_id_str = book.asset_id.to_string();
    {
        let mut w = books.write().await;
        w.insert(
            token_id_str,
            BookEntry {
                book: order_book,
                updated_at: Instant::now(),
            },
        );
    }
    notify.notify_one();
}

async fn handle_price_change(
    ev: PriceChange,
    books: &Arc<RwLock<HashMap<String, BookEntry>>>,
    notify: &Arc<Notify>,
) {
    let mut books_w = books.write().await;
    let mut overall_new = false;
    
    for change in &ev.price_changes {
        let token_id_str = change.asset_id.to_string();
        
        let entry = books_w
            .entry(token_id_str.clone())
            .or_insert_with(|| BookEntry {
                book: OrderBook::default(),
                updated_at: Instant::now(),
            });

        let mut new_ask_level = false;
        let mut new_bid_level = false;
        let size_f64: f64 = change.size.map(|d| d.to_string().parse().unwrap_or(0.0)).unwrap_or(0.0);
        let price_str = change.price.to_string();
        let size_str = change.size.map(|s| s.to_string()).unwrap_or_else(|| "0".to_string());

        let is_buy = matches!(change.side, polymarket_client_sdk::clob::types::Side::Buy);

        if is_buy {
            let levels = &mut entry.book.bids;
            if size_f64 == 0.0 {
                levels.retain(|l| l.price != price_str);
            } else if let Some(level) = levels.iter_mut().find(|l| l.price == price_str) {
                level.size = size_str;
            } else {
                levels.push(PriceLevel {
                    price: price_str.clone(),
                    size: size_str,
                });
                new_bid_level = true;
            }
        } else {
            let levels = &mut entry.book.asks;
            if size_f64 == 0.0 {
                levels.retain(|l| l.price != price_str);
            } else if let Some(level) = levels.iter_mut().find(|l| l.price == price_str) {
                level.size = size_str;
            } else {
                levels.push(PriceLevel {
                    price: price_str.clone(),
                    size: size_str,
                });
                new_ask_level = true;
            }
        }

        if new_ask_level || new_bid_level {
            entry.book.sort();
        }
        entry.updated_at = Instant::now();
        overall_new = true;
    }
    
    drop(books_w);
    if overall_new {
        notify.notify_one();
    }
}

async fn ws_user_loop(
    api_key: String,
    api_secret: String,
    api_passphrase: String,
    events_tx: mpsc::UnboundedSender<Value>,
    err_tx: mpsc::UnboundedSender<String>,
    is_connected: Arc<AtomicBool>,
) {
    let uuid_key = match uuid::Uuid::parse_str(&api_key) {
        Ok(u) => u,
        Err(_) => {
            let _ = err_tx.send("WS User Error: API key is not valid UUID".to_string());
            return;
        }
    };
    
    let credentials = Credentials::new(uuid_key, api_secret.clone(), api_passphrase.clone());
    let address = Address::default();
    
    let auth_client = match Client::default().authenticate(credentials, address) {
        Ok(c) => c,
        Err(e) => {
            let _ = err_tx.send(format!("WS User Error on Auth: {}", e));
            return;
        }
    };
    
    is_connected.store(true, Ordering::Relaxed);
    info!("WS SDK User: connected securely");

    let stream_res = auth_client.subscribe_user_events(vec![]);
    let mut stream = match stream_res {
        Ok(s) => Box::pin(s),
        Err(e) => {
            let _ = err_tx.send(format!("WS SDK User stream error: {}", e));
            return;
        }
    };

    while let Some(msg_res) = stream.next().await {
        match msg_res {
            Ok(WsMessage::Order(o)) => {
                let v = json!({
                    "event_type": "order",
                    "id": o.id,
                    "owner": o.owner.map(|a| a.to_string()).unwrap_or_default(),
                    "market": o.market.to_string(),
                    "asset_id": o.asset_id.to_string(),
                    "side": format!("{:?}", o.side).to_uppercase(),
                    "original_size": o.original_size.map(|d| d.to_string()).unwrap_or_else(|| "0".to_string()),
                    "size_matched": o.size_matched.map(|d| d.to_string()).unwrap_or_else(|| "0".to_string()),
                    "price": o.price.to_string(),
                    "outcome": o.outcome,
                    "status": o.status.map(|s| format!("{:?}", s).to_uppercase()).unwrap_or_else(|| "UNKNOWN".to_string())
                });
                let _ = events_tx.send(v);
            }
            Ok(WsMessage::Trade(t)) => {
                let v = json!({
                    "event_type": "trade",
                    "id": t.id,
                    "taker_order_id": t.taker_order_id.unwrap_or_default(),
                    "market": t.market.to_string(),
                    "asset_id": t.asset_id.to_string(),
                    "side": format!("{:?}", t.side).to_uppercase(),
                    "size": t.size.to_string(),
                    "price": t.price.to_string(),
                    "status": format!("{:?}", t.status).to_uppercase(),
                    "matchtime": t.matchtime.map(|m| m.to_string()).unwrap_or_default(),
                    "timestamp": t.timestamp.map(|m| m.to_string()).unwrap_or_default()
                });
                let _ = events_tx.send(v);
            }
            Ok(_) => {} // ignore other types
            Err(e) => {
                warn!("WS SDK User event error: {}", e);
            }
        }
    }
    
    is_connected.store(false, Ordering::Relaxed);
    info!("WS SDK User: stream ended");
}
