#![allow(unused)]
use anyhow::Result;
use futures_util::StreamExt;
use polymarket_client_sdk::auth::Credentials;
use polymarket_client_sdk::clob::ws::Client;
use polymarket_client_sdk::clob::ws::types::response::{BookUpdate, PriceChange, WsMessage};
use polymarket_client_sdk::types::{Address, B256, U256};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Notify, RwLock};

pub struct WsClient;

async fn ws_market_loop(
    mut cmd_rx: mpsc::UnboundedReceiver<Vec<String>>,
    err_tx: mpsc::UnboundedSender<String>,
    is_connected: Arc<AtomicBool>,
) {
    let client = Client::default();
    let mut active_tokens: Vec<U256> = Vec::new();
    // In SDK, if we just want raw messages we might subscribe and listen
    // Actually `subscribe_orderbook` gives `Result<BookUpdate>`. But we also want price changes?
    // Wait!! The SDK `subscribe_orderbook` ONLY yields `BookUpdate` messages. If we want incremental price updates, we have to subscribe to `subscribe_prices(assets)` or `subscribe_orderbook` handles it under the hood?
    // Let me check if `subscribe_orderbook` yields both `Book` AND `PriceChange`, or ONLY `BookUpdate`.
}

fn main() {}
