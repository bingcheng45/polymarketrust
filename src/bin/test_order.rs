//! Single-order connectivity tester (replicates test_order.ts).
//!
//! Places a small GTC BUY order then immediately cancels it.
//!
//! Usage: cargo run --bin test_order

use anyhow::{Context, Result};
use polymarketrust::clob_client::ClobClient;
use polymarketrust::config::Config;
use polymarketrust::types::{Side, TimeInForce};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let config = Arc::new(Config::load().context("Failed to load config")?);
    let client = ClobClient::new(Arc::clone(&config)).await.context("Failed to build CLOB client")?;

    println!("Signer  : {:?}", client.signer_address());
    println!("Maker   : {:?}", client.maker_address());

    let market_slug = config.market_slugs.first().cloned().unwrap_or_default();
    let mi = match client.find_active_market(&market_slug).await? {
        Some(m) => m,
        None => {
            eprintln!("No active market found for slug '{market_slug}'");
            std::process::exit(1);
        }
    };

    println!("Market  : {}", mi.question);
    println!("YES ID  : {}", mi.tokens.yes);
    println!("NO  ID  : {}", mi.tokens.no);

    let book = client.get_order_book(&mi.tokens.yes).await?;
    let yes_ask = book.best_ask().unwrap_or(0.5);
    println!("YES ask : {yes_ask:.3}");

    // Place a minimal order far from the market (very unlikely to fill).
    // Round to market tick precision to avoid float artefacts like 0.5199999999.
    let raw_test_price = (yes_ask - 0.05).max(0.01);
    let test_price = (raw_test_price * 100.0).round() / 100.0;
    let test_size = 1.0;

    println!("Placing test GTC BUY: size={test_size} @ {test_price:.3}…");

    let order = client
        .sign_order(
            &mi.tokens.yes,
            test_price,
            test_size,
            Side::Buy,
            TimeInForce::Gtc,
            mi.neg_risk,
            0,
        )
        .await?;

    let resp = client.post_order(order, "GTC").await?;
    let order_id = &resp.order_id;

    if order_id.is_empty() {
        println!("Order not placed — status: {:?}", resp.status);
    } else {
        println!("Order placed! ID: {order_id}");
        client.cancel_order(order_id).await?;
        println!("Order cancelled. Connectivity test PASSED!");
    }

    Ok(())
}
