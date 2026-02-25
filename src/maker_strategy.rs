//! GTC limit order maker strategy (replicates maker_strategy.ts).
//!
//! Instead of taking liquidity with FOK orders, this mode posts GTC
//! bids at `ask - (SPREAD_TICKS × tick_size)` for both YES and NO legs.
//! It handles partial and full fills, hedges imbalances, and only updates
//! orders when prices move more than one tick from the last posted price.

use crate::clob_client::ClobClient;
use crate::config::Config;
use crate::types::{MarketInfo, Side, TimeInForce};
use anyhow::Result;
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

pub struct MakerStrategy {
    config: Arc<Config>,
    yes_order_id: Option<String>,
    no_order_id: Option<String>,
    last_yes_price: Option<f64>,
    last_no_price: Option<f64>,
    last_update: Option<Instant>,
}

impl MakerStrategy {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            yes_order_id: None,
            no_order_id: None,
            last_yes_price: None,
            last_no_price: None,
            last_update: None,
        }
    }

    /// Post or update GTC bids below current best asks.
    ///
    /// Returns true if orders were (re)posted.
    pub async fn update_orders(
        &mut self,
        client: &ClobClient,
        market: &MarketInfo,
        yes_ask: f64,
        no_ask: f64,
        fee_rate_bps: u64,
    ) -> Result<bool> {
        let spread_ticks = self.config.maker_spread_ticks as f64 * market.tick_size;
        let yes_bid = (yes_ask - spread_ticks).max(0.01);
        let no_bid = (no_ask - spread_ticks).max(0.01);

        // Ensure combined bid is below $1 (otherwise no arb edge)
        if yes_bid + no_bid >= 1.0 {
            return Ok(false);
        }

        // Only update if prices moved more than 1 tick since last post
        let tick = market.tick_size;
        let yes_moved = self
            .last_yes_price
            .map(|p| (yes_bid - p).abs() > tick)
            .unwrap_or(true);
        let no_moved = self
            .last_no_price
            .map(|p| (no_bid - p).abs() > tick)
            .unwrap_or(true);

        if !yes_moved && !no_moved {
            return Ok(false);
        }

        // Cancel stale orders first
        if let Some(id) = self.yes_order_id.take() {
            let _ = client.cancel_order(&id).await;
        }
        if let Some(id) = self.no_order_id.take() {
            let _ = client.cancel_order(&id).await;
        }

        let size = self.config.min_liquidity_size;

        // Sign both bids
        let yes_order = client
            .sign_order(
                &market.tokens.yes,
                yes_bid,
                size,
                Side::Buy,
                TimeInForce::Gtc,
                market.neg_risk,
                fee_rate_bps,
            )
            .await?;

        let no_order = client
            .sign_order(
                &market.tokens.no,
                no_bid,
                size,
                Side::Buy,
                TimeInForce::Gtc,
                market.neg_risk,
                fee_rate_bps,
            )
            .await?;

        // Post both
        let results = client
            .post_orders(&[&yes_order, &no_order], "GTC")
            .await?;

        if results.len() >= 2 {
            self.yes_order_id = Some(results[0].order_id.clone());
            self.no_order_id = Some(results[1].order_id.clone());
            self.last_yes_price = Some(yes_bid);
            self.last_no_price = Some(no_bid);
            self.last_update = Some(Instant::now());

            info!(
                "Maker: posted YES bid @ {yes_bid:.3} / NO bid @ {no_bid:.3} (size {size})"
            );
        }

        Ok(true)
    }

    /// Poll for fills and return any imbalance that needs hedging.
    ///
    /// Returns `(yes_filled, no_filled)` sizes.
    pub async fn check_fills(
        &mut self,
        client: &ClobClient,
        condition_id: &str,
    ) -> Result<(f64, f64)> {
        let mut yes_filled = 0.0_f64;
        let mut no_filled = 0.0_f64;

        if let Some(ref id) = self.yes_order_id.clone() {
            if let Ok(Some(order)) = client.get_order(id).await {
                yes_filled = order.matched_f64();
                if order.remaining_f64() <= 0.0 {
                    self.yes_order_id = None;
                }
            }
        }

        if let Some(ref id) = self.no_order_id.clone() {
            if let Ok(Some(order)) = client.get_order(id).await {
                no_filled = order.matched_f64();
                if order.remaining_f64() <= 0.0 {
                    self.no_order_id = None;
                }
            }
        }

        Ok((yes_filled, no_filled))
    }

    /// Cancel all open maker orders.
    pub async fn cancel_all(&mut self, client: &ClobClient) -> Result<()> {
        if let Some(id) = self.yes_order_id.take() {
            let _ = client.cancel_order(&id).await;
        }
        if let Some(id) = self.no_order_id.take() {
            let _ = client.cancel_order(&id).await;
        }
        self.last_yes_price = None;
        self.last_no_price = None;
        Ok(())
    }

    pub fn has_open_orders(&self) -> bool {
        self.yes_order_id.is_some() || self.no_order_id.is_some()
    }
}
