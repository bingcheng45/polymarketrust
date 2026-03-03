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
    yes_net_filled: f64,
    no_net_filled: f64,
    fee_enabled: bool,
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
            yes_net_filled: 0.0,
            no_net_filled: 0.0,
            fee_enabled: false,
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
        balance_usdc: f64,
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
        self.yes_net_filled = 0.0;
        self.no_net_filled = 0.0;

        self.fee_enabled = fee_rate_bps > 0;
        let size = self.quote_order_size(yes_bid, no_bid, balance_usdc);
        if size <= 0.0 {
            info!(
                "Maker: quote skipped (insufficient balance ${balance_usdc:.2} for min viable paired size)"
            );
            return Ok(false);
        }

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
            .post_orders(vec![yes_order, no_order], "GTC")
            .await?;

        if results.len() >= 2 {
            let yes_order_id = results[0].order_id.trim().to_string();
            let no_order_id = results[1].order_id.trim().to_string();
            if yes_order_id.is_empty() || no_order_id.is_empty() {
                return Ok(false);
            }

            self.yes_order_id = Some(yes_order_id);
            self.no_order_id = Some(no_order_id);
            self.last_yes_price = Some(yes_bid);
            self.last_no_price = Some(no_bid);
            self.yes_net_filled = 0.0;
            self.no_net_filled = 0.0;
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
        _condition_id: &str,
    ) -> Result<(f64, f64)> {
        let mut yes_delta = 0.0_f64;
        let mut no_delta = 0.0_f64;

        if let Some(ref id) = self.yes_order_id.clone() {
            if let Ok(Some(order)) = client.get_order(id).await {
                let net_total = self.net_size_after_fee(order.matched_f64(), order.price_f64());
                yes_delta = (net_total - self.yes_net_filled).max(0.0);
                self.yes_net_filled = net_total;
                if order.remaining_f64() <= 0.0 {
                    self.yes_order_id = None;
                    self.yes_net_filled = 0.0;
                }
            }
        }

        if let Some(ref id) = self.no_order_id.clone() {
            if let Ok(Some(order)) = client.get_order(id).await {
                let net_total = self.net_size_after_fee(order.matched_f64(), order.price_f64());
                no_delta = (net_total - self.no_net_filled).max(0.0);
                self.no_net_filled = net_total;
                if order.remaining_f64() <= 0.0 {
                    self.no_order_id = None;
                    self.no_net_filled = 0.0;
                }
            }
        }

        Ok((yes_delta, no_delta))
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
        self.yes_net_filled = 0.0;
        self.no_net_filled = 0.0;
        Ok(())
    }

    pub fn has_open_orders(&self) -> bool {
        self.yes_order_id.is_some() || self.no_order_id.is_some()
    }

    fn quote_order_size(&self, yes_price: f64, no_price: f64, balance_usdc: f64) -> f64 {
        let min_sellable = std::env::var("MIN_PAIRED_SHARES")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(5.0)
            .max(5.0);

        let max_fee = self.effective_fee(yes_price).max(self.effective_fee(no_price));
        let denom = (1.0 - max_fee).max(0.000_001);
        let min_gross_for_sellable = (((min_sellable + 0.01) / denom) * 100.0).ceil() / 100.0;
        let pair_price = (yes_price + no_price).max(0.000_001);
        let spendable = (balance_usdc * 0.98).max(0.0);
        let balance_cap = ((spendable / pair_price) * 100.0).floor() / 100.0;
        let size_cap = self.config.max_trade_size.max(0.0);
        let hard_cap = balance_cap.min(size_cap);
        if hard_cap + 1e-9 < min_gross_for_sellable {
            return 0.0;
        }
        self.config
            .min_liquidity_size
            .max(min_gross_for_sellable)
            .min(hard_cap)
    }

    fn net_size_after_fee(&self, gross_size: f64, price: f64) -> f64 {
        let net = gross_size * (1.0 - self.effective_fee(price));
        (net * 100.0).floor() / 100.0
    }

    fn effective_fee(&self, price: f64) -> f64 {
        if !self.fee_enabled {
            return 0.0;
        }
        self.config.clob_fee_rate * (price * (1.0 - price)).powf(self.config.clob_fee_exponent)
    }
}
