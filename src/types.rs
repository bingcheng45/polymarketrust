use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ─── Orderbook ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PriceLevel {
    pub price: String,
    pub size: String,
}

impl PriceLevel {
    pub fn price_f64(&self) -> f64 {
        self.price.parse().unwrap_or(0.0)
    }
    pub fn size_f64(&self) -> f64 {
        self.size.parse().unwrap_or(0.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OrderBook {
    /// Bids sorted descending by price (highest first).
    pub bids: Vec<PriceLevel>,
    /// Asks sorted ascending by price (lowest first).
    pub asks: Vec<PriceLevel>,
    pub timestamp: Option<u64>,
}

impl OrderBook {
    /// Best ask price (lowest ask).
    pub fn best_ask(&self) -> Option<f64> {
        self.asks.first().map(|l| l.price_f64())
    }

    /// Size available at the best ask price level.
    pub fn best_ask_size(&self) -> f64 {
        self.asks.first().map(|l| l.size_f64()).unwrap_or(0.0)
    }

    /// Best bid price (highest bid).
    pub fn best_bid(&self) -> Option<f64> {
        self.bids.first().map(|l| l.price_f64())
    }

    /// Total available liquidity at or below a given price cap.
    pub fn ask_liquidity_at(&self, price_cap: f64) -> f64 {
        self.asks
            .iter()
            .filter(|l| l.price_f64() <= price_cap)
            .map(|l| l.size_f64())
            .sum()
    }

    /// Sort asks ascending, bids descending (applied after ingestion).
    pub fn sort(&mut self) {
        self.asks.sort_by(|a, b| {
            a.price_f64()
                .partial_cmp(&b.price_f64())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        self.bids.sort_by(|a, b| {
            b.price_f64()
                .partial_cmp(&a.price_f64())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }
}

// ─── Token IDs ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenIds {
    pub yes: String,
    pub no: String,
}

// ─── Market Info ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketInfo {
    pub condition_id: String,
    pub question: String,
    pub end_date: DateTime<Utc>,
    pub neg_risk: bool,
    pub tick_size: f64,
    pub tokens: TokenIds,
}

// ─── Arbitrage Opportunity ───────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ArbOpportunity {
    pub yes_price: f64,
    pub no_price: f64,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub total_cost: f64,
    /// Nominal spread: 1.0 - total_cost (before fees/gas)
    pub spread: f64,
}

// ─── Order ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Side {
    #[serde(rename = "BUY")]
    Buy,
    #[serde(rename = "SELL")]
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TimeInForce {
    #[serde(rename = "FOK")]
    Fok,
    #[serde(rename = "GTC")]
    Gtc,
    #[serde(rename = "GTD")]
    Gtd,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OrderStatus {
    #[serde(rename = "LIVE")]
    Live,
    #[serde(rename = "MATCHED")]
    Matched,
    #[serde(rename = "DELAYED")]
    Delayed,
    #[serde(rename = "FILLED")]
    Filled,
    #[serde(rename = "CANCELLED")]
    Cancelled,
    #[serde(rename = "UNMATCHED")]
    Unmatched,
}

/// The EIP-712 signed order body submitted to the CLOB.
pub use polymarket_client_sdk::clob::types::SignedOrder;

/// Response from POST /order or POST /orders.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderResponse {
    #[serde(rename = "orderID")]
    pub order_id: String,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(rename = "sizeMatched", default)]
    pub size_matched: Option<String>,
    #[serde(rename = "errorMsg", default)]
    pub error_msg: Option<String>,
}

/// An open (live) order returned by GET /orders.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenOrder {
    pub id: String,
    #[serde(rename = "tokenID", default)]
    pub token_id: String,
    pub side: String,
    pub price: String,
    #[serde(rename = "originalSize")]
    pub original_size: String,
    #[serde(rename = "sizeMatched")]
    pub size_matched: String,
    #[serde(rename = "remainingSize", default)]
    pub remaining_size: String,
    #[serde(default)]
    pub status: Option<String>,
}

impl OpenOrder {
    pub fn matched_f64(&self) -> f64 {
        self.size_matched.parse().unwrap_or(0.0)
    }
    pub fn remaining_f64(&self) -> f64 {
        self.remaining_size.parse().unwrap_or(0.0)
    }
    pub fn price_f64(&self) -> f64 {
        self.price.parse().unwrap_or(0.0)
    }
}

/// A historical trade returned by GET /trades.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub id: String,
    #[serde(rename = "conditionId", default)]
    pub condition_id: String,
    #[serde(rename = "tokenID", default)]
    pub token_id: String,
    pub side: String,
    pub price: String,
    pub size: String,
    #[serde(rename = "feeRateBps", default)]
    pub fee_rate_bps: Option<String>,
    #[serde(rename = "createdAt", default)]
    pub created_at: Option<String>,
}

// ─── Trade Log Entry ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TradeType {
    #[serde(rename = "EXECUTION")]
    Execution,
    #[serde(rename = "HEDGE")]
    Hedge,
    #[serde(rename = "SELLBACK")]
    Sellback,
    #[serde(rename = "REDEMPTION")]
    Redemption,
    #[serde(rename = "MAKER_FILL")]
    MakerFill,
    #[serde(rename = "MERGE")]
    Merge,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeLogEntry {
    pub timestamp: DateTime<Utc>,
    #[serde(rename = "type")]
    pub trade_type: TradeType,
    #[serde(rename = "conditionId")]
    pub condition_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub market: Option<String>,
    #[serde(rename = "yesPrice", skip_serializing_if = "Option::is_none")]
    pub yes_price: Option<f64>,
    #[serde(rename = "noPrice", skip_serializing_if = "Option::is_none")]
    pub no_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<f64>,
    #[serde(rename = "costUsd", skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
    #[serde(rename = "gasFeeUsd", skip_serializing_if = "Option::is_none")]
    pub gas_fee_usd: Option<f64>,
    #[serde(rename = "profitUsd", skip_serializing_if = "Option::is_none")]
    pub profit_usd: Option<f64>,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// ─── Market Statistics ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MarketStats {
    pub total_arb_opportunities: u64,
    pub total_arb_executions: u64,
    pub successes: u64,
    pub failures: u64,
    pub hedged: u64,
    pub sold_back: u64,
    pub total_minutes: f64,
    pub total_pnl_usd: f64,
    pub best_spread: f64,
    pub avg_arb_spread: f64,
    pub last_session: String,
}

// ─── Position State ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct Position {
    pub yes_size: f64,
    pub no_size: f64,
    pub yes_cost: f64,
    pub no_cost: f64,
}

impl Position {
    pub fn is_balanced(&self) -> bool {
        (self.yes_size - self.no_size).abs() < 0.01
    }

    pub fn mergeable_amount(&self) -> f64 {
        f64::min(self.yes_size, self.no_size)
    }

    pub fn has_imbalance(&self) -> bool {
        !self.is_balanced() && (self.yes_size > 0.01 || self.no_size > 0.01)
    }
}

// ─── WebSocket Events ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsBookEvent {
    pub event_type: String,
    pub asset_id: String,
    #[serde(default)]
    pub bids: Vec<PriceLevel>,
    #[serde(default)]
    pub asks: Vec<PriceLevel>,
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceChange {
    pub price: String,
    pub size: String,
    pub side: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsPriceChangeEvent {
    pub event_type: String,
    pub asset_id: String,
    #[serde(default)]
    pub changes: Vec<PriceChange>,
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsOrderEvent {
    pub event_type: String, // "order"
    pub id: String, // order hash
    pub owner: String, // eoa/proxy
    pub market: String, // condition ID
    pub asset_id: String, // token ID
    pub side: String, // "BUY" | "SELL"
    pub original_size: String,
    pub size_matched: String,
    pub price: String,
    pub outcome: Option<String>, // "YES" | "NO"
    pub status: String, // "LIVE" | "MATCHED" | "PARTIALLY_MATCHED" | "CANCELED"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsTradeEvent {
    pub event_type: String, // "trade"
    pub id: String, // trade ID
    pub taker_order_id: String,
    pub market: String, // condition ID
    pub asset_id: String, // token ID
    pub side: String, // "BUY" | "SELL"
    pub size: String,
    pub price: String,
    pub status: String, // "MATCHED"
    pub matchtime: String,
    pub timestamp: String,
}

// ─── Gas Cache ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct GasCache {
    pub gas_price_gwei: f64,
    pub pol_price_usd: f64,
    /// Estimated USD cost of a merge transaction.
    pub fee_per_merge_usd: f64,
    pub updated_at: std::time::Instant,
}

impl GasCache {
    pub fn is_stale(&self) -> bool {
        self.updated_at.elapsed().as_secs() > 30
    }
}

impl Default for GasCache {
    fn default() -> Self {
        Self {
            gas_price_gwei: 30.0,
            pol_price_usd: 0.50,
            fee_per_merge_usd: 0.004,
            updated_at: std::time::Instant::now()
                - std::time::Duration::from_secs(60), // stale immediately
        }
    }
}

// ─── Balance ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceAllowance {
    #[serde(rename = "balance")]
    pub balance: String,
}

impl BalanceAllowance {
    pub fn balance_f64(&self) -> f64 {
        // CLOB returns balance in micro-USDC (6 decimals): "30980000" = $30.98
        let raw: f64 = self.balance.parse().unwrap_or(0.0);
        raw / 1_000_000.0
    }
}

// ─── Gamma API (market discovery) ────────────────────────────────────────────

/// Top-level event returned by GET /events on the Gamma API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GammaEvent {
    pub slug: Option<String>,
    #[serde(rename = "endDate")]
    pub end_date: Option<String>,
    #[serde(default)]
    pub markets: Option<Vec<GammaMarket>>,
}

/// A market nested inside a GammaEvent.
///
/// Token IDs and outcomes are JSON-encoded strings from the API, e.g.:
///   `clobTokenIds`: `"[\"id1\",\"id2\"]"`
///   `outcomes`:     `"[\"Up\",\"Down\"]"`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GammaMarket {
    #[serde(rename = "conditionId")]
    pub condition_id: String,
    pub question: String,
    pub slug: Option<String>,
    #[serde(rename = "endDate")]
    pub end_date: Option<String>,
    #[serde(rename = "negRisk", default)]
    pub neg_risk: bool,
    /// JSON string: `"[\"token_id_1\",\"token_id_2\"]"`
    #[serde(rename = "clobTokenIds")]
    pub clob_token_ids: Option<String>,
    /// JSON string: `"[\"Up\",\"Down\"]"` or `"[\"Yes\",\"No\"]"`
    pub outcomes: Option<String>,
    #[serde(rename = "enableOrderBook", default)]
    pub enable_order_book: Option<bool>,
    #[serde(rename = "orderPriceMinTickSize")]
    pub tick_size: Option<f64>,
    #[serde(default)]
    pub closed: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_position_balanced() {
        let mut pos = Position::default();
        assert!(pos.is_balanced());
        
        pos.yes_size = 50.0;
        pos.no_size = 50.0;
        assert!(pos.is_balanced());
        
        pos.yes_size = 50.0;
        pos.no_size = 49.0;
        assert!(!pos.is_balanced());
    }

    #[test]
    fn test_position_mergeable_amount() {
        let pos = Position {
            yes_size: 100.0,
            no_size: 150.0,
            yes_cost: 0.0,
            no_cost: 0.0,
        };
        assert_eq!(pos.mergeable_amount(), 100.0);
    }

    #[test]
    fn test_position_has_imbalance() {
        let pos = Position {
            yes_size: 100.0,
            no_size: 50.0,
            ..Default::default()
        };
        assert!(pos.has_imbalance());
        
        let pos2 = Position {
            yes_size: 100.0,
            no_size: 100.0,
            ..Default::default()
        };
        assert!(!pos2.has_imbalance());
    }
}
