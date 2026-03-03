//! Polymarket CLOB REST API client (replicates @polymarket/clob-client).
//!
//! Handles:
//! - L2 authentication (HMAC-SHA256 signed headers)
//! - EIP-712 order signing (manual implementation — no derive macros)
//! - Order placement, cancellation, and querying
//! - Balance/allowance queries
//! - Trade history & market metadata

use crate::config::Config;
use crate::types::{
    GammaEvent, GammaMarket, MarketInfo, OpenOrder, OrderBook, OrderResponse,
    Side, SignedOrder, TimeInForce, TokenIds, TradeRecord,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

const CLOB_BASE: &str = "https://clob.polymarket.com";
const GAMMA_BASE: &str = "https://gamma-api.polymarket.com";
const MARKET_EXPIRY_BUFFER_SECS: i64 = 10;
const SAFE_RECEIPT_TIMEOUT_SECS: u64 = 20;
pub const POLYGON_RPCS: [&str; 3] = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
    "https://polygon.drpc.org",
];

// Polygon contract addresses
pub const CTF_EXCHANGE: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";
pub const NEG_RISK_CTF_EXCHANGE: &str = "0xC5d563A36AE78145C45a50134d48A1215220f80a";
pub const NEG_RISK_ADAPTER: &str = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296";
pub const CONDITIONAL_TOKENS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
pub const USDC_ADDRESS: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";

// SDK imports
use polymarket_client_sdk::auth::Credentials;
use polymarket_client_sdk::clob::Client as SdkClient;
use polymarket_client_sdk::clob::Config as SdkConfig;
use polymarket_client_sdk::types::{Address, U256, B256, Decimal};
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::clob::types::{OrderType as SdkOrderType, Side as SdkSide, AssetType, SignatureType};
use polymarket_client_sdk::clob::types::request::{BalanceAllowanceRequest, OrderBookSummaryRequest, OrdersRequest, TradesRequest};
use polymarket_client_sdk::data::Client as DataClient;
use polymarket_client_sdk::data::types::request::PositionsRequest;
// CTF client for on-chain operations
use polymarket_client_sdk::ctf;
use polymarket_client_sdk::ctf::types::{MergePositionsRequest, RedeemPositionsRequest};
use polymarket_client_sdk::POLYGON;

// Alloy imports
use alloy::network::EthereumWallet;
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer as AlloySigner;
use alloy::sol_types::SolCall;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

// ─── Solidity interfaces via alloy sol! macro ─────────────────────────────────

// CTF contract — used for ABI encoding calldata in Gnosis Safe path + read-only eth_call.
// The EOA path uses polymarket_client_sdk::ctf::Client which has its own internal sol! binding.
alloy::sol! {
    interface IConditionalTokens {
        function balanceOf(address account, uint256 id) external view returns (uint256);
        function isApprovedForAll(address account, address operator) external view returns (bool);
        function setApprovalForAll(address operator, bool approved) external;
        function mergePositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] calldata partition,
            uint256 amount
        ) external;
        function redeemPositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] calldata indexSets
        ) external;
    }
}

// Gnosis Safe — used for the Safe path when SIGNATURE_TYPE=2.
alloy::sol! {
    #[sol(rpc)]
    interface IGnosisSafe {
        function execTransaction(
            address to,
            uint256 value,
            bytes calldata data,
            uint8 operation,
            uint256 safeTxGas,
            uint256 baseGas,
            uint256 gasPrice,
            address gasToken,
            address refundReceiver,
            bytes memory signatures
        ) external payable returns (bool success);
    }
}

// ─── Client ───────────────────────────────────────────────────────────────────

pub struct ClobClient {
    http: reqwest::Client,
    config: Arc<Config>,
    sdk_client: SdkClient<Authenticated<Normal>>,
    signer: PrivateKeySigner,
    signer_addr: Address,
    maker_address: Address,
}

impl ClobClient {
    pub async fn new(config: Arc<Config>) -> Result<Self> {
        crate::init_rustls_provider();

        let disable_system_proxy = std::env::var("DISABLE_SYSTEM_PROXY")
            .unwrap_or_else(|_| "true".to_string())
            .parse::<bool>()
            .unwrap_or(true);
        if disable_system_proxy
            && (std::env::var("HTTP_PROXY").is_ok()
                || std::env::var("HTTPS_PROXY").is_ok()
                || std::env::var("ALL_PROXY").is_ok())
        {
            warn!(
                "DISABLE_SYSTEM_PROXY=true: ignoring HTTP(S)_PROXY/ALL_PROXY for lower latency path"
            );
        }

        let mut http_builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(2))
            .pool_idle_timeout(Duration::from_secs(30));
        if disable_system_proxy {
            http_builder = http_builder.no_proxy();
        }
        let http = http_builder.build()?;

        let raw_key = config.private_key.trim_start_matches("0x");

        let signer = PrivateKeySigner::from_str(raw_key)
            .context("Invalid private key")?
            .with_chain_id(Some(POLYGON));

        let signer_addr = signer.address();

        let maker_address = if let Some(proxy) = &config.poly_proxy_address {
            proxy.parse::<Address>().context("Invalid POLY_PROXY_ADDRESS")?
        } else {
            signer_addr
        };

        let sdk_config = SdkConfig::builder().use_server_time(true).build();
        let unauth_client = SdkClient::new(CLOB_BASE, sdk_config)?;

        let uuid_key = uuid::Uuid::parse_str(&config.poly_api_key)
            .context("Invalid API Key UUID")?;

        let credentials = Credentials::new(
            uuid_key,
            config.poly_api_secret.clone(),
            config.poly_api_passphrase.clone(),
        );

        let sig_type = match config.signature_type {
            1 => SignatureType::Proxy,
            2 => SignatureType::GnosisSafe,
            _ => SignatureType::Eoa,
        };

        let auth_client = unauth_client
            .authentication_builder(&signer)
            .credentials(credentials)
            .funder(maker_address)
            .signature_type(sig_type)
            .authenticate()
            .await
            .context("Failed to authenticate SDK Client")?;

        Ok(Self {
            http,
            config,
            sdk_client: auth_client,
            signer,
            signer_addr,
            maker_address,
        })
    }

    pub fn signer_address(&self) -> Address {
        self.signer_addr
    }

    pub fn maker_address(&self) -> Address {
        self.maker_address
    }

    // ─── EIP-712 Order Signing ────────────────────────────────────────────────

    pub async fn sign_order(
        &self,
        token_id: &str,
        price: f64,
        size: f64,
        side: Side,
        time_in_force: TimeInForce,
        _neg_risk: bool,
        _fee_rate_bps: u64,
    ) -> Result<SignedOrder> {
        self.sign_order_with_post_only(
            token_id,
            price,
            size,
            side,
            time_in_force,
            _neg_risk,
            _fee_rate_bps,
            false,
        )
        .await
    }

    pub async fn sign_order_with_post_only(
        &self,
        token_id: &str,
        price: f64,
        size: f64,
        side: Side,
        time_in_force: TimeInForce,
        _neg_risk: bool,
        _fee_rate_bps: u64,
        post_only: bool,
    ) -> Result<SignedOrder> {
        let size = (size * 100.0).floor() / 100.0;
        let size_decimal = Decimal::from_str(&size.to_string())?;
        let price_decimal = Decimal::from_str(&price.to_string())?;

        let sdk_side = match side {
            Side::Buy => SdkSide::Buy,
            Side::Sell => SdkSide::Sell,
        };
        let sdk_order_type = match time_in_force {
            TimeInForce::Fok => SdkOrderType::FOK,
            TimeInForce::Fak => SdkOrderType::FAK,
            TimeInForce::Gtc => SdkOrderType::GTC,
            TimeInForce::Gtd => SdkOrderType::GTD,
        };

        let limit_order = self
            .sdk_client
            .limit_order()
            .token_id(U256::from_str(token_id)?)
            .price(price_decimal)
            .size(size_decimal)
            .side(sdk_side)
            .order_type(sdk_order_type)
            .post_only(post_only)
            .build()
            .await?;

        let signed = self.sdk_client.sign(&self.signer, limit_order).await?;
        Ok(signed)
    }

    // ─── REST API Methods ─────────────────────────────────────────────────────

    pub async fn get_order_book(&self, token_id: &str) -> Result<OrderBook> {
        let req = OrderBookSummaryRequest::builder()
            .token_id(U256::from_str(token_id)?)
            .build();

        match self.sdk_client.order_book(&req).await {
            Ok(resp) => {
                let mut book = OrderBook {
                    bids: resp.bids.into_iter().map(|o| crate::types::PriceLevel {
                        price: o.price.to_string(),
                        size: o.size.to_string(),
                    }).collect(),
                    asks: resp.asks.into_iter().map(|o| crate::types::PriceLevel {
                        price: o.price.to_string(),
                        size: o.size.to_string(),
                    }).collect(),
                    timestamp: None,
                };
                book.sort();
                Ok(book)
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("404") || msg.contains("Not Found") {
                    Ok(OrderBook::default())
                } else {
                    Err(e.into())
                }
            }
        }
    }

    pub async fn post_order(
        &self,
        order: SignedOrder,
        _time_in_force: &str,
    ) -> Result<OrderResponse> {
        match self.sdk_client.post_order(order).await {
            Ok(resp) => {
                let status_str = format!("{:?}", resp.status).to_uppercase();
                Ok(OrderResponse {
                    order_id: resp.order_id,
                    status: Some(status_str),
                    size_matched: Some(resp.taking_amount.to_string()),
                    error_msg: resp.error_msg,
                })
            }
            Err(e) => {
                let text = e.to_string();
                let text_lc = text.to_ascii_lowercase();
                if text_lc.contains("not filled")
                    || text_lc.contains("no matching")
                    || text_lc.contains("no orders found to match")
                {
                    return Ok(OrderResponse {
                        order_id: String::new(),
                        status: Some("UNMATCHED".to_string()),
                        size_matched: Some("0".to_string()),
                        error_msg: None,
                    });
                }
                anyhow::bail!("post_order error: {e}");
            }
        }
    }

    pub async fn post_orders(
        &self,
        orders: Vec<SignedOrder>,
        _time_in_force: &str,
    ) -> Result<Vec<OrderResponse>> {
        let resps = self.sdk_client.post_orders(orders).await?;
        Ok(resps.into_iter().map(|resp| OrderResponse {
            order_id: resp.order_id,
            status: Some(format!("{:?}", resp.status).to_uppercase()),
            size_matched: Some(resp.taking_amount.to_string()),
            error_msg: resp.error_msg,
        }).collect())
    }

    pub async fn cancel_order(&self, order_id: &str) -> Result<()> {
        let _ = self.sdk_client.cancel_order(order_id).await?;
        Ok(())
    }

    pub async fn cancel_all_orders(&self) -> Result<()> {
        let _ = self.sdk_client.cancel_all_orders().await?;
        Ok(())
    }

    pub async fn get_open_orders(&self, condition_id: &str) -> Result<Vec<OpenOrder>> {
        let req = OrdersRequest::builder().market(B256::from_str(condition_id)?).build();
        let page = self.sdk_client.orders(&req, None).await?;

        Ok(page.data.into_iter().map(|o| OpenOrder {
            id: o.id,
            status: Some(format!("{:?}", o.status).to_uppercase()),
            token_id: o.asset_id.to_string(),
            side: format!("{:?}", o.side).to_uppercase(),
            original_size: o.original_size.to_string(),
            size_matched: o.size_matched.to_string(),
            remaining_size: (o.original_size - o.size_matched).to_string(),
            price: o.price.to_string(),
        }).collect())
    }

    pub async fn get_order(&self, order_id: &str) -> Result<Option<OpenOrder>> {
        match self.sdk_client.order(order_id).await {
            Ok(o) => {
                Ok(Some(OpenOrder {
                    id: o.id,
                    status: Some(format!("{:?}", o.status).to_uppercase()),
                    token_id: o.asset_id.to_string(),
                    side: format!("{:?}", o.side).to_uppercase(),
                    original_size: o.original_size.to_string(),
                    size_matched: o.size_matched.to_string(),
                    remaining_size: (o.original_size - o.size_matched).to_string(),
                    price: o.price.to_string(),
                }))
            }
            Err(_) => Ok(None)
        }
    }

    pub async fn get_balance(&self) -> Result<f64> {
        const USDC_MICRO_UNITS: f64 = 1_000_000.0;
        let req = BalanceAllowanceRequest::builder().asset_type(AssetType::Collateral).build();
        let resp = self.sdk_client.balance_allowance(req).await?;
        let balance_str = resp.balance.to_string();
        let raw = balance_str.parse::<f64>().unwrap_or(0.0);
        let usdc = raw / USDC_MICRO_UNITS;
        info!("Balance: raw='{}' micro-USDC → ${:.6}", balance_str, usdc);
        Ok(usdc)
    }

    pub async fn get_trades(&self, condition_id: Option<&str>) -> Result<Vec<TradeRecord>> {
        let req = if let Some(cid) = condition_id {
            TradesRequest::builder()
                .maker_address(self.maker_address.0.0.into())
                .market(B256::from_str(cid)?)
                .build()
        } else {
            TradesRequest::builder()
                .maker_address(self.maker_address.0.0.into())
                .build()
        };
        const TERMINAL_CURSOR: &str = "LTE=";
        const MAX_TRADE_PAGES: usize = 50;

        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        let mut pages = 0usize;

        loop {
            let page = self.sdk_client.trades(&req, cursor.clone()).await?;
            out.extend(page.data.into_iter().map(|t| TradeRecord {
                id: t.id,
                condition_id: format!("{:?}", t.market),
                token_id: t.asset_id.to_string(),
                side: format!("{:?}", t.side).to_uppercase(),
                price: t.price.to_string(),
                size: t.size.to_string(),
                fee_rate_bps: Some(t.fee_rate_bps.to_string()),
                created_at: Some(t.match_time.timestamp().to_string()),
            }));

            pages += 1;
            if page.next_cursor == TERMINAL_CURSOR || page.next_cursor.is_empty() {
                break;
            }
            if pages >= MAX_TRADE_PAGES {
                info!(
                    "Trade pagination stopped at {} pages to avoid excessive API usage",
                    MAX_TRADE_PAGES
                );
                break;
            }
            cursor = Some(page.next_cursor);
        }

        Ok(out)
    }

    pub async fn get_market_fee_rate_bps(&self, token_id: &str) -> Result<u64> {
        let resp = self.sdk_client.fee_rate_bps(U256::from_str(token_id)?).await?;
        Ok(resp.base_fee as u64)
    }

    /// Discover redeemable conditions for this wallet using Polymarket Data API.
    ///
    /// Returns tuples of `(condition_id, market_title, claimable_usdc)`.
    pub async fn get_redeemable_conditions(&self) -> Result<Vec<(String, String, f64)>> {
        const LIMIT: i32 = 500;
        const MAX_OFFSET: i32 = 10_000;

        let data_client = DataClient::default();
        let mut offset = 0_i32;
        let mut by_condition: HashMap<String, (String, f64)> = HashMap::new();

        loop {
            let req = PositionsRequest::builder()
                .user(self.maker_address)
                .redeemable(true)
                .size_threshold(Decimal::ZERO)
                .limit(LIMIT)?
                .offset(offset)?
                .build();

            let positions = data_client.positions(&req).await?;
            if positions.is_empty() {
                break;
            }

            for pos in positions.iter() {
                let current_value = pos.current_value.to_string().parse::<f64>().unwrap_or(0.0);
                if current_value <= 0.0 {
                    continue;
                }

                let condition_id = format!("{:?}", pos.condition_id);
                let entry = by_condition
                    .entry(condition_id)
                    .or_insert_with(|| (pos.title.clone(), 0.0));
                entry.1 += current_value;
            }

            if positions.len() < LIMIT as usize {
                break;
            }

            offset += LIMIT;
            if offset > MAX_OFFSET {
                info!(
                    "Redeemable position scan stopped at offset {} to avoid excessive API usage",
                    MAX_OFFSET
                );
                break;
            }
        }

        let mut out: Vec<(String, String, f64)> = by_condition
            .into_iter()
            .map(|(cid, (title, amount))| (cid, title, amount))
            .collect();
        out.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        Ok(out)
    }

    // ─── Gamma API (Market Discovery) ────────────────────────────────────────
    /// Find the active market for a slug prefix using the Gamma API.
    /// Returns the soonest-ending active market to maximise theta-decay edge.
    pub async fn find_active_market(&self, slug_prefix: &str) -> Result<Option<MarketInfo>> {
        let slug_prefix = slug_prefix.trim().to_ascii_lowercase();
        if slug_prefix.is_empty() {
            return Ok(None);
        }

        let tag_id: Option<u64> = if slug_prefix.contains("15m") {
            Some(102467)
        } else if slug_prefix.contains("5m") {
            Some(102892)
        } else if slug_prefix.contains("updown") {
            Some(102127)
        } else {
            None
        };

        let now = Utc::now();
        let end_date_min = now.to_rfc3339_opts(SecondsFormat::Secs, true);
        let min_end_date = now + ChronoDuration::seconds(MARKET_EXPIRY_BUFFER_SECS);

        let build_events_url = |with_end_date_min: bool, with_tag_id: Option<u64>| {
            let mut url = format!(
                "{GAMMA_BASE}/events?limit=100&active=true&closed=false&order=endDate&ascending=true"
            );
            if with_end_date_min {
                url.push_str(&format!("&end_date_min={end_date_min}"));
            }
            if let Some(tid) = with_tag_id {
                url.push_str(&format!("&tag_id={tid}"));
            }
            url
        };
        let build_markets_url = |with_end_date_min: bool, with_tag_id: Option<u64>| {
            let mut url = format!(
                "{GAMMA_BASE}/markets?limit=300&active=true&closed=false&order=endDate&ascending=true"
            );
            if with_end_date_min {
                url.push_str(&format!("&end_date_min={end_date_min}"));
            }
            if let Some(tid) = with_tag_id {
                url.push_str(&format!("&tag_id={tid}"));
            }
            url
        };

        // Primary query is strict; fallbacks protect against transient tag/filter/API inconsistencies.
        let query_plan = [
            ("tag+end_date_min", true, tag_id),
            ("no_tag+end_date_min", true, None),
            ("tag_only", false, tag_id),
            ("no_tag", false, None),
        ];

        let mut seen_event_urls = HashSet::new();
        let mut had_successful_gamma_response = false;
        let mut last_gamma_error: Option<anyhow::Error> = None;

        for (label, with_end_date_min, with_tag_id) in query_plan {
            let url = build_events_url(with_end_date_min, with_tag_id);
            if !seen_event_urls.insert(url.clone()) {
                continue;
            }

            let events = match self.fetch_gamma_events(&url).await {
                Ok(events) => {
                    had_successful_gamma_response = true;
                    events
                }
                Err(e) => {
                    warn!(
                        "Gamma discovery query '{label}' failed for slug '{slug_prefix}': {e}"
                    );
                    last_gamma_error = Some(e);
                    continue;
                }
            };

            if events.is_empty() {
                continue;
            }

            let sorted = select_market_candidates(events, &slug_prefix, min_end_date);
            for (m, end_date) in sorted {
                if let Some(info) = market_info_from_gamma_market(m, end_date) {
                    return Ok(Some(info));
                }
            }
        }

        // Secondary source: Gamma /markets. This endpoint tends to stay healthy
        // even when /events intermittently fails.
        let mut seen_market_urls = HashSet::new();
        let mut had_successful_market_response = false;
        let mut last_market_error: Option<anyhow::Error> = None;
        for (label, with_end_date_min, with_tag_id) in query_plan {
            let url = build_markets_url(with_end_date_min, with_tag_id);
            if !seen_market_urls.insert(url.clone()) {
                continue;
            }

            let markets = match self.fetch_gamma_markets(&url).await {
                Ok(markets) => {
                    had_successful_market_response = true;
                    markets
                }
                Err(e) => {
                    warn!(
                        "Gamma markets discovery query '{label}' failed for slug '{slug_prefix}': {e}"
                    );
                    last_market_error = Some(e);
                    continue;
                }
            };

            if markets.is_empty() {
                continue;
            }

            let sorted = select_market_candidates_from_markets(markets, &slug_prefix, min_end_date);
            for (m, end_date) in sorted {
                if let Some(info) = market_info_from_gamma_market(m, end_date) {
                    return Ok(Some(info));
                }
            }
        }

        if !had_successful_gamma_response && !had_successful_market_response {
            if let Some(err) = last_market_error.or(last_gamma_error) {
                return Err(err).context(format!(
                    "all Gamma discovery queries failed for slug '{slug_prefix}'"
                ));
            }
        }

        Ok(None)
    }

    async fn fetch_gamma_events(&self, url: &str) -> Result<Vec<GammaEvent>> {
        self.fetch_gamma_payload(url, "events").await
    }

    async fn fetch_gamma_markets(&self, url: &str) -> Result<Vec<GammaMarket>> {
        self.fetch_gamma_payload(url, "markets").await
    }

    async fn fetch_gamma_payload<T>(&self, url: &str, payload_name: &str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let retries = env_u64("GAMMA_REQUEST_RETRIES", 2);
        let base_backoff_ms = env_u64("GAMMA_RETRY_BASE_MS", 300).max(50);
        let mut last_err: Option<anyhow::Error> = None;

        for attempt in 0..=retries {
            match Self::fetch_gamma_payload_with_client(&self.http, url, payload_name).await {
                Ok(payload) => return Ok(payload),
                Err(e) => {
                    last_err = Some(e);
                    if attempt < retries {
                        let delay_ms = base_backoff_ms.saturating_mul(1u64 << attempt.min(6));
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                }
            }
        }

        // Fallback path: use a fresh client with system proxy settings enabled.
        // This helps recovery when the long-lived low-latency client is in a bad
        // network state or no-proxy routing is temporarily broken.
        let fallback_http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(3))
            .pool_idle_timeout(Duration::from_secs(5))
            .build()
            .context("failed to build fallback Gamma HTTP client")?;

        match Self::fetch_gamma_payload_with_client(&fallback_http, url, payload_name).await {
            Ok(payload) => Ok(payload),
            Err(fallback_err) => {
                let primary = last_err
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "unknown primary Gamma error".to_string());
                Err(fallback_err).with_context(|| {
                    format!("Gamma primary retries exhausted: {primary}")
                })
            }
        }
    }

    async fn fetch_gamma_payload_with_client<T>(
        client: &reqwest::Client,
        url: &str,
        payload_name: &str,
    ) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let resp = client
            .get(url)
            .send()
            .await
            .with_context(|| format!("Gamma request failed: {url}"))?;
        let status = resp.status();
        let body = resp
            .text()
            .await
            .context("Failed to read Gamma response body")?;

        if !status.is_success() {
            let snippet: String = body.chars().take(240).collect();
            return Err(anyhow!(
                "Gamma {} API HTTP {} for {} | body: {}",
                payload_name,
                status,
                url,
                snippet
            ));
        }

        serde_json::from_str::<T>(&body)
            .with_context(|| format!("Failed to parse Gamma {payload_name} payload from {url}"))
    }

    // ─── Price Feeds ──────────────────────────────────────────────────────────

    /// Fetch POL/USD price: Binance → CoinGecko → static $0.50.
    pub async fn get_pol_price_usd(&self) -> f64 {
        if let Ok(resp) = self
            .http
            .get("https://api.binance.com/api/v3/ticker/price?symbol=POLUSDT")
            .send()
            .await
        {
            if let Ok(v) = resp.json::<Value>().await {
                if let Some(p) = v
                    .get("price")
                    .and_then(|x| x.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                {
                    return p;
                }
            }
        }

        if let Ok(resp) = self
            .http
            .get("https://api.coingecko.com/api/v3/simple/price?ids=matic-network&vs_currencies=usd")
            .send()
            .await
        {
            if let Ok(v) = resp.json::<Value>().await {
                if let Some(p) = v
                    .get("matic-network")
                    .and_then(|x| x.get("usd"))
                    .and_then(|x| x.as_f64())
                {
                    return p;
                }
            }
        }

        0.50
    }

    /// Fetch current Polygon gas price in Gwei via JSON-RPC.
    pub async fn get_gas_price_gwei(&self) -> f64 {
        let body = json!({
            "jsonrpc": "2.0",
            "method": "eth_gasPrice",
            "params": [],
            "id": 1
        });

        for rpc in &POLYGON_RPCS {
            if let Ok(resp) = self.http.post(*rpc).json(&body).send().await {
                if let Ok(v) = resp.json::<Value>().await {
                    if let Some(hex_str) = v.get("result").and_then(|x| x.as_str()) {
                        let clean = hex_str.trim_start_matches("0x");
                        if let Ok(wei) = u64::from_str_radix(clean, 16) {
                            return wei as f64 / 1e9;
                        }
                    }
                }
            }
        }

        30.0 // Fallback: 30 Gwei
    }

    // ─── On-chain Contract Calls ──────────────────────────────────────────────

    /// Burns YES + NO shares → USDC by calling `mergePositions` on the Conditional Token contract.
    ///
    /// Supports both EOA (SIGNATURE_TYPE=0) and Gnosis Safe (SIGNATURE_TYPE=2).
    /// `amount` is in shares; internally converted to 1e6 units (USDC precision).
    ///
    /// EOA path: delegates to polymarket_client_sdk::ctf::Client (handles all contract routing).
    /// Safe path: builds calldata via sol! ABI encoding and calls execTransaction on the Safe.
    pub async fn merge_positions(
        &self,
        condition_id: &str,
        amount: f64,
        neg_risk: bool,
        rpc_url: &str,
    ) -> Result<B256> {
        let usdc: Address = USDC_ADDRESS.parse()?;
        let cid: B256 = B256::from_str(condition_id)?;
        let amount_u256 = U256::from((amount * 1_000_000.0).floor() as u128);

        if self.config.uses_proxy() {
            // ── Gnosis Safe path ──────────────────────────────────────────────
            let safe_addr: Address = self
                .config
                .poly_proxy_address
                .as_deref()
                .unwrap()
                .parse()
                .context("Invalid POLY_PROXY_ADDRESS")?;

            // mergePositions is on CONDITIONAL_TOKENS for standard markets,
            // or NEG_RISK_ADAPTER for neg-risk markets.
            let contract_addr: Address = if neg_risk {
                NEG_RISK_ADAPTER.parse()?
            } else {
                CONDITIONAL_TOKENS.parse()?
            };

            let inner = IConditionalTokens::mergePositionsCall {
                collateralToken: usdc,
                parentCollectionId: B256::ZERO,
                conditionId: cid,
                partition: vec![U256::from(1u64), U256::from(2u64)],
                amount: amount_u256,
            }.abi_encode();

            self.safe_exec_transaction(safe_addr, contract_addr, inner, 500_000, rpc_url)
                .await
        } else {
            // ── EOA path — SDK ctf::Client handles all contract routing ───────
            let provider = ProviderBuilder::new()
                .wallet(EthereumWallet::from(self.signer.clone()))
                .connect_http(rpc_url.parse().context("Invalid RPC URL")?);

            let ctf_client = if neg_risk {
                ctf::Client::with_neg_risk(provider, POLYGON)?
            } else {
                ctf::Client::new(provider, POLYGON)?
            };

            let req = MergePositionsRequest::for_binary_market(usdc, cid, amount_u256);
            let resp = ctf_client.merge_positions(&req).await?;
            Ok(resp.transaction_hash)
        }
    }

    /// Claims USDC from a resolved market by calling `redeemPositions` on the CTF contract.
    ///
    /// Uses the standard binary partition [1, 2] for 2-outcome markets.
    /// Supports both EOA and Gnosis Safe paths.
    pub async fn redeem_positions(
        &self,
        condition_id: &str,
        rpc_url: &str,
    ) -> Result<B256> {
        let usdc: Address = USDC_ADDRESS.parse()?;
        let cid: B256 = B256::from_str(condition_id)?;

        if self.config.uses_proxy() {
            // ── Gnosis Safe path ──────────────────────────────────────────────
            let safe_addr: Address = self
                .config
                .poly_proxy_address
                .as_deref()
                .unwrap()
                .parse()
                .context("Invalid POLY_PROXY_ADDRESS")?;

            let ctf_addr: Address = CONDITIONAL_TOKENS.parse()?;

            let inner = IConditionalTokens::redeemPositionsCall {
                collateralToken: usdc,
                parentCollectionId: B256::ZERO,
                conditionId: cid,
                indexSets: vec![U256::from(1u64), U256::from(2u64)],
            }.abi_encode();

            self.safe_exec_transaction(safe_addr, ctf_addr, inner, 500_000, rpc_url)
                .await
        } else {
            // ── EOA path ─────────────────────────────────────────────────────
            let provider = ProviderBuilder::new()
                .wallet(EthereumWallet::from(self.signer.clone()))
                .connect_http(rpc_url.parse().context("Invalid RPC URL")?);

            let ctf_client = ctf::Client::new(provider, POLYGON)?;
            let req = RedeemPositionsRequest::for_binary_market(usdc, cid);
            let resp = ctf_client.redeem_positions(&req).await?;
            Ok(resp.transaction_hash)
        }
    }

    /// Check on-chain CTF ERC-1155 token balance for a given token ID and holder.
    pub async fn get_ctf_balance(
        &self,
        token_id: &str,
        holder: Address,
        rpc_url: &str,
    ) -> Result<f64> {
        let token_id_u256 = parse_token_id(token_id)?;

        let calldata = IConditionalTokens::balanceOfCall {
            account: holder,
            id: token_id_u256,
        }.abi_encode();

        let call_hex = format!("0x{}", hex::encode(&calldata));
        let body = json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": CONDITIONAL_TOKENS, "data": call_hex}, "latest"],
            "id": 1
        });

        let resp = self.http.post(rpc_url).json(&body).send().await?;
        let v: Value = resp.json().await?;
        if let Some(err) = v.get("error") {
            anyhow::bail!("RPC eth_call error: {}", err);
        }
        let hex_result = v
            .get("result")
            .and_then(|x| x.as_str())
            .context("RPC eth_call missing result")?;
        let clean = hex_result.trim_start_matches("0x");
        let raw =
            u128::from_str_radix(if clean.is_empty() { "0" } else { clean }, 16).unwrap_or(0);

        Ok(raw as f64 / 1_000_000.0)
    }

    /// Ensure the Gnosis Safe has `setApprovalForAll` granted to all three CTF operators.
    ///
    /// Should be called once during bot initialization when SIGNATURE_TYPE=2.
    /// Already-approved operators are skipped to avoid wasting gas.
    pub async fn ensure_ctf_approvals(&self, rpc_url: &str) -> Result<()> {
        if !self.config.uses_proxy() {
            return Ok(());
        }

        let safe_addr: Address = self
            .config
            .poly_proxy_address
            .as_deref()
            .unwrap()
            .parse()
            .context("Invalid POLY_PROXY_ADDRESS")?;

        let ctf_addr: Address = CONDITIONAL_TOKENS.parse()?;

        let operators: &[(&str, &str)] = &[
            ("CTF Exchange", CTF_EXCHANGE),
            ("NegRisk CTF Exchange", NEG_RISK_CTF_EXCHANGE),
            ("NegRisk Adapter", NEG_RISK_ADAPTER),
        ];

        for (name, operator_str) in operators {
            let operator: Address = operator_str.parse()?;

            let already_approved = self
                .check_ctf_approval(safe_addr, operator, rpc_url)
                .await
                .unwrap_or(false);

            if already_approved {
                info!("CTF operator already approved: {name}");
                continue;
            }

            info!("Granting CTF setApprovalForAll to {name} via Safe...");

            let calldata = IConditionalTokens::setApprovalForAllCall {
                operator,
                approved: true,
            }.abi_encode();

            self.safe_exec_transaction(safe_addr, ctf_addr, calldata, 200_000, rpc_url)
                .await?;

            info!("CTF approval granted to {name}");
        }

        Ok(())
    }

    /// Query `isApprovedForAll(account, operator)` on the CTF contract via eth_call.
    async fn check_ctf_approval(
        &self,
        account: Address,
        operator: Address,
        rpc_url: &str,
    ) -> Result<bool> {
        let calldata = IConditionalTokens::isApprovedForAllCall {
            account,
            operator,
        }.abi_encode();

        let call_hex = format!("0x{}", hex::encode(&calldata));
        let body = json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": CONDITIONAL_TOKENS, "data": call_hex}, "latest"],
            "id": 1
        });

        let resp = self.http.post(rpc_url).json(&body).send().await?;
        let v: Value = resp.json().await?;
        let hex_result = v
            .get("result")
            .and_then(|x| x.as_str())
            .unwrap_or("0x0");
        let clean = hex_result.trim_start_matches("0x");
        let val =
            u128::from_str_radix(if clean.is_empty() { "0" } else { clean }, 16).unwrap_or(0);

        Ok(val != 0)
    }

    /// Execute a call through the Gnosis Safe using a pre-validated signature.
    ///
    /// The EOA (`self.signer_address`) must already be a registered owner of the Safe.
    /// Uses a legacy (type-0) transaction with an explicit gas limit + 20%-buffered gas price.
    ///
    /// Replicates the TypeScript pattern:
    ///   `safe.execTransaction(to, 0, data, 0, 0, 0, 0, addr0, addr0, signatures)`
    async fn safe_exec_transaction(
        &self,
        safe_addr: Address,
        target: Address,
        inner_calldata: Vec<u8>,
        gas_limit: u64,
        rpc_url: &str,
    ) -> Result<B256> {
        // Fetch live gas price and add 20% buffer
        let gas_price_gwei = self.get_gas_price_gwei().await;
        let gas_price_wei: u128 = ((gas_price_gwei * 1.2) * 1e9) as u128;

        // Pre-validated signature: uint256(signerAddr) ‖ uint256(0) ‖ uint8(1)
        let sigs = pre_validated_signature(self.signer_addr);

        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(self.signer.clone()))
            .connect_http(rpc_url.parse().context("Invalid RPC URL")?);

        let pending_tx = IGnosisSafe::new(safe_addr, provider)
            .execTransaction(
                target,
                U256::ZERO,               // value = 0 ETH
                inner_calldata.into(),    // inner calldata
                0u8,                      // operation = CALL
                U256::ZERO,               // safeTxGas (0 = use all gas forwarded)
                U256::ZERO,               // baseGas
                U256::ZERO,               // gasPrice (no Safe refund)
                Address::ZERO,            // gasToken
                Address::ZERO,            // refundReceiver
                sigs.into(),              // pre-validated signatures
            )
            .gas(gas_limit)
            .gas_price(gas_price_wei)
            .send()
            .await
            .context("Gnosis Safe execTransaction: failed to send")?;

        let tx_hash = *pending_tx.tx_hash();

        let receipt_timeout_secs = env_u64("SAFE_RECEIPT_TIMEOUT_SECS", SAFE_RECEIPT_TIMEOUT_SECS);
        tokio::time::timeout(Duration::from_secs(receipt_timeout_secs), pending_tx.get_receipt())
            .await
            .with_context(|| {
                format!(
                    "Gnosis Safe execTransaction: receipt wait timed out after {}s",
                    receipt_timeout_secs
                )
            })?
            .context("Gnosis Safe execTransaction: tx dropped from mempool")?;

        Ok(tx_hash)
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Build a Gnosis Safe "pre-validated signature".
///
/// Layout: uint256(signerAddress) ‖ uint256(0) ‖ uint8(1) = 65 bytes total.
/// - Bytes  0-31: signer address right-aligned in a 32-byte word (12 zero bytes + 20-byte addr)
/// - Bytes 32-63: zero  (s = 0)
/// - Byte    64:  0x01  (v = 1 = pre-validated, not an ECDSA sig)
///
/// This tells the Safe that `signerAddress` (an on-chain registered owner) approves
/// the transaction without requiring a live ECDSA signature.
fn pre_validated_signature(signer: Address) -> Vec<u8> {
    let mut sig = vec![0u8; 65];
    sig[12..32].copy_from_slice(signer.as_slice()); // right-align address in first 32 bytes
    // bytes 32-63 already zero (s = 0)
    sig[64] = 1u8; // v = 1 (pre-validated)
    sig
}

/// Parse a token ID that might be decimal or hex.
fn parse_token_id(token_id: &str) -> Result<U256> {
    if token_id.starts_with("0x") || token_id.starts_with("0X") {
        let clean = &token_id[2..];
        U256::from_str_radix(clean, 16)
            .map_err(|e| anyhow::anyhow!("Invalid hex token_id '{}': {}", token_id, e))
    } else {
        U256::from_str_radix(token_id, 10)
            .map_err(|e| anyhow::anyhow!("Invalid decimal token_id '{}': {}", token_id, e))
    }
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn select_market_candidates(
    events: Vec<GammaEvent>,
    slug_prefix: &str,
    min_end_date: DateTime<Utc>,
) -> Vec<(GammaMarket, DateTime<Utc>)> {
    let mut candidates = Vec::new();
    for event in events {
        let event_slug_matches = event
            .slug
            .as_deref()
            .map(|slug| slug.to_ascii_lowercase().starts_with(slug_prefix))
            .unwrap_or(false);

        let markets = match event.markets {
            Some(markets) => markets,
            None => continue,
        };

        for market in markets {
            let market_slug_matches = market
                .slug
                .as_deref()
                .map(|slug| slug.to_ascii_lowercase().starts_with(slug_prefix))
                .unwrap_or(false);

            if !event_slug_matches && !market_slug_matches {
                continue;
            }

            let end_date = match parse_market_end_date(market.end_date.as_deref()) {
                Some(dt) => dt,
                None => continue,
            };

            if end_date <= min_end_date {
                continue;
            }

            candidates.push((market, end_date));
        }
    }

    candidates.sort_by(|a, b| a.1.cmp(&b.1));
    candidates
}

fn select_market_candidates_from_markets(
    markets: Vec<GammaMarket>,
    slug_prefix: &str,
    min_end_date: DateTime<Utc>,
) -> Vec<(GammaMarket, DateTime<Utc>)> {
    let mut candidates = Vec::new();
    for market in markets {
        let market_slug_matches = market
            .slug
            .as_deref()
            .map(|slug| slug.to_ascii_lowercase().starts_with(slug_prefix))
            .unwrap_or(false);
        if !market_slug_matches {
            continue;
        }
        if market.closed.unwrap_or(false) {
            continue;
        }

        let end_date = match parse_market_end_date(market.end_date.as_deref()) {
            Some(dt) => dt,
            None => continue,
        };
        if end_date <= min_end_date {
            continue;
        }

        candidates.push((market, end_date));
    }

    candidates.sort_by(|a, b| a.1.cmp(&b.1));
    candidates
}

fn parse_market_end_date(raw: Option<&str>) -> Option<DateTime<Utc>> {
    raw.and_then(|s| s.parse::<DateTime<Utc>>().ok())
}

fn market_info_from_gamma_market(market: GammaMarket, end_date: DateTime<Utc>) -> Option<MarketInfo> {
    let clob_token_ids_str = market.clob_token_ids.as_deref()?;
    let (yes, no) = extract_binary_token_pair(clob_token_ids_str, market.outcomes.as_deref())?;
    Some(MarketInfo {
        condition_id: market.condition_id,
        question: market.question,
        end_date,
        neg_risk: market.neg_risk,
        tick_size: market.tick_size.unwrap_or(0.01),
        tokens: TokenIds { yes, no },
    })
}

fn extract_binary_token_pair(
    clob_token_ids_raw: &str,
    outcomes_raw: Option<&str>,
) -> Option<(String, String)> {
    let clob_token_ids: Vec<String> = serde_json::from_str(clob_token_ids_raw).ok()?;
    if clob_token_ids.len() < 2 {
        return None;
    }

    if let Some(outcomes_raw) = outcomes_raw {
        if let Ok(outcomes) = serde_json::from_str::<Vec<String>>(outcomes_raw) {
            let mut yes_token = None;
            let mut no_token = None;

            for (tid, out) in clob_token_ids.iter().zip(outcomes.iter()) {
                let label = out.trim().to_ascii_uppercase();
                if label == "YES" || label == "UP" {
                    yes_token = Some(tid.clone());
                } else if label == "NO" || label == "DOWN" {
                    no_token = Some(tid.clone());
                }
            }

            if let (Some(yes), Some(no)) = (yes_token, no_token) {
                return Some((yes, no));
            }
        }
    }

    Some((clob_token_ids[0].clone(), clob_token_ids[1].clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn gamma_market(
        slug: &str,
        end_date: &str,
        outcomes: &str,
        clob_token_ids: &str,
    ) -> GammaMarket {
        GammaMarket {
            condition_id: format!("cid-{slug}"),
            question: format!("Question for {slug}"),
            slug: Some(slug.to_string()),
            end_date: Some(end_date.to_string()),
            neg_risk: false,
            clob_token_ids: Some(clob_token_ids.to_string()),
            outcomes: Some(outcomes.to_string()),
            enable_order_book: Some(true),
            tick_size: Some(0.01),
            closed: Some(false),
        }
    }

    fn gamma_event(slug: &str, markets: Vec<GammaMarket>) -> GammaEvent {
        GammaEvent {
            slug: Some(slug.to_string()),
            end_date: markets.first().and_then(|m| m.end_date.clone()),
            markets: Some(markets),
        }
    }

    #[test]
    fn extract_binary_pair_yes_no() {
        let pair = extract_binary_token_pair(r#"["yes_id","no_id"]"#, Some(r#"["Yes","No"]"#))
            .expect("expected token pair");
        assert_eq!(pair.0, "yes_id");
        assert_eq!(pair.1, "no_id");
    }

    #[test]
    fn extract_binary_pair_up_down_and_lowercase() {
        let pair = extract_binary_token_pair(r#"["up_id","down_id"]"#, Some(r#"["up","down"]"#))
            .expect("expected token pair");
        assert_eq!(pair.0, "up_id");
        assert_eq!(pair.1, "down_id");
    }

    #[test]
    fn extract_binary_pair_unknown_labels_falls_back_to_first_two() {
        let pair = extract_binary_token_pair(
            r#"["token_a","token_b","token_c"]"#,
            Some(r#"["Bull","Bear","Sideways"]"#),
        )
        .expect("expected token pair");
        assert_eq!(pair.0, "token_a");
        assert_eq!(pair.1, "token_b");
    }

    #[test]
    fn select_market_candidates_prefers_nearest_future_match() {
        let now = Utc
            .with_ymd_and_hms(2026, 2, 26, 18, 30, 0)
            .single()
            .expect("valid datetime");

        let events = vec![
            gamma_event(
                "btc-updown-5m-foo",
                vec![
                    gamma_market(
                        "btc-updown-5m-early",
                        "2026-02-26T18:20:00Z",
                        r#"["Up","Down"]"#,
                        r#"["a","b"]"#,
                    ),
                    gamma_market(
                        "btc-updown-5m-target",
                        "2026-02-26T18:35:00Z",
                        r#"["Up","Down"]"#,
                        r#"["c","d"]"#,
                    ),
                    gamma_market(
                        "btc-updown-5m-later",
                        "2026-02-26T18:40:00Z",
                        r#"["Up","Down"]"#,
                        r#"["e","f"]"#,
                    ),
                ],
            ),
            gamma_event(
                "eth-updown-5m-foo",
                vec![gamma_market(
                    "eth-updown-5m-other",
                    "2026-02-26T18:31:00Z",
                    r#"["Up","Down"]"#,
                    r#"["g","h"]"#,
                )],
            ),
        ];

        let selected = select_market_candidates(events, "btc-updown-5m", now);
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].0.slug.as_deref(), Some("btc-updown-5m-target"));
        assert_eq!(selected[1].0.slug.as_deref(), Some("btc-updown-5m-later"));
    }

    #[test]
    fn select_market_candidates_filters_non_matching_prefixes() {
        let now = Utc
            .with_ymd_and_hms(2026, 2, 26, 18, 30, 0)
            .single()
            .expect("valid datetime");

        let events = vec![
            gamma_event(
                "eth-updown-5m-foo",
                vec![gamma_market(
                    "eth-updown-5m-1",
                    "2026-02-26T18:45:00Z",
                    r#"["Up","Down"]"#,
                    r#"["eth_up","eth_down"]"#,
                )],
            ),
            gamma_event(
                "xrp-updown-5m-foo",
                vec![gamma_market(
                    "xrp-updown-5m-1",
                    "2026-02-26T18:45:00Z",
                    r#"["Up","Down"]"#,
                    r#"["xrp_up","xrp_down"]"#,
                )],
            ),
            gamma_event(
                "btc-updown-5m-foo",
                vec![gamma_market(
                    "btc-updown-5m-1",
                    "2026-02-26T18:45:00Z",
                    r#"["Up","Down"]"#,
                    r#"["btc_up","btc_down"]"#,
                )],
            ),
        ];

        let selected = select_market_candidates(events, "btc-updown-5m", now);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].0.slug.as_deref(), Some("btc-updown-5m-1"));
    }

    #[test]
    fn select_market_candidates_from_markets_prefers_nearest_future_match() {
        let now = Utc
            .with_ymd_and_hms(2026, 3, 2, 10, 40, 0)
            .single()
            .expect("valid datetime");

        let markets = vec![
            gamma_market(
                "btc-updown-5m-early",
                "2026-03-02T10:39:59Z",
                r#"["Up","Down"]"#,
                r#"["a","b"]"#,
            ),
            gamma_market(
                "eth-updown-5m-x",
                "2026-03-02T10:45:00Z",
                r#"["Up","Down"]"#,
                r#"["c","d"]"#,
            ),
            gamma_market(
                "btc-updown-5m-1",
                "2026-03-02T10:45:00Z",
                r#"["Up","Down"]"#,
                r#"["e","f"]"#,
            ),
            gamma_market(
                "btc-updown-5m-2",
                "2026-03-02T10:50:00Z",
                r#"["Up","Down"]"#,
                r#"["g","h"]"#,
            ),
        ];

        let selected = select_market_candidates_from_markets(markets, "btc-updown-5m", now);
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].0.slug.as_deref(), Some("btc-updown-5m-1"));
        assert_eq!(selected[1].0.slug.as_deref(), Some("btc-updown-5m-2"));
    }
}
