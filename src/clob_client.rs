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
    BalanceAllowance, GammaMarket, MarketInfo, OpenOrder, OrderBook, OrderResponse,
    Side, SignedOrder, TimeInForce, TokenIds, TradeRecord,
};
use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use chrono::Utc;
use ethers::abi::{encode as abi_encode, Token};
use ethers::core::k256::ecdsa::SigningKey;
use ethers::prelude::*;
use ethers::utils::keccak256;
use hmac::{Hmac, Mac};
use reqwest::{Client, Response};
use serde_json::{json, Value};
use sha2::Sha256;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

const CLOB_BASE: &str = "https://clob.polymarket.com";
const GAMMA_BASE: &str = "https://gamma-api.polymarket.com";

// Polygon contract addresses
pub const CTF_EXCHANGE: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";
pub const NEG_RISK_CTF_EXCHANGE: &str = "0xC5d563A36AE78145C45a50134d48A1215220f80a";
pub const NEG_RISK_ADAPTER: &str = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296";
pub const CONDITIONAL_TOKENS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
pub const USDC_ADDRESS: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";

// ─── EIP-712 Signing (manual) ─────────────────────────────────────────────────

const ORDER_TYPE_STR: &str = "Order(uint256 salt,address maker,address signer,address taker,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint256 expiration,uint256 nonce,uint256 feeRateBps,uint8 side,uint8 signatureType)";

const DOMAIN_TYPE_STR: &str =
    "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)";

fn domain_separator(verifying_contract: Address) -> [u8; 32] {
    let domain_type_hash = keccak256(DOMAIN_TYPE_STR.as_bytes());
    let name_hash = keccak256("Polymarket CTF Exchange".as_bytes());
    let version_hash = keccak256("1".as_bytes());

    let encoded = abi_encode(&[
        Token::FixedBytes(domain_type_hash.to_vec()),
        Token::FixedBytes(name_hash.to_vec()),
        Token::FixedBytes(version_hash.to_vec()),
        Token::Uint(U256::from(137u64)), // Polygon chain ID
        Token::Address(verifying_contract),
    ]);

    keccak256(encoded)
}

fn order_hash(
    salt: U256,
    maker: Address,
    signer: Address,
    token_id: U256,
    maker_amount: U256,
    taker_amount: U256,
    expiration: U256,
    nonce: U256,
    fee_rate_bps: U256,
    side: u8,
    signature_type: u8,
) -> [u8; 32] {
    let type_hash = keccak256(ORDER_TYPE_STR.as_bytes());

    let encoded = abi_encode(&[
        Token::FixedBytes(type_hash.to_vec()),
        Token::Uint(salt),
        Token::Address(maker),
        Token::Address(signer),
        Token::Address(Address::zero()), // taker = zero address
        Token::Uint(token_id),
        Token::Uint(maker_amount),
        Token::Uint(taker_amount),
        Token::Uint(expiration),
        Token::Uint(nonce),
        Token::Uint(fee_rate_bps),
        Token::Uint(U256::from(side as u64)),
        Token::Uint(U256::from(signature_type as u64)),
    ]);

    keccak256(encoded)
}

/// Compute the final EIP-712 hash: \x19\x01 || domainSeparator || structHash
fn typed_data_hash(domain_sep: [u8; 32], struct_hash: [u8; 32]) -> [u8; 32] {
    let mut data = Vec::with_capacity(66);
    data.extend_from_slice(&[0x19u8, 0x01u8]);
    data.extend_from_slice(&domain_sep);
    data.extend_from_slice(&struct_hash);
    keccak256(data)
}

// ─── Client ───────────────────────────────────────────────────────────────────

pub struct ClobClient {
    http: Client,
    config: Arc<Config>,
    wallet: LocalWallet,
    signer_address: Address,
    /// Maker address: proxy if using proxy, else signer.
    maker_address: Address,
    /// Domain separator for normal CTF Exchange.
    ds_ctf: [u8; 32],
    /// Domain separator for NegRisk CTF Exchange.
    ds_neg_risk: [u8; 32],
}

impl ClobClient {
    pub fn new(config: Arc<Config>) -> Result<Self> {
        let private_key_bytes = hex::decode(&config.private_key)?;
        let signing_key = SigningKey::from_bytes(private_key_bytes.as_slice().into())
            .context("Invalid private key")?;
        let wallet = LocalWallet::from(signing_key).with_chain_id(137u64);
        let signer_address = wallet.address();

        let maker_address = if let Some(proxy) = &config.poly_proxy_address {
            proxy
                .parse::<Address>()
                .context("Invalid POLY_PROXY_ADDRESS")?
        } else {
            signer_address
        };

        let ctf_addr: Address = CTF_EXCHANGE.parse().unwrap();
        let neg_risk_addr: Address = NEG_RISK_CTF_EXCHANGE.parse().unwrap();

        let http = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;

        Ok(Self {
            http,
            config,
            wallet,
            signer_address,
            maker_address,
            ds_ctf: domain_separator(ctf_addr),
            ds_neg_risk: domain_separator(neg_risk_addr),
        })
    }

    pub fn signer_address(&self) -> Address {
        self.signer_address
    }

    pub fn maker_address(&self) -> Address {
        self.maker_address
    }

    // ─── L2 Auth Helpers ──────────────────────────────────────────────────────

    fn l2_headers(&self, method: &str, path: &str, body: &str) -> Result<Vec<(String, String)>> {
        let timestamp = Utc::now().timestamp().to_string();
        let nonce = "0";
        let msg = format!("{}{}{}{}", timestamp, method.to_uppercase(), path, body);

        let secret_bytes = BASE64
            .decode(&self.config.poly_api_secret)
            .context("Invalid POLY_API_SECRET (not base64)")?;

        let mut mac =
            Hmac::<Sha256>::new_from_slice(&secret_bytes).context("HMAC key error")?;
        mac.update(msg.as_bytes());
        let result = mac.finalize();
        let signature = BASE64.encode(result.into_bytes());

        Ok(vec![
            (
                "POLY_ADDRESS".to_string(),
                format!("{:?}", self.maker_address),
            ),
            ("POLY_SIGNATURE".to_string(), signature),
            ("POLY_TIMESTAMP".to_string(), timestamp),
            ("POLY_NONCE".to_string(), nonce.to_string()),
            (
                "POLY_API_KEY".to_string(),
                self.config.poly_api_key.clone(),
            ),
            (
                "POLY_PASSPHRASE".to_string(),
                self.config.poly_api_passphrase.clone(),
            ),
        ])
    }

    async fn authenticated_get(&self, path: &str) -> Result<Response> {
        let url = format!("{CLOB_BASE}{path}");
        let headers = self.l2_headers("GET", path, "")?;
        let mut req = self.http.get(&url);
        for (k, v) in headers {
            req = req.header(k, v);
        }
        Ok(req.send().await?)
    }

    async fn authenticated_post(&self, path: &str, body: &Value) -> Result<Response> {
        let body_str = serde_json::to_string(body)?;
        let headers = self.l2_headers("POST", path, &body_str)?;
        let url = format!("{CLOB_BASE}{path}");
        let mut req = self.http.post(&url).json(body);
        for (k, v) in headers {
            req = req.header(k, v);
        }
        Ok(req.send().await?)
    }

    async fn authenticated_delete(&self, path: &str, body: &Value) -> Result<Response> {
        let body_str = serde_json::to_string(body)?;
        let headers = self.l2_headers("DELETE", path, &body_str)?;
        let url = format!("{CLOB_BASE}{path}");
        let mut req = self.http.delete(&url).json(body);
        for (k, v) in headers {
            req = req.header(k, v);
        }
        Ok(req.send().await?)
    }

    // ─── EIP-712 Order Signing ────────────────────────────────────────────────

    /// Compute makerAmount and takerAmount from price/size/side.
    /// All amounts are in "units of 1e6" (USDC has 6 decimals; CTF shares use 1e6 precision).
    fn amounts(price: f64, size: f64, side: &Side) -> (U256, U256) {
        let usdc = (price * size * 1_000_000.0).floor() as u128;
        let shares = (size * 1_000_000.0).floor() as u128;
        match side {
            Side::Buy => (U256::from(usdc), U256::from(shares)),
            Side::Sell => (U256::from(shares), U256::from(usdc)),
        }
    }

    /// Sign an order using EIP-712 typed data.
    pub async fn sign_order(
        &self,
        token_id: &str,
        price: f64,
        size: f64,
        side: Side,
        time_in_force: TimeInForce,
        neg_risk: bool,
        fee_rate_bps: u64,
    ) -> Result<SignedOrder> {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        // Floor size to 2 decimal places to prevent "insufficient balance" errors
        let size = (size * 100.0).floor() / 100.0;

        let salt = U256::from(rng.gen::<u64>());

        let token_id_u256 = parse_token_id(token_id)?;

        let expiration = match time_in_force {
            TimeInForce::Fok | TimeInForce::Gtc => U256::zero(),
            TimeInForce::Gtd => U256::from(Utc::now().timestamp() as u64 + 300),
        };

        let side_u8: u8 = match &side {
            Side::Buy => 0,
            Side::Sell => 1,
        };

        let (maker_amount, taker_amount) = Self::amounts(price, size, &side);

        let o_hash = order_hash(
            salt,
            self.maker_address,
            self.signer_address,
            token_id_u256,
            maker_amount,
            taker_amount,
            expiration,
            U256::zero(),
            U256::from(fee_rate_bps),
            side_u8,
            self.config.signature_type,
        );

        let ds = if neg_risk { self.ds_neg_risk } else { self.ds_ctf };
        let final_hash = typed_data_hash(ds, o_hash);

        // Sign the final hash using the wallet's private key
        let signature = self
            .wallet
            .sign_hash(H256::from(final_hash))
            .context("Signing failed")?;

        let side_str = match &side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        };

        Ok(SignedOrder {
            salt: salt.to_string(),
            maker: format!("{:?}", self.maker_address),
            signer: format!("{:?}", self.signer_address),
            taker: format!("{:?}", Address::zero()),
            token_id: token_id.to_string(),
            maker_amount: maker_amount.to_string(),
            taker_amount: taker_amount.to_string(),
            expiration: expiration.to_string(),
            nonce: "0".to_string(),
            fee_rate_bps: fee_rate_bps.to_string(),
            side: side_str.to_string(),
            signature_type: self.config.signature_type.to_string(),
            signature: format!("{signature}"),
        })
    }

    // ─── REST API Methods ─────────────────────────────────────────────────────

    /// GET /book?token_id=<id> — Fetch REST orderbook snapshot.
    pub async fn get_order_book(&self, token_id: &str) -> Result<OrderBook> {
        let path = format!("/book?token_id={token_id}");
        let resp = self
            .http
            .get(&format!("{CLOB_BASE}{path}"))
            .send()
            .await?;

        if resp.status() == 404 {
            return Ok(OrderBook::default());
        }

        let mut book: OrderBook = resp
            .json()
            .await
            .context("Failed to parse orderbook")?;
        book.sort();
        Ok(book)
    }

    /// POST /order — Place a single signed order.
    pub async fn post_order(
        &self,
        order: &SignedOrder,
        time_in_force: &str,
    ) -> Result<OrderResponse> {
        let body = json!({
            "order": order,
            "owner": format!("{:?}", self.maker_address),
            "orderType": time_in_force,
        });

        let resp = self.authenticated_post("/order", &body).await?;
        let status = resp.status();
        let text = resp.text().await?;

        if !status.is_success() {
            if text.contains("not filled") || text.contains("No matching") {
                return Ok(OrderResponse {
                    order_id: String::new(),
                    status: Some("UNMATCHED".to_string()),
                    size_matched: Some("0".to_string()),
                    error_msg: None,
                });
            }
            anyhow::bail!("post_order HTTP {status}: {text}");
        }

        serde_json::from_str(&text).context("Failed to parse order response")
    }

    /// POST /orders — Place multiple orders atomically (batch).
    pub async fn post_orders(
        &self,
        orders: &[&SignedOrder],
        time_in_force: &str,
    ) -> Result<Vec<OrderResponse>> {
        let order_list: Vec<Value> = orders
            .iter()
            .map(|o| {
                json!({
                    "order": o,
                    "owner": format!("{:?}", self.maker_address),
                    "orderType": time_in_force,
                })
            })
            .collect();

        let resp = self.authenticated_post("/orders", &json!(order_list)).await?;
        let status = resp.status();
        let text = resp.text().await?;

        if !status.is_success() {
            anyhow::bail!("post_orders HTTP {status}: {text}");
        }

        serde_json::from_str::<Vec<OrderResponse>>(&text)
            .context("Failed to parse orders response")
    }

    /// DELETE /order — Cancel a single order by ID.
    pub async fn cancel_order(&self, order_id: &str) -> Result<()> {
        let body = json!({ "orderID": order_id });
        let resp = self.authenticated_delete("/order", &body).await?;
        if !resp.status().is_success() {
            let text = resp.text().await?;
            warn!("cancel_order failed: {text}");
        }
        Ok(())
    }

    /// DELETE /orders — Cancel all open orders.
    pub async fn cancel_all_orders(&self) -> Result<()> {
        let body = json!({});
        let resp = self.authenticated_delete("/orders", &body).await?;
        if !resp.status().is_success() {
            let text = resp.text().await?;
            warn!("cancel_all_orders failed: {text}");
        }
        Ok(())
    }

    /// GET /orders?market=<condition_id> — Fetch open orders for a market.
    pub async fn get_open_orders(&self, condition_id: &str) -> Result<Vec<OpenOrder>> {
        let path = format!("/orders?market={condition_id}");
        let resp = self.authenticated_get(&path).await?;
        if resp.status() == 404 {
            return Ok(vec![]);
        }
        let text = resp.text().await?;
        serde_json::from_str(&text).context("Failed to parse open orders")
    }

    /// GET /order/<id> — Fetch a specific order's current state.
    pub async fn get_order(&self, order_id: &str) -> Result<Option<OpenOrder>> {
        let path = format!("/order/{order_id}");
        let resp = self.authenticated_get(&path).await?;
        if resp.status() == 404 {
            return Ok(None);
        }
        let text = resp.text().await?;
        Ok(Some(
            serde_json::from_str(&text).context("Failed to parse order")?,
        ))
    }

    /// GET /balance-allowance — Fetch USDC balance.
    pub async fn get_balance(&self) -> Result<f64> {
        let asset_type = if self.config.uses_proxy() { 1 } else { 0 };
        let path = format!(
            "/balance-allowance?asset_type={asset_type}&signature_type={}",
            self.config.signature_type
        );
        let resp = self.authenticated_get(&path).await?;
        let ba: BalanceAllowance = resp.json().await?;
        Ok(ba.balance_f64())
    }

    /// GET /trades — Fetch historical trade records.
    pub async fn get_trades(&self, condition_id: Option<&str>) -> Result<Vec<TradeRecord>> {
        let mut path = format!(
            "/trades?maker_address={}",
            format!("{:?}", self.maker_address)
        );
        if let Some(cid) = condition_id {
            path.push_str(&format!("&condition_id={cid}"));
        }
        let resp = self.authenticated_get(&path).await?;
        if resp.status() == 404 {
            return Ok(vec![]);
        }
        resp.json().await.context("Failed to parse trades")
    }

    /// GET /markets/<condition_id> — Fetch market's fee rate in bps.
    pub async fn get_market_fee_rate_bps(&self, condition_id: &str) -> Result<u64> {
        let resp = self
            .http
            .get(&format!("{CLOB_BASE}/markets/{condition_id}"))
            .send()
            .await?;
        let v: Value = resp.json().await?;
        let bps = v
            .get("feeRateBps")
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        Ok(bps)
    }

    // ─── Gamma API (Market Discovery) ────────────────────────────────────────

    /// Find the active market for a slug prefix using the Gamma API.
    /// Returns the soonest-ending active market to maximise theta-decay edge.
    pub async fn find_active_market(&self, slug_prefix: &str) -> Result<Option<MarketInfo>> {
        let tag_id: Option<u64> = if slug_prefix.contains("15m") {
            Some(102467)
        } else if slug_prefix.contains("5m") {
            Some(102892)
        } else if slug_prefix.contains("updown") {
            Some(102127)
        } else {
            None
        };

        let mut url = format!(
            "{GAMMA_BASE}/markets?slug_prefix={slug_prefix}&active=true&closed=false&limit=20"
        );
        if let Some(tid) = tag_id {
            url.push_str(&format!("&tag_id={tid}"));
        }

        let resp = self.http.get(&url).send().await?;
        if !resp.status().is_success() {
            return Ok(None);
        }

        let markets: Vec<GammaMarket> = resp
            .json()
            .await
            .context("Failed to parse Gamma markets")?;

        if markets.is_empty() {
            return Ok(None);
        }

        let mut sorted = markets;
        sorted.sort_by(|a, b| {
            a.end_date
                .as_deref()
                .unwrap_or("9999")
                .cmp(b.end_date.as_deref().unwrap_or("9999"))
        });

        for m in sorted {
            let tokens = match &m.tokens {
                Some(t) if t.len() >= 2 => t.clone(),
                _ => continue,
            };

            let yes_token = tokens
                .iter()
                .find(|t| t.outcome.to_uppercase() == "YES")
                .cloned();
            let no_token = tokens
                .iter()
                .find(|t| t.outcome.to_uppercase() == "NO")
                .cloned();

            let (yes, no) = match (yes_token, no_token) {
                (Some(y), Some(n)) => (y, n),
                _ => continue,
            };

            let end_date = m
                .end_date
                .as_deref()
                .unwrap_or("2099-01-01T00:00:00Z")
                .parse::<chrono::DateTime<chrono::Utc>>()
                .unwrap_or_else(|_| chrono::Utc::now() + chrono::Duration::days(365));

            return Ok(Some(MarketInfo {
                condition_id: m.condition_id,
                question: m.question,
                end_date,
                neg_risk: m.neg_risk,
                tick_size: m.tick_size.unwrap_or(0.01),
                tokens: TokenIds {
                    yes: yes.token_id,
                    no: no.token_id,
                },
            }));
        }

        Ok(None)
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

        let rpcs = [
            "https://polygon-rpc.com",
            "https://polygon.llamarpc.com",
            "https://rpc.ankr.com/polygon",
        ];

        for rpc in &rpcs {
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

    /// Call `mergePositions` on the CTF Exchange to burn YES + NO shares → USDC.
    ///
    /// This replicates the TypeScript `mergeBalancedPosition()` call via ethers.
    /// `amount` is in shares (will be converted to 1e6 units).
    /// Returns the transaction hash on success.
    pub async fn merge_positions(
        &self,
        condition_id: &str,
        amount: f64,
        neg_risk: bool,
        rpc_url: &str,
    ) -> Result<H256> {
        use ethers::providers::{Http, Provider};
        use ethers::middleware::SignerMiddleware;
        use ethers::abi::{encode as abi_encode, Token};

        let provider = Provider::<Http>::try_from(rpc_url)?;
        let client = Arc::new(SignerMiddleware::new(provider, self.wallet.clone()));

        // amount in 1e6 units (CTF uses 1e6 precision)
        let amount_u256 = U256::from((amount * 1_000_000.0).floor() as u128);

        // condition_id is a bytes32 hex string
        let condition_bytes: [u8; 32] = {
            let clean = condition_id.trim_start_matches("0x");
            let mut arr = [0u8; 32];
            let decoded = hex::decode(clean).context("Invalid condition_id")?;
            arr[..decoded.len()].copy_from_slice(&decoded);
            arr
        };

        // mergePositions(address collateralToken, bytes32 parentCollectionId, bytes32 conditionId, uint[] partition, uint amount)
        // Polymarket uses a 2-outcome partition: [1, 2]
        let partition = vec![U256::one(), U256::from(2u64)];
        let parent_collection_id = [0u8; 32]; // zero bytes

        // Encode the call manually (ABI encoding of mergePositions)
        let function_selector = &keccak256("mergePositions(address,bytes32,bytes32,uint256[],uint256)")[..4];

        let encoded_params = abi_encode(&[
            Token::Address(USDC_ADDRESS.parse::<Address>().unwrap()),
            Token::FixedBytes(parent_collection_id.to_vec()),
            Token::FixedBytes(condition_bytes.to_vec()),
            Token::Array(partition.into_iter().map(Token::Uint).collect()),
            Token::Uint(amount_u256),
        ]);

        let mut calldata = Vec::with_capacity(4 + encoded_params.len());
        calldata.extend_from_slice(function_selector);
        calldata.extend_from_slice(&encoded_params);

        let contract_addr: Address = if neg_risk {
            NEG_RISK_CTF_EXCHANGE.parse().unwrap()
        } else {
            CTF_EXCHANGE.parse().unwrap()
        };

        let tx = ethers::types::TransactionRequest::new()
            .to(contract_addr)
            .data(calldata)
            .from(self.signer_address);

        let pending = client.send_transaction(tx, None).await?;
        let receipt = pending
            .await?
            .context("mergePositions: transaction dropped")?;

        Ok(receipt.transaction_hash)
    }

    /// Call `redeemPositions` on the Conditional Tokens contract to claim USDC from
    /// a resolved market.  `index_sets` is typically `[1, 2]` for a 2-outcome market.
    pub async fn redeem_positions(
        &self,
        condition_id: &str,
        index_sets: &[U256],
        rpc_url: &str,
    ) -> Result<H256> {
        use ethers::providers::{Http, Provider};
        use ethers::middleware::SignerMiddleware;
        use ethers::abi::{encode as abi_encode, Token};

        let provider = Provider::<Http>::try_from(rpc_url)?;
        let client = Arc::new(SignerMiddleware::new(provider, self.wallet.clone()));

        let condition_bytes: [u8; 32] = {
            let clean = condition_id.trim_start_matches("0x");
            let mut arr = [0u8; 32];
            let decoded = hex::decode(clean).context("Invalid condition_id")?;
            arr[..decoded.len()].copy_from_slice(&decoded);
            arr
        };

        let parent_collection_id = [0u8; 32];

        // redeemPositions(address collateralToken, bytes32 parentCollectionId, bytes32 conditionId, uint256[] indexSets)
        let function_selector =
            &keccak256("redeemPositions(address,bytes32,bytes32,uint256[])")[..4];

        let encoded_params = abi_encode(&[
            Token::Address(USDC_ADDRESS.parse::<Address>().unwrap()),
            Token::FixedBytes(parent_collection_id.to_vec()),
            Token::FixedBytes(condition_bytes.to_vec()),
            Token::Array(index_sets.iter().cloned().map(Token::Uint).collect()),
        ]);

        let mut calldata = Vec::with_capacity(4 + encoded_params.len());
        calldata.extend_from_slice(function_selector);
        calldata.extend_from_slice(&encoded_params);

        let ctf_addr: Address = CONDITIONAL_TOKENS.parse().unwrap();

        let tx = ethers::types::TransactionRequest::new()
            .to(ctf_addr)
            .data(calldata)
            .from(self.signer_address);

        let pending = client.send_transaction(tx, None).await?;
        let receipt = pending
            .await?
            .context("redeemPositions: transaction dropped")?;

        Ok(receipt.transaction_hash)
    }

    /// Check on-chain CTF token balance for a given token ID and holder address.
    pub async fn get_ctf_balance(
        &self,
        token_id: &str,
        holder: Address,
        rpc_url: &str,
    ) -> Result<f64> {
        use ethers::providers::{Http, Provider};
        use ethers::abi::{encode as abi_encode, Token};

        let provider = Provider::<Http>::try_from(rpc_url)?;

        // balanceOf(address account, uint256 id)
        let function_selector = &keccak256("balanceOf(address,uint256)")[..4];
        let token_id_u256 = parse_token_id(token_id)?;

        let encoded = abi_encode(&[
            Token::Address(holder),
            Token::Uint(token_id_u256),
        ]);
        let mut calldata = Vec::with_capacity(4 + encoded.len());
        calldata.extend_from_slice(function_selector);
        calldata.extend_from_slice(&encoded);

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
        let raw = u128::from_str_radix(if clean.is_empty() { "0" } else { clean }, 16)
            .unwrap_or(0);

        Ok(raw as f64 / 1_000_000.0)
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Parse a token ID that might be decimal or hex.
fn parse_token_id(token_id: &str) -> Result<U256> {
    if let Ok(v) = U256::from_dec_str(token_id) {
        return Ok(v);
    }
    let clean = token_id.trim_start_matches("0x");
    U256::from_str_radix(clean, 16).context("Invalid token_id (not decimal or hex)")
}
