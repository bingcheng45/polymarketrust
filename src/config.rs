use anyhow::{Context, Result};
use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub private_key: String,
    pub poly_api_key: String,
    pub poly_api_secret: String,
    pub poly_api_passphrase: String,
    pub poly_proxy_address: Option<String>,
    pub signature_type: u8,
    pub market_slugs: Vec<String>,
    pub max_trade_size: f64,
    pub min_liquidity_size: f64,
    pub min_net_profit_usd: f64,
    pub min_leg_price: f64,
    pub mock_currency: bool,
    pub max_daily_loss_usd: f64,
    pub max_consecutive_failures: u32,
    pub circuit_breaker_cooldown_ms: u64,
    pub ws_enabled: bool,
    pub pre_sign_enabled: bool,
    pub maker_mode_enabled: bool,
    pub maker_spread_ticks: u32,
    pub gtc_taker_timeout_ms: u64,
    pub ws_fill_primary: bool,
    pub ws_fill_fallback_poll_ms: u64,
    pub adaptive_throttle_min_ms: u64,
    pub adaptive_throttle_burst_debounce_ms: u64,
    pub actionable_delta_min_ticks: u32,
    pub shadow_engine_enabled: bool,
    pub shadow_engine_send_orders: bool,
    pub merge_reconcile_interval_secs: u64,
    pub clob_fee_rate: f64,
    pub clob_fee_exponent: f64,
}

impl Config {
    pub fn load() -> Result<Self> {
        dotenv::dotenv().ok();

        let private_key = env::var("PRIVATE_KEY")
            .context("PRIVATE_KEY not set")?
            .trim_start_matches("0x")
            .to_string();

        if private_key.len() != 64 {
            anyhow::bail!("PRIVATE_KEY must be 32 bytes (64 hex chars)");
        }
        hex::decode(&private_key).context("PRIVATE_KEY is not valid hex")?;

        let poly_api_key = env::var("POLY_API_KEY").unwrap_or_default();
        let poly_api_secret = env::var("POLY_API_SECRET").unwrap_or_default();
        let poly_api_passphrase = env::var("POLY_API_PASSPHRASE").unwrap_or_default();

        let poly_proxy_address = env::var("POLY_PROXY_ADDRESS").ok().filter(|s| !s.is_empty());

        let signature_type: u8 = env::var("SIGNATURE_TYPE")
            .unwrap_or_else(|_| "0".to_string())
            .parse()
            .context("SIGNATURE_TYPE must be 0, 1, or 2")?;

        if signature_type > 2 {
            anyhow::bail!("SIGNATURE_TYPE must be 0 (EOA), 1 (Poly Proxy), or 2 (Gnosis Safe)");
        }

        if signature_type >= 1 && poly_proxy_address.is_none() {
            anyhow::bail!("POLY_PROXY_ADDRESS is required when SIGNATURE_TYPE >= 1");
        }

        let market_slug_str =
            env::var("MARKET_SLUG").unwrap_or_else(|_| "btc-updown-15m".to_string());
        let market_slugs: Vec<String> = market_slug_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if market_slugs.is_empty() {
            anyhow::bail!("MARKET_SLUG must not be empty");
        }

        Ok(Self {
            private_key,
            poly_api_key,
            poly_api_secret,
            poly_api_passphrase,
            poly_proxy_address,
            signature_type,
            market_slugs,
            max_trade_size: env_f64("MAX_TRADE_SIZE", 50.0)?,
            min_liquidity_size: env_f64("MIN_LIQUIDITY_SIZE", 10.0)?,
            min_net_profit_usd: env_f64("MIN_NET_PROFIT_USD", 0.05)?,
            min_leg_price: env_f64("MIN_LEG_PRICE", 0.10)?,
            mock_currency: env::var("MOCK_CURRENCY")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),
            max_daily_loss_usd: env_f64("MAX_DAILY_LOSS_USD", 10.0)?,
            max_consecutive_failures: env::var("MAX_CONSECUTIVE_FAILURES")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
            circuit_breaker_cooldown_ms: env::var("CIRCUIT_BREAKER_COOLDOWN_MS")
                .unwrap_or_else(|_| "300000".to_string())
                .parse()
                .unwrap_or(300_000),
            ws_enabled: env::var("WS_ENABLED")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            pre_sign_enabled: env::var("PRE_SIGN_ENABLED")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            maker_mode_enabled: env::var("MAKER_MODE_ENABLED")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),
            maker_spread_ticks: env::var("MAKER_SPREAD_TICKS")
                .unwrap_or_else(|_| "2".to_string())
                .parse()
                .unwrap_or(2),
            gtc_taker_timeout_ms: env::var("GTC_TAKER_TIMEOUT_MS")
                .unwrap_or_else(|_| "5000".to_string())
                .parse()
                .unwrap_or(5000),
            ws_fill_primary: env::var("WS_FILL_PRIMARY")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            ws_fill_fallback_poll_ms: env::var("WS_FILL_FALLBACK_POLL_MS")
                .unwrap_or_else(|_| "300".to_string())
                .parse()
                .unwrap_or(300),
            adaptive_throttle_min_ms: env::var("ADAPTIVE_THROTTLE_MIN_MS")
                .unwrap_or_else(|_| "0".to_string())
                .parse()
                .unwrap_or(0),
            adaptive_throttle_burst_debounce_ms: env::var("ADAPTIVE_THROTTLE_BURST_DEBOUNCE_MS")
                .unwrap_or_else(|_| "8".to_string())
                .parse()
                .unwrap_or(8),
            actionable_delta_min_ticks: env::var("ACTIONABLE_DELTA_MIN_TICKS")
                .unwrap_or_else(|_| "1".to_string())
                .parse()
                .unwrap_or(1),
            shadow_engine_enabled: env::var("SHADOW_ENGINE_ENABLED")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            shadow_engine_send_orders: env::var("SHADOW_ENGINE_SEND_ORDERS")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),
            merge_reconcile_interval_secs: env::var("MERGE_RECONCILE_INTERVAL_SECS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
            clob_fee_rate: env_f64("CLOB_FEE_RATE", 0.25)?,
            clob_fee_exponent: env_f64("CLOB_FEE_EXPONENT", 2.0)?,
        })
    }

    /// Whether this config uses a proxy wallet.
    pub fn uses_proxy(&self) -> bool {
        self.signature_type >= 1
    }
}

fn env_f64(key: &str, default: f64) -> Result<f64> {
    match env::var(key) {
        Ok(v) => v
            .parse::<f64>()
            .with_context(|| format!("{key} must be a number")),
        Err(_) => Ok(default),
    }
}
