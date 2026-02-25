//! Proxy wallet diagnostic tool (replicates check_proxy.ts).
//!
//! Usage: cargo run --bin check_proxy

use anyhow::{Context, Result};
use ethers::core::k256::ecdsa::SigningKey;
use ethers::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let private_key = std::env::var("PRIVATE_KEY")
        .context("PRIVATE_KEY not set")?
        .trim_start_matches("0x")
        .to_string();

    let proxy_address_str = std::env::var("POLY_PROXY_ADDRESS").unwrap_or_default();
    let signature_type: u8 = std::env::var("SIGNATURE_TYPE")
        .unwrap_or_else(|_| "0".to_string())
        .parse()
        .unwrap_or(0);

    let private_key_bytes = hex::decode(&private_key).context("Invalid PRIVATE_KEY")?;
    let signing_key = SigningKey::from_bytes(private_key_bytes.as_slice().into())?;
    let wallet = LocalWallet::from(signing_key);
    let signer_address = wallet.address();

    println!("═══════════════════════════════════════");
    println!("  Polymarket Proxy Wallet Diagnostics");
    println!("═══════════════════════════════════════");
    println!("Signer address : {signer_address:?}");
    println!("Signature type : {signature_type}");

    let check_address = if signature_type >= 1 {
        if proxy_address_str.is_empty() {
            eprintln!("ERROR: POLY_PROXY_ADDRESS is not set but SIGNATURE_TYPE >= 1");
            std::process::exit(1);
        }
        let proxy: Address = proxy_address_str
            .parse()
            .context("Invalid POLY_PROXY_ADDRESS")?;
        println!("Proxy address  : {proxy:?}");
        proxy
    } else {
        signer_address
    };

    // Query USDC balance via eth_call on Polygon
    let usdc_address = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
    let call_data = format!(
        "0x70a08231000000000000000000000000{}",
        hex::encode(check_address.as_bytes())
    );

    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{"to": usdc_address, "data": call_data}, "latest"],
        "id": 1
    });

    let client = reqwest::Client::new();
    match client
        .post("https://polygon-rpc.com")
        .json(&body)
        .send()
        .await
    {
        Ok(resp) => {
            if let Ok(v) = resp.json::<serde_json::Value>().await {
                if let Some(hex_str) = v.get("result").and_then(|x| x.as_str()) {
                    let clean = hex_str.trim_start_matches("0x");
                    if let Ok(balance_raw) = u128::from_str_radix(clean, 16) {
                        let balance_usdc = balance_raw as f64 / 1_000_000.0;
                        println!("USDC balance   : ${balance_usdc:.4}");
                    } else {
                        println!("USDC balance   : (parse error)");
                    }
                }
            }
        }
        Err(e) => println!("Balance check failed: {e}"),
    }

    println!("═══════════════════════════════════════");
    Ok(())
}
