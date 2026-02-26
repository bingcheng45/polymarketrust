//! API key generator (replicates biogen.ts).
//!
//! Derives Polymarket CLOB API credentials from your Ethereum private key.
//!
//! Usage: cargo run --bin biogen

use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::SignerSync;
use sha2::{Digest, Sha256};
use std::str::FromStr;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let private_key = std::env::var("PRIVATE_KEY")
        .context("PRIVATE_KEY not set in .env")?
        .trim_start_matches("0x")
        .to_string();

    let signer = PrivateKeySigner::from_str(&private_key)
        .context("Invalid private key")?;
    let address = signer.address();

    println!("Wallet address : {address}");
    println!("Deriving API credentials…\n");

    let derivation_msg =
        "This message is used to generate a Polymarket CLOB API key. Please sign this message.";

    let signature = signer
        .sign_message_sync(derivation_msg.as_bytes())
        .context("Failed to sign derivation message")?;

    // Convert signature to 65-byte array [r(32) || s(32) || v(1)]
    // alloy stores v as parity (0/1); add 27 to match ethers.js personal_sign convention.
    let mut sig_bytes = signature.as_bytes().to_vec();
    if sig_bytes[64] < 27 {
        sig_bytes[64] += 27;
    }

    let mut hasher = Sha256::new();
    hasher.update(&sig_bytes);
    let hash = hasher.finalize();

    let key = BASE64.encode(&hash[..16]);
    let secret = BASE64.encode(&sig_bytes);
    let passphrase = BASE64.encode(&hash[16..32]);

    println!("Add these to your .env file:\n");
    println!("POLY_API_KEY={key}");
    println!("POLY_API_SECRET={secret}");
    println!("POLY_API_PASSPHRASE={passphrase}");
    println!();
    println!("Note: If the API rejects these, register them at:");
    println!("  https://clob.polymarket.com/auth/derive-api-key");

    Ok(())
}
