//! API key generator (replicates biogen.ts).
//!
//! Derives Polymarket CLOB API credentials from your Ethereum private key.
//!
//! Usage: cargo run --bin biogen

use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use ethers::core::k256::ecdsa::SigningKey;
use ethers::prelude::*;
use sha2::{Digest, Sha256};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let private_key = std::env::var("PRIVATE_KEY")
        .context("PRIVATE_KEY not set in .env")?
        .trim_start_matches("0x")
        .to_string();

    let private_key_bytes = hex::decode(&private_key).context("Invalid PRIVATE_KEY hex")?;
    let signing_key = SigningKey::from_bytes(private_key_bytes.as_slice().into())
        .context("Invalid private key bytes")?;
    let wallet = LocalWallet::from(signing_key);
    let address = wallet.address();

    println!("Wallet address : {address:?}");
    println!("Deriving API credentials…\n");

    let derivation_msg =
        "This message is used to generate a Polymarket CLOB API key. Please sign this message.";

    let signature = wallet
        .sign_message(derivation_msg)
        .await
        .context("Failed to sign derivation message")?;

    let sig_bytes = signature.to_vec();

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
