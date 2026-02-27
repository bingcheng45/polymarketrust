use anyhow::Result;
use polymarketrust::clob_client::ClobClient;
use polymarketrust::config::Config;
use polymarketrust::types::GammaMarket;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let config = Arc::new(Config::load()?);
    let client = Arc::new(ClobClient::new(Arc::clone(&config)).await?);
    let maker = client.maker_address();
    let signer = client.signer_address();
    let cash_balance = client.get_balance().await.unwrap_or(0.0);

    println!("Signer: {}", signer);
    println!("Maker : {}", maker);
    println!("Cash  : ${:.6}", cash_balance);

    let redeemables = client.get_redeemable_conditions().await?;
    let total_claimable: f64 = redeemables.iter().map(|x| x.2).sum();
    println!("Claimable conditions: {}", redeemables.len());
    println!("Claimable total     : ${:.4}", total_claimable);
    for (cid, title, amount) in redeemables.iter().take(10) {
        println!("  ${:.4} | {} | {}", amount, title, cid);
    }

    let trades = client.get_trades(None).await?;
    println!("Trades fetched: {}", trades.len());

    let mut uniq_assets = BTreeSet::new();
    for t in &trades {
        uniq_assets.insert(t.token_id.clone());
    }
    println!("Unique assets from trades: {}", uniq_assets.len());

    let mut balances = BTreeMap::new();
    let mut signer_balances = BTreeMap::new();
    let rpc = polymarketrust::clob_client::POLYGON_RPCS[0];
    for token_id in &uniq_assets {
        let maker_bal = client
            .get_ctf_balance(token_id, maker, rpc)
            .await
            .unwrap_or(0.0);
        if maker_bal > 0.0 {
            balances.insert(token_id.clone(), maker_bal);
        }
        let signer_bal = client
            .get_ctf_balance(token_id, signer, rpc)
            .await
            .unwrap_or(0.0);
        if signer_bal > 0.0 {
            signer_balances.insert(token_id.clone(), signer_bal);
        }
    }

    println!("Assets with on-chain CTF balance > 0: {}", balances.len());
    for (token_id, bal) in &balances {
        println!("  token={} balance={:.6}", token_id, bal);
    }
    println!(
        "Signer assets with on-chain CTF balance > 0: {}",
        signer_balances.len()
    );
    for (token_id, bal) in &signer_balances {
        println!("  signer token={} balance={:.6}", token_id, bal);
    }

    let http = reqwest::Client::new();
    let probe_tokens: Vec<String> = if balances.is_empty() {
        uniq_assets.iter().take(3).cloned().collect()
    } else {
        balances.keys().cloned().collect()
    };

    for token_id in &probe_tokens {
        let snake = format!(
            "https://gamma-api.polymarket.com/markets?clob_token_ids={}",
            token_id
        );
        let camel = format!(
            "https://gamma-api.polymarket.com/markets?clobTokenIds={}",
            token_id
        );

        let snake_resp = http.get(&snake).send().await?;
        let snake_status = snake_resp.status();
        let snake_text = snake_resp.text().await?;
        println!(
            "\n[snake] {} -> status={} bytes={}",
            token_id,
            snake_status,
            snake_text.len()
        );
        if let Ok(markets) = serde_json::from_str::<Vec<GammaMarket>>(&snake_text) {
            println!("  parsed markets: {}", markets.len());
            for m in markets.iter().take(3) {
                println!(
                    "  condition={} closed={:?} question={}",
                    m.condition_id,
                    m.closed,
                    m.question
                );
            }
        } else {
            println!(
                "  parse failed (first 200): {}",
                snake_text.chars().take(200).collect::<String>()
            );
        }

        let camel_resp = http.get(&camel).send().await?;
        let camel_status = camel_resp.status();
        let camel_text = camel_resp.text().await?;
        println!(
            "[camel] {} -> status={} bytes={}",
            token_id,
            camel_status,
            camel_text.len()
        );
        if let Ok(markets) = serde_json::from_str::<Vec<GammaMarket>>(&camel_text) {
            println!("  parsed markets: {}", markets.len());
            for m in markets.iter().take(3) {
                println!(
                    "  condition={} closed={:?} question={}",
                    m.condition_id,
                    m.closed,
                    m.question
                );
            }
        } else {
            println!(
                "  parse failed (first 200): {}",
                camel_text.chars().take(200).collect::<String>()
            );
        }
    }

    Ok(())
}
