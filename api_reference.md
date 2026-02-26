# Polymarket API Reference (Rust Context)

This document provides a high-level reference for interacting with Polymarket's APIs from a Rust environment, based on the official Polymarket documentation.

## Core APIs

Polymarket architecture is separated into three primary API domains:

1. **Gamma API (Market Data)**
   - **URL:** `https://gamma-api.polymarket.com`
   - **Purpose:** Used to query market metadata, resolution statuses, event schedules, etc.
   - **Usage:** Standard public REST endpoints. No authentication is typically required for public market data.

2. **Data API**
   - **URL:** `https://data-api.polymarket.com`
   - **Purpose:** General analytical and historical data.

3. **CLOB API (Central Limit Order Book)**
   - **URL:** `https://clob.polymarket.com`
   - **Purpose:** Placing, canceling, and managing orders. Querying order books and user balances. 
   - **Authentication:** Strict Level-1/Level-2 signatures are required. All trading operations must be signed cryptographically.

---

## SDKs and Client Libraries

[Official Docs on Clients & SDKs](https://docs.polymarket.com/api-reference/clients-sdks)

Polymarket officially maintains open-source clients.

### 1. Official Rust SDK (`rs-clob-client`)
- **Repository:** [github.com/Polymarket/rs-clob-client](https://github.com/Polymarket/rs-clob-client)
- **Status:** Official library provided by the Polymarket team for interacting with the CLOB API in Rust. It handles the complex L1/L2 cryptographic signing and REST/WebSocket implementations out-of-the-box.
- **Recommendation:** Recommended for new projects to avoid maintaining manual EIP-712 signature generation logic.

### 2. Custom Implementation (Current Bot Architecture)
In this project (`polymarketrust`), the official SDK is **not** used. Instead, the bot interacts with the APIs directly:
- **HTTP/REST:** `reqwest` is used to send HTTP payloads to the Gamma and CLOB APIs.
- **WebSocket:** `tokio-tungstenite` is used directly to track live order book updates.
- **Cryptography & Signing:** The `ethers-rs` library along with `hmac`/`sha2` are leveraged manually to generate EIP-712 signatures (`POLY_SIGNATURE`), timestamps, and headers required by the CLOB API endpoints. 

### 3. Builder & Relayer SDKs
Polymarket offers Relayer SDKs to execute gasless meta-transactions (e.g., merging positions or proxy wallet redemptions). Currently, these are primarily maintained for TypeScript/Python:
- `@polymarket/builder-signing-sdk`
- `@polymarket/builder-relayer-client`

*Since the relayer clients are officially TS/Python, Rust implementations generally interact with Polymarket's relayer API (`https://relayer.polymarket.com`) via explicit programmatic HTTP endpoints or via direct Polygon Mainnet smart contract interactions (`safe.execTransaction()`).*

---

## Complete API Schemas & OpenAPIs

The underlying Polymarket APIs are extensively documented via OpenAPI and AsyncAPI specifications. If you plan to generate Rust boilerplate models, you can use these OpenAPI specs directly (e.g., with `openapi-generator-cli` or similar Rust OpenAPI generation tools):

- **CLOB API Spec:** [clob-openapi.yaml](https://docs.polymarket.com/api-spec/clob-openapi.yaml)
- **Gamma API Spec:** [gamma-openapi.yaml](https://docs.polymarket.com/api-spec/gamma-openapi.yaml)
- **Data API Spec:** [data-openapi.yaml](https://docs.polymarket.com/api-spec/data-openapi.yaml)
- **Bridge API Spec:** [bridge-openapi.yaml](https://docs.polymarket.com/api-spec/bridge-openapi.yaml)

### AsyncAPI (Websockets)
Polymarket uses WebSockets for real-time order books, trades, and lifecycle updates.

- **WebSocket Connection Docs:** [connect-wss.json](https://docs.polymarket.com/developers/open-api/connect-wss.json)
- **General WS Spec:** [asyncapi.json](https://docs.polymarket.com/asyncapi.json)
- **User Authorized WS Spec:** [asyncapi-user.json](https://docs.polymarket.com/asyncapi-user.json)
- **Sports Data WS Spec:** [asyncapi-sports.json](https://docs.polymarket.com/asyncapi-sports.json)

---

## Notable Rust Endpoints References

If you are expanding the current bot's raw REST/WS implementations, here is the index mapping:

### Market Data (No Auth)
*   **Get Order Book:** `GET /book` (docs: [get-order-book](https://docs.polymarket.com/api-reference/market-data/get-order-book))
*   **Get Midpoint/Spread:** Evaluated using the order book or specific data APIs. 
*   **Get Markets (Gamma):** `GET /markets` (docs: [list-markets](https://docs.polymarket.com/api-reference/markets/list-markets))

### Trading Operations (L2 Authentication Required)
All endpoints here require the signature headers (`POLY-SIGNATURE`, `POLY-TIMESTAMP`, `POLY-NONCE`, etc.):
*   **Create Order:** `POST /order` (docs: [create-order](https://docs.polymarket.com/trading/orders/create))
*   **Cancel Single Order:** `DELETE /order` (docs: [cancel-single-order](https://docs.polymarket.com/api-reference/trade/cancel-single-order))
*   **Cancel All:** `DELETE /orders` (docs: [cancel-all-orders](https://docs.polymarket.com/api-reference/trade/cancel-all-orders))
*   **Get Open Orders:** `GET /orders` (docs: [get-user-orders](https://docs.polymarket.com/api-reference/trade/get-user-orders))

### Real-Time Sockets (`tokio-tungstenite`)
*   **Market Channel:** Subscribe to `market` for orderbook data (docs: [Market Channel](https://docs.polymarket.com/api-reference/wss/market))
*   **User Channel:** Subscribe to `user` for authenticated drops regarding open orders matching (docs: [User Channel](https://docs.polymarket.com/api-reference/wss/user))

---

## Full Documentation Index (from `llms.txt`)

Below is the complete index of available Polymarket documentation pages, useful for exploring deeper into specific topics:

### Core Concepts & Markets
- [Negative Risk Markets](https://docs.polymarket.com/advanced/neg-risk)
- [Markets & Events Categories](https://docs.polymarket.com/concepts/markets-events)
- [Order Lifecycle](https://docs.polymarket.com/concepts/order-lifecycle)
- [Positions & Tokens](https://docs.polymarket.com/concepts/positions-tokens)
- [Prices & Orderbook](https://docs.polymarket.com/concepts/prices-orderbook)
- [Resolution](https://docs.polymarket.com/concepts/resolution)
- [Polymarket 101](https://docs.polymarket.com/polymarket-101)

### Trading & Orders
- [Overview](https://docs.polymarket.com/trading/overview)
- [Create Order](https://docs.polymarket.com/trading/orders/create)
- [Cancel Order](https://docs.polymarket.com/trading/orders/cancel)
- [Order Attribution](https://docs.polymarket.com/trading/orders/attribution)
- [Fees](https://docs.polymarket.com/trading/fees)
- [Gasless Transactions](https://docs.polymarket.com/trading/gasless)
- [Matching Engine Restarts](https://docs.polymarket.com/trading/matching-engine)

### Market Data
- [Fetching Markets](https://docs.polymarket.com/market-data/fetching-markets)
- [Subgraph](https://docs.polymarket.com/market-data/subgraph)
- [Get midpoint price](https://docs.polymarket.com/api-reference/data/get-midpoint-price)
- [Get server time](https://docs.polymarket.com/api-reference/data/get-server-time)
- [Get fee rate](https://docs.polymarket.com/api-reference/market-data/get-fee-rate)
- [Get spread](https://docs.polymarket.com/api-reference/market-data/get-spread)
- [Get tick size](https://docs.polymarket.com/api-reference/market-data/get-tick-size)

### Conditional Token Framework (CTF)
- [CTF Overview](https://docs.polymarket.com/trading/ctf/overview)
- [Merge Tokens](https://docs.polymarket.com/trading/ctf/merge)
- [Redeem Tokens](https://docs.polymarket.com/trading/ctf/redeem)
- [Split Tokens](https://docs.polymarket.com/trading/ctf/split)

### Bridge & Deposit APIs
- [Deposit Assets](https://docs.polymarket.com/trading/bridge/deposit)
- [Withdraw Assets](https://docs.polymarket.com/trading/bridge/withdraw)
- [Quote Bridge](https://docs.polymarket.com/trading/bridge/quote)
- [Create deposit addresses](https://docs.polymarket.com/api-reference/bridge/create-deposit-addresses)

### Market Makers
- [Market Making Overview](https://docs.polymarket.com/market-makers/overview)
- [Inventory Management](https://docs.polymarket.com/market-makers/inventory)
- [Liquidity Rewards](https://docs.polymarket.com/market-makers/liquidity-rewards)
- [Maker Rebates Program](https://docs.polymarket.com/market-makers/maker-rebates)

### Builders
- [Builder Program](https://docs.polymarket.com/builders/overview)
- [API Keys](https://docs.polymarket.com/builders/api-keys)
- [Tiers](https://docs.polymarket.com/builders/tiers)

### Reference & Resources
- [Authentication](https://docs.polymarket.com/api-reference/authentication)
- [Contract Addresses](https://docs.polymarket.com/resources/contract-addresses)
- [Error Codes](https://docs.polymarket.com/resources/error-codes)
- [Geographic Restrictions](https://docs.polymarket.com/api-reference/geoblock)
- [Rate Limits](https://docs.polymarket.com/api-reference/rate-limits)
