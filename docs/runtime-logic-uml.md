# Runtime Logic UML and Gap Map

This document describes the current execution model and highlights where logic gaps or residual risks still exist.

## 1) Runtime Sequence (WS Hot Path + Background Workers)

```mermaid
sequenceDiagram
    autonumber
    participant WS as Polymarket WS
    participant WSC as ws_client.rs
    participant MAIN as main.rs Scheduler
    participant MON as market_monitor.rs
    participant D as dashboard.rs
    participant CLOB as clob_client.rs
    participant DATA as Polymarket Data API
    participant CHAIN as Polygon / Safe

    WS->>WSC: orderbook / price deltas
    WSC->>MAIN: Notify signal
    MAIN->>MON: check_opportunity()
    MON->>MON: WS cache read + strategy decision
    MON->>D: append tick/action + render
    alt opportunity found
        MON->>CLOB: place/cancel/poll orders
        CLOB->>CHAIN: tx / RPC calls
    end

    par balance worker (2s)
        MAIN->>MON: run_balance_refresh_cycle()
        MON->>CLOB: get_balance()
        CLOB-->>MON: cached balance update
    and gas worker (30s)
        MAIN->>MON: run_gas_refresh_cycle()
        MON->>CLOB: gas + POL price fetch
        CLOB-->>MON: gas cache update
    and claim worker (5s)
        MAIN->>MON: run_claim_cycle()
        MON->>DATA: positions(redeemable=true)
        DATA-->>MON: claimable conditions
        MON->>CLOB: redeem_positions(condition)
        CLOB->>CHAIN: Safe/EOA redemption tx
        MON->>D: claim attempt/success/failure action log
    end
```

## 2) Control Flow (No-Market + Rollover + Stale WS)

```mermaid
flowchart TD
    A["initialize()"] --> B{"active market found?"}
    B -- "no" --> C["wait 5s and retry discovery"]
    C --> B
    B -- "yes" --> D["connect WS + start scheduler loops"]

    D --> E["strategy checks market expiry"]
    E --> F{"expired?"}
    F -- "yes" --> G["cancel maker orders + clear market state"]
    G --> H["discover next market (retry path if none)"]
    H --> I["re-init WS for new tokens"]
    I --> D
    F -- "no" --> J["normal arbitrage decision loop"]

    J --> K["sample WS book age"]
    K --> L{"age > stale threshold?"}
    L -- "yes" --> M["set reconnect requested + log stale warning"]
    M --> N["WS health loop reconnects"]
    L -- "no" --> J
```

## 3) Gap Map (Current)

| Area | Current Behavior | Gap / Risk | Suggested Next Hardening |
|---|---|---|---|
| WS tick continuity | change-only tick rendering | quiet periods can look like pauses even when healthy | optional heartbeat line with latest age/connection status |
| Concurrency model | single `MarketMonitor` mutex shared across loops | long critical sections can still delay loop scheduling | split state into finer-grained locks (`strategy`, `account`, `claims`) |
| Safe tx confirmation | tx hash + receipt driven confirmation | receipt success may not capture full Safe execution semantics | parse Safe execution events and fail on `ExecutionFailure` |
| Claim retries | cooldown + suppression windows | repeated API lag can still create temporary stale pending-claim display | add per-condition state machine with explicit terminal outcomes |

## 4) Operational Checks

- Ensure `logs/session_*.txt` contains explicit `Claim attempt` and `Redemption confirmed/failed` lines.
- Track WS age lines in dashboard:
  - `WS age: Up=<ms> | Down=<ms>`
- Watch periodic perf logs (p50/p95) for:
  - `check_opportunity`
  - `claim cycle`
  - `balance refresh`
