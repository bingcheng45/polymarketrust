# Arb Upgrade Tracker

## Scope
HFT Capture Reliability Upgrade Program for `polymarketrust`.

## Milestone Checklist
- [x] Phase 0: tracker created and baseline metrics scaffolded
- [x] Phase 1: WS fill engine primary, polling fallback only
- [x] Phase 2: adaptive triggering and lock-contention reductions
- [x] Phase 3: batch engine hardening and deterministic pair timeout states
- [x] Phase 4: depth-aware hedge/sellback model
- [x] Phase 5: merge reconciliation with reserve accounting
- [x] Phase 6: shadow rollout gates and controlled cutover

## KPI Table
| KPI | Baseline | Target | Current | Gate |
|---|---:|---:|---:|---|
| p95 detect->submit (ms) | TBD | <60 | TBD | Open |
| opportunity->post ratio | TBD | increase | TBD | Open |
| post->paired ratio | TBD | increase | TBD | Open |
| hedge rate | TBD | <= baseline | TBD | Open |
| stale-poll fallback rate | TBD | decrease | TBD | Open |
| merge mismatch incidents/day | TBD | 0 critical | TBD | Open |

## Rollout Gate Status
- Shadow engine enabled by default.
- Order send disabled in shadow by default.
- Cutover gate window: 24h continuous metrics.
- Rollback mode: config flag flip (`SHADOW_ENGINE_SEND_ORDERS=false`).

## Acceptance Gates
1. `p95 detect->submit < 60ms`
2. No critical merge/state mismatches
3. Hedge rate <= baseline
4. Failed executions not increased vs baseline

## Open Risks
- User WS order events may be missing or delayed under exchange load.
- Single monitor mutex can still create contention during heavy market bursts.
- Merge state reconciliation accuracy depends on timely on-chain balance visibility.

## Implementation Notes (Current)
- Event-driven fill tracker keyed by `order_id` now updates from User WS `order` and `trade` events.
- Fallback polling is only used when WS fill stream is stale/missing by configured threshold.
- Adaptive trigger gate uses actionable best-ask/size deltas instead of fixed 20ms throttle.
- Batch posting now applies a harder-to-fill-first score from visible ask-depth.
- Hedge/sellback pre-trade estimates now use live orderbook depth when available.
- Merge flow now reserves size first and finalizes state only on async confirmation result.
