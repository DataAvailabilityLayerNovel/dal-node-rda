# RDA-aware DAS: Detailed Implementation Checklist

## RDA Primer for Software Engineers

### What RDA means in this project

RDA here means Robust Distributed Array.

Think of it as a deterministic way to spread data symbols across a peer grid so that:

- Sampling can be done with low-latency local-subnet queries (row/column locality).
- Missing symbols can be recovered by syncing from peers that should own those symbols.
- Every accepted symbol is validated by a predicate (Pred(h, i, x)) instead of trusting transport success alone.

In short, Robust Distributed Array is the data placement and retrieval topology, while DAS is the service that verifies availability using that topology.

### Core model (shared vocabulary)

- h: handle (typically data root / block-linked identifier).
- i: share index.
- x: symbol payload with proof.
- Pred(h, i, x): validation function that must pass before data is considered valid.

Node placement and symbol routing are deterministic:

- PeerID maps to grid coordinates (row, col).
- Symbol index i maps to a target column via Cell(i).
- Query path typically targets peers in the same relevant row/column subnet first.

This determinism is important because it gives predictable retry, load distribution, and recovery behavior.

### Mental model of the protocol flow

1. STORE phase (write/distribute path)

- Producer-side pushes symbols into the RDA topology.
- Receiving nodes validate with Pred(h, i, x) before storing.
- Data is forwarded according to row/column policy to ensure robust placement.

2. GET phase (sampling/read path)

- DAS requests symbols from the RDA grid using (h, i).
- Requester chooses candidate peers deterministically (with retry order).
- Returned symbols are accepted only if Pred(h, i, x) passes.

3. SYNC phase (repair/catchup path)

- New/recovering nodes ask subnet peers for missing symbols.
- Synced symbols are validated, deduplicated, then committed.
- This supports recovery without reverting to expensive full-network scans.

### How this changes DAS behavior

Classic DAS is mostly "sample via availability interface and retry by height".

RDA-aware DAS becomes:

- "sample by symbol/query path" inside each target header.
- Retry policy driven by RDA errors (timeout, peer unavailable, invalid proof).
- Optional fallback to classic path when robust distributed array path is unhealthy.
- Extra state for per-symbol failures (not only per-height failures).

### Non-negotiable invariants

- Never accept a symbol if Pred(h, i, x) fails.
- Never let SYNC overwrite newer valid data with stale data.
- Never start RDA sampling before subnet discovery/lifecycle is ready.
- Always keep a safe fallback path (at least in hybrid mode rollout).

### Common misunderstandings to avoid

- RDA is not a replacement name for DAS. RDA is the topology/protocol; DAS is the sampling service.
- Successful network response is not equivalent to valid data. Predicate verification is mandatory.
- High retry count is not "robustness" by itself. Deterministic retry + bounded policy + fallback is robustness.
- Metrics that are declared but not wired provide no operational confidence.

### Practical engineering guidance

- Keep classic mode intact first, then enable robust distributed array path behind a flag.
- Implement GET correctness before worker orchestration changes.
- Add checkpoint versioning before persisting new per-symbol state.
- Treat observability as a feature, not a follow-up task.

## Terminology (must be explicit)

- [ ] RDA in this document means Robust Distributed Array (not Random Data Availability, not generic redundancy term).
- [ ] Add the same clarification to PR description, design docs, and commit messages.
- [ ] Keep naming consistent in code/comments:
  - [ ] "RDA" for Robust Distributed Array topology/protocol.
  - [ ] "DAS" for Data Availability Sampling service.
  - [ ] "RDA-aware DAS" for DAS that uses Robust Distributed Array protocols for sampling/recovery.

## 0) Goal and Scope

- [ ] Migrate current DAS from header-only sampling flow to RDA-aware sampling flow.
- [ ] Keep backward compatibility: support classic DAS and RDA DAS side-by-side (feature flag).
- [ ] Preserve existing APIs where possible, then extend with RDA-specific observability and controls.
- [ ] Avoid regressions for bridge/light startup, catchup, checkpoint resume, and fraud proof handling.
- [ ] Preserve robust distributed array invariants:
  - [ ] Deterministic peer-to-cell mapping.
  - [ ] Subnet locality (row/column) for query path.
  - [ ] Predicate validation Pred(h, i, x) before accepting symbol.

## 1) Target Architecture Decisions (must finalize first)

- [x] Decide runtime mode strategy:
  - [x] Classic only
  - [x] RDA only
  - [x] Hybrid (recommended: canary and fallback)
- [x] Decide where RDA sampling logic lives:
  - [ ] Inside das worker/coordinator (new RDA strategy layer)
  - [ ] Inside share availability implementation (adapter model)
  - [x] Hybrid (recommended: strategy in das + transport in share/rda protocols)
- [ ] Define fallback policy:
  - [x] RDA query timeout -> retry peer
  - [x] Exceed retries -> fallback to existing light availability path
  - [x] Policy and thresholds configurable
- [ ] Define production readiness criteria:
  - [ ] Correctness (proof validation + data recovery)
  - [ ] Catchup behavior
  - [ ] Performance budget (latency/throughput)
  - [ ] Operability (metrics/logs/debuggability)

### 1.1 RDA (Robust Distributed Array) design decisions

- [x] Define canonical symbol model for robust distributed array flow:
  - [x] h = handle/data root.
  - [x] i = share index.
  - [x] x = symbol payload + proof.
- [ ] Define canonical mapping function for robust distributed array:
  - [ ] Cell(i) -> target column c.
  - [ ] peer(row, c) selection policy (deterministic + retry order).
- [ ] Define robust distributed array proof policy:
  - [ ] required proof fields.
  - [ ] strict verification failures and error codes.
  - [ ] acceptable soft failures (timeouts/temporary peer absence).
- [ ] Define robust distributed array recovery policy:
  - [ ] when to trigger SYNC.
  - [ ] when to trigger fallback to classic path.
  - [ ] how to mark sample as terminal failure.

## 2) Configuration and Feature Flags

### Files

- [x] nodebuilder/das/config.go
- [x] nodebuilder/das/module.go
- [x] nodebuilder/das/constructors.go
- [ ] nodebuilder/share/config.go

### Tasks

- [x] Add DAS mode config (example: das_mode = classic|rda|hybrid).
- [x] Add RDA DAS tuning knobs:
  - [x] rda_query_timeout
  - [x] rda_max_retries
  - [x] rda_parallel_queries
  - [x] rda_fallback_enabled
  - [x] rda_fallback_after_retries
  - [x] rda_sync_on_catchup
  - [x] rda_mode_strict_predicate (fail-fast vs tolerant mode)
  - [x] rda_min_peers_per_subnet
  - [x] rda_sync_batch_size
  - [x] rda_checkpoint_version
- [x] Validate config combinations (invalid combinations fail fast). (implemented for mode validation and initial RDA fallback/retry constraints)
- [ ] Default values:
  - [ ] Safe defaults for migration (hybrid with fallback enabled).
- [x] Plumb config into fx wiring and constructor chain.
- [x] Add config migration notes from old TOML (if fields renamed/split).

### Config Migration Notes

- [x] Existing DAS settings remain backward compatible with classic defaults (`enabled=true`, `mode=classic`).
- [x] New RDA knobs are additive; old TOML without these fields falls back to defaults from `das.DefaultParameters()`.
- [x] `hybrid` mode now requires `rda_fallback_enabled=true`; invalid combinations fail fast at startup.

### Done Criteria

- [x] Node starts with classic mode unchanged.
- [x] Node starts with rda/hybrid mode and wiring succeeds.
- [x] Invalid config combinations return clear error messages.

## 3) DAS Strategy Abstraction Layer

### Files

- [x] das/daser.go
- [ ] das/worker.go
- [ ] das/coordinator.go
- [ ] das/state.go
- [x] das/options.go

### Tasks

- [x] Introduce sampling strategy interface for worker runtime.
- [x] Refactor DASer sample path to delegate to strategy instead of direct da.SharesAvailable only.
- [x] Keep current behavior as ClassicStrategy.
- [x] Add RDAStrategy skeleton (no behavior switch until tests added).
- [x] Add HybridStrategy (RDA first, classic fallback).
- [ ] Ensure strategy has required dependencies:
  - [x] header access
  - [x] rda requester/service adapter
  - [ ] metrics hooks
  - [x] fallback path
  - [x] robust distributed array topology snapshot provider
  - [x] robust distributed array query client
- [x] Keep fraud proof propagation path intact for byzantine errors.

### 3.1 Strategy contracts (explicit)

- [x] Define strategy contract input/output clearly:
  - [x] Input: header + sampling budget + context.
  - [x] Output: success/failure + retryable set + telemetry payload.
- [x] Ensure all strategies produce common result type for coordinator.
- [x] Ensure retry classification is deterministic and testable.

### Done Criteria

- [x] Existing unit tests for classic path still pass.
- [x] RDA strategy can be selected at runtime without panic.

## 4) RDA Service Accessors and Adapters (bridge between DAS and share/rda)

### Files

- [x] share/rda_service.go
- [ ] nodebuilder/share/rda_module.go
- [ ] nodebuilder/node.go

### Tasks

- [x] Expose safe accessor(s) for requester components needed by DAS:
  - [x] get requester
  - [x] sync requester
  - [x] topology info (row/col peers, grid dimensions)
- [x] Define narrow adapter interfaces to avoid tight coupling between das and concrete rda types.
- [x] Inject adapter into das constructor via nodebuilder wiring.
- [ ] Ensure lifecycle readiness order:
  - [x] DAS should not run RDA queries before subnet discovery is ready.
- [x] Add adapter methods required by robust distributed array DAS:
  - [x] QuerySymbol(handle, shareIndex).
  - [x] SyncColumn(sinceHeight).
  - [x] GetTopologySnapshot().
  - [x] GetHealthSnapshot().

### Done Criteria

- [x] DAS can call RDA query adapter in runtime.
- [x] No circular dependency in fx graph.

## 5) GET Protocol Hardening (production-level behavior)

### Files

- [ ] share/rda_get_protocol.go

### Tasks

- [x] Remove simulation shortcuts in handler lookup path.
- [x] Implement real share lookup by handle + share index in storage backend.
- [x] Return real block height metadata (no placeholder).
- [ ] Align GET semantics with robust distributed array contract:
  - [x] Response must include enough data for Pred(h, i, x).
  - [x] Request and response IDs must be traceable end-to-end.
- [ ] Harden response contract:
  - [x] request id integrity
  - [x] payload validation
  - [x] timeout and stream close handling
- [ ] Requester behavior:
  - [x] deterministic target column selection from share index
  - [x] peer selection policy in same subnet
  - [x] retry with backoff
  - [x] proof validation before accept
- [x] Add explicit error taxonomy for requester/handler outcomes.

### 5.1 Error taxonomy for robust distributed array GET

- [x] Define stable errors:
  - [x] ErrRDAQueryTimeout.
  - [x] ErrRDAPeerUnavailable.
  - [x] ErrRDAProofInvalid.
  - [x] ErrRDASymbolNotFound.
  - [x] ErrRDAProtocolDecode.
- [x] Map each error to retry policy (retry/no-retry/fallback).

### Done Criteria

- [x] QueryShare succeeds on valid peer and returns validated symbol.
- [x] QueryShare failure paths are deterministic and observable.

## 6) SYNC Protocol Completion (catchup/recovery support)

### Files

- [ ] share/rda_sync_protocol.go
- [ ] share/rda_blockstore_bridge.go
- [ ] share/rda_blockstore_bridge_integration_test.go

### Tasks

- [x] Implement real storage scan API for column sync.
- [x] Remove placeholder/empty scan behavior.
- [x] Return real block height metadata.
- [x] Validate each synced symbol via predicate before commit.
- [x] Integrate with blockstore bridge path for reconstruction compatibility.
- [x] Add limits and pagination for large sync payloads.
- [x] Add deduplication policy for incoming synced symbols.
- [x] Add integrity checks before storing synced data.

### 6.1 Robust Distributed Array recovery invariants

- [x] SYNC must not bypass Pred(h, i, x) verification.
- [x] SYNC must not overwrite newer valid symbol with older copy.
- [x] SYNC path must emit recovery metrics and audit logs.

### Done Criteria

- [x] New node can sync recent column data after join.
- [x] Synced data available for downstream reconstruction path.

## 7) Worker/Coordinator RDA Sampling Behavior

### Files

- [ ] das/worker.go
- [ ] das/coordinator.go
- [ ] das/state.go

### Tasks

- [ ] Extend worker job execution model for RDA strategy:
  - [x] for each target header, derive RDA sample set
  - [x] execute per-sample query with timeout/retry
  - [x] aggregate pass/fail and classify retryable vs terminal
  - [x] count robust distributed array direct-hit vs fallback-hit
- [ ] State management changes:
  - [x] track failed units with richer context (header, sample index, attempts)
  - [x] preserve compatibility with current checkpoint model or version it
  - [x] include robust distributed array per-symbol failure reason
- [x] Keep recent/catchup priority semantics stable.
- [x] Keep cancellation semantics and graceful shutdown semantics stable.

### 7.1 Coordinator scheduling policy for robust distributed array

- [x] Reserve worker budget for recent jobs to avoid head lag.
- [x] Prevent retry storms per subnet (cap retries per row/col window).
- [x] Add fairness rule so one bad subnet does not starve all jobs.

### Done Criteria

- [ ] Catchup and recent sampling both work under RDA mode.
- [ ] Retry queue converges without starvation.

## 8) Checkpoint and Resume Versioning

### Files

- [ ] das/checkpoint.go
- [ ] das/store.go
- [ ] das/stats.go

### Tasks

- [x] Add checkpoint schema versioning.
- [x] Backward-compatible load for old checkpoints.
- [x] Include RDA-specific in-progress/failure state where needed.
- [x] Ensure force-quit recovery still resumes safely.
- [x] Add migration path or reset logic for incompatible checkpoint versions.
- [x] Persist robust distributed array sampling cursor metadata.
- [x] Persist robust distributed array retry state with attempt timestamps.

### Done Criteria

- [x] Restart in rda/hybrid mode resumes correctly from persisted state.
- [x] Old checkpoint files do not crash new binaries.

## 9) Metrics and Observability

### Files

- [ ] das/metrics.go
- [ ] das/worker.go
- [ ] das/coordinator.go
- [ ] nodebuilder/settings.go

### Tasks

- [ ] Wire currently-declared RDA metrics into real execution path:
  - [x] direct fetch latency
  - [x] recovery count/latency
  - [x] throughput
  - [x] bandwidth savings
- [ ] Add robust distributed array protocol metrics:
  - [x] get_request_total / get_success_total / get_timeout_total
  - [x] sync_request_total / sync_symbols_received_total
  - [x] pred_validation_fail_total
- [x] Add labels for mode (classic/rda/hybrid), outcome, fallback hit.
- [ ] Add dashboard-friendly counters/gauges:
  - [x] rda query success ratio
  - [x] fallback ratio
  - [x] retry depth distribution
- [x] Ensure metrics registration remains safe on disabled modes.
- [x] Ensure cardinality-safe labels (no peer ID label in hot counters).

### Done Criteria

- [x] Metrics show non-zero values during RDA sampling runs.
- [x] No duplicate registration or callback leaks on start/stop.

## 10) RPC/API Extensions for Operations

### Files

- [ ] nodebuilder/das/das.go
- [ ] nodebuilder/das/cmd/\* (if needed)

### Tasks

- [x] Keep existing SamplingStats and WaitCatchUp behavior.
- [ ] Add optional RDA diagnostics endpoint(s):
  - [x] mode
  - [x] query success/fallback stats
  - [x] subnet readiness and peer availability snapshot
- [x] Add explicit field in stats payload: rda_definition = "Robust Distributed Array".
- [x] Add runtime mode endpoint: classic/rda/hybrid + current effective fallback state.
- [x] Document API behavior differences by mode.

### Done Criteria

- [x] Operators can verify RDA mode health without reading raw logs.

## 11) Availability Layer Integration Options

### Files

- [ ] share/availability.go
- [ ] share/availability/light/availability.go
- [ ] share/availability/full/availability.go
- [ ] nodebuilder/share/module.go

### Tasks

- [ ] Decide integration boundary:
  - [ ] Strategy layer only (recommended first)
  - [ ] or add new RDA-aware availability implementation
- [ ] If adding new implementation:
  - [ ] create share/availability/rda package
  - [ ] wire in module selection logic by config
- [ ] Ensure full/bridge behavior stays correct for non-RDA paths.
- [ ] If robust distributed array availability package is created:
  - [ ] define clear boundary with das strategy (no duplicate retry logic).
  - [ ] keep interface compatible with existing share.Availability call sites.

### Done Criteria

- [ ] Light node RDA sampling path is reachable and testable.
- [ ] Existing availability tests for classic path remain green.

## 12) Testing Plan (must be implemented with each phase)

### Unit Tests

- [x] das worker tests for rda query retry/fallback/cancel.
- [x] das coordinator tests for priority and fairness with rda tasks.
- [x] checkpoint tests for versioned load/store.
- [x] get/sync protocol tests for request/response validation.
- [x] robust distributed array predicate failure tests (invalid proof, mismatched index).
- [x] robust distributed array retry classification tests.

### Integration Tests

- [ ] nodebuilder/tests: light node sampling through RDA GET path.
- [ ] bootstrap/subnet discovery + DAS start synchronization.
- [ ] recovery scenario: partial unavailability then recovery via sync/fallback.
- [ ] robust distributed array subnet partition test (row/col isolated then healed).
- [ ] robust distributed array mixed-mode test (some nodes classic, some hybrid).

### Integration gaps observed on Windows (to close on Linux)

- [ ] Re-run api auth RPC integration flow on Linux (`go test ./api -run TestAuthedRPC -count=1`) to avoid Windows symlink privilege and temp cleanup lock issues.
- [ ] Re-run nodebuilder integration suites on Linux for RDA startup/discovery/recovery claims before checking Integration Tests items above.

### Regression Tests

- [x] Classic mode regression suite unchanged.
- [x] Existing p2p/blob/share tests unaffected.
- [x] Existing DAS API behavior remains compatible in classic mode.

### Done Criteria

- [x] New RDA tests pass.
- [x] Existing baseline tests pass.

## 13) Rollout and Safety Controls

- [ ] Start with hybrid mode in testnet/staging.
- [ ] Add kill switch to force classic mode at runtime config level.
- [ ] Define SLO-based rollback triggers:
  - [ ] high fallback ratio
  - [ ] catchup lag increase
  - [ ] failure/retry explosion
- [ ] Write runbook for mode switching and checkpoint handling.
- [ ] Add explicit rollback procedure for robust distributed array mode:
  - [ ] set mode to classic
  - [ ] preserve checkpoint backup
  - [ ] verify catchup recovery in classic mode

## 14) Suggested Execution Order (practical)

- [ ] Phase A: Config + strategy abstraction (no behavior change).
- [ ] Phase B: GET protocol hardening + DAS adapter wiring.
- [ ] Phase C: Worker RDA execution + fallback path.
- [ ] Phase D: SYNC completion + checkpoint extensions.
- [ ] Phase E: Metrics + APIs + docs.
- [ ] Phase F: Full test pass and staged rollout.

## 14.1 Milestone-by-milestone deliverables

- [ ] Milestone 1 (No behavior change): config + strategy interfaces + compile green.
- [ ] Milestone 2 (Protocol correctness): GET/SYNC hardened and unit tested.
- [ ] Milestone 3 (Behavior enable): RDA strategy active behind flag + fallback.
- [ ] Milestone 4 (Stability): checkpoint versioning + metrics + integration tests.
- [ ] Milestone 5 (Operational): rollout guide + dashboards + rollback drill.

## 15) Definition of Done (overall)

- [ ] Classic DAS mode remains stable and backward compatible.
- [ ] RDA mode can sample, retry, fallback, and catch up successfully.
- [ ] Checkpoint resume works for restart scenarios.
- [ ] Metrics and API provide actionable operational insight.
- [ ] Test suites (unit + integration + regression) are green.
- [ ] Operational runbook exists for deployment and rollback.
- [ ] Documentation explicitly states: RDA = Robust Distributed Array.

## 15.1 Non-functional acceptance gates

- [ ] No startup deadlocks between DAS and subnet discovery lifecycle.
- [ ] No goroutine leaks on repeated start/stop.
- [ ] No unbounded retry memory growth.
- [ ] P95 sampling latency in hybrid mode within accepted budget.
- [ ] Fallback ratio remains below target threshold in healthy network.

## 16) Personal Coding Worksheet (optional)

- [ ] Create branch per phase.
- [ ] Commit small slices with tests.
- [ ] After each phase:
  - [ ] run focused tests
  - [ ] run package-level tests
  - [ ] check logs/metrics on local multi-node setup
- [ ] Keep a migration notes file with pitfalls and fixes.

## 17) Per-file actionable TODO map

- [ ] das/daser.go
  - [x] Introduce strategy injection and mode selection.
  - [x] Keep fraud broadcast behavior unchanged.
- [ ] das/worker.go
  - [ ] Add per-symbol execution path for robust distributed array.
  - [ ] Add fallback execution path and result tagging.
- [ ] das/coordinator.go
  - [ ] Add scheduling guards for subnet-related retry storms.
  - [ ] Preserve catchup/recent job guarantees.
- [ ] das/state.go
  - [ ] Add extended retry metadata and versioned state representation.
- [ ] das/checkpoint.go
  - [ ] Introduce schema version + migration logic.
- [ ] das/store.go
  - [ ] Persist/recover extended robust distributed array state safely.
- [ ] das/metrics.go
  - [ ] Wire robust distributed array metrics into runtime code paths.
- [ ] nodebuilder/das/config.go
  - [ ] Add mode and robust distributed array tuning parameters.
- [ ] nodebuilder/das/module.go
  - [ ] Wire strategy/adapters based on mode.
- [ ] nodebuilder/das/constructors.go
  - [ ] Inject robust distributed array adapter dependencies.
- [ ] nodebuilder/share/rda_module.go
  - [ ] Export adapter-friendly providers for DAS.
- [ ] share/rda_service.go
  - [ ] Add requester/topology accessors and health snapshot API.
- [ ] share/rda_get_protocol.go
  - [ ] Replace simulation and placeholders with real data path.
- [ ] share/rda_sync_protocol.go
  - [ ] Implement real column scan + sync payload correctness.
- [ ] share/rda_blockstore_bridge.go
  - [ ] Validate integration with recovery/sync write path.
