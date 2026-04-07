# RDA-aware DAS Context Handoff (up to 2026-04-07)

## 1. Current branch and repository

- Repository: celestiaorg/celestia-node
- Working branch: implement-das
- Goal: complete RDA-aware DAS migration checklist in RDA_aware_sampilng.md

## 2. What is already completed

### 2.1 DAS and API

- Runtime mode support is wired: classic, rda, hybrid.
- Runtime diagnostics APIs are added and tested:
  - SamplingStats includes rda_definition, mode, fallback_active.
  - RuntimeMode endpoint is added.
  - RDADiagnostics endpoint is added.
- API auth behavior was extended for RDADiagnostics and RuntimeMode, and WaitCatchUp compatibility checks were added in rpc tests.

### 2.2 Checkpoint and state

- Versioned checkpoint schema is implemented.
- Legacy checkpoint loading compatibility is implemented.
- Unsupported checkpoint version reset behavior is implemented.
- Retry state metadata persistence and restore are implemented.

### 2.3 Worker/coordinator and strategy

- Strategy model is in place with fallback behavior.
- Retry classification and fallback policy are unit tested.
- Coordinator fairness and non-recent budget behavior are unit tested.
- Worker unit tests were added for:
  - cancel behavior,
  - retry/failure details capture,
  - outside sampling window skip behavior.

### 2.4 GET and SYNC protocol hardening

- GET request/response identity and proof/payload validation paths are covered.
- SYNC pagination, integrity, dedup, and stale protection paths are covered.

### 2.5 Checklist status (high level)

- Section 12 Unit Tests: all listed items are checked.
- Section 12 Regression Tests: all listed items are checked.
- Section 12 Done Criteria: both items are checked.
- Section 12 Integration Tests: still open.

## 3. Test evidence already executed

- go test ./das -run TestClassifyRetryableDeterministic|TestShouldFallbackPolicy|TestMakeResultContract -count=1
- go test ./share -run TestValidateGetResponse*|TestQueryShare*|TestSyncScanColumnShares*|TestProcessSyncedShares*|TestSyncFromPeer\_ -count=1
- go test ./das -run TestCheckpointStore|TestCheckpointStore_LoadLegacyCheckpointWithoutVersion|TestCheckpointStore_LoadRetryStateWithoutFailedCount|TestCheckpointStore_LoadUnsupportedVersion|TestDASer_CheckpointUnsupportedVersionResets|TestDASer_LegacyCheckpoint_DoesNotCrashNewBinary -count=1
- go test ./das -run TestWorker_CancelStopsWithoutPublishingResult|TestWorker_RetryFailureCapturesFailedDetails|TestWorker_OutsideSamplingWindowIsSkippedNotFailed -count=1
- go test ./das ./blob ./share ./nodebuilder/p2p -count=1
- go test ./das ./share -run RDA|RDADiagnostics|RuntimeMode|SyncFromPeer|ValidateGetResponse|QueryShare -count=1

## 4. Windows-specific limitations observed

- TestAuthedRPC in package api is flaky/failing on Windows due to symlink privilege and temp cleanup file-lock behavior.
- Because of this, integration-level confidence is incomplete on Windows, even though unit/regression suites above are green.

## 5. What to do next on Linux

### 5.1 First priority: clear Windows gaps

- Re-run api auth RPC test on Linux:
  - go test ./api -run TestAuthedRPC -count=1

### 5.2 Integration tests to execute and map back to Section 12

- Light node sampling through RDA GET path:
  - go test ./nodebuilder/tests -tags share -run RDA -count=1
- Bootstrap/subnet discovery plus DAS start synchronization:
  - go test ./nodebuilder/tests -tags p2p -run Subnet|DAS|RDA -count=1
- Recovery scenario:
  - go test ./nodebuilder/tests -tags reconstruction -run RDA|Recovery|Sync -count=1
- Mixed mode scenarios if present:
  - go test ./nodebuilder/tests -tags p2p -run Hybrid|Classic|RDA -count=1

Note: adjust exact tag and test name filters based on available tests in nodebuilder/tests.

### 5.3 Build validation on Linux

- make build
- make test-unit
- Optional full confidence sweep:
  - go test ./... -count=1

## 6. Remaining checklist focus after Linux integration

- Section 12 Integration Tests: expected to be the next block to close.
- Section 13 Rollout and Safety Controls: still open.
- Section 15 Definition of Done and non-functional gates: still open.

## 7. Quick claim status for today

- Can the repo build and run RDA-enabled DAS nodes together at wiring level: yes.
- Is full cross-node integration proof completed in checklist: not yet (pending Integration Tests on Linux).
