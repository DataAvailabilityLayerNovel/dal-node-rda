# RDA-aware DAS Linux Test Context (2026-04-08)

## 1. Environment

- OS: Linux
- Repository: celestiaorg/celestia-node
- Branch used during validation: implement-availability (and later context confirms active branch implement-das)

## 2. Goal of this run

- Re-validate current RDA-aware DAS status on Linux.
- Close/verify items listed in [context.md](context.md), especially integration confidence gaps and build/unit health.

## 3. Commands executed and outcomes

### 3.1 API auth integration gap from Windows

- Command: go test ./api -run TestAuthedRPC -count=1
- Result: PASS

### 3.2 nodebuilder/tests discovery by tag

- Command: go test ./nodebuilder/tests -tags share -list .
  - Result: only TestShareModule exists under share tag.
- Command: go test ./nodebuilder/tests -tags p2p -list .
  - Result: found RDA/discovery tests:
    - TestRDA_GridAutoSize
    - TestRDA_GridDistribution_MockNet
    - TestRDA_BridgeNode_Service
    - TestRDA_Discovery_BootstrapAndRendezvous
    - TestRDA_Discovery_DHT_PeerFinding
    - plus baseline discovery tests
- Command: go test ./nodebuilder/tests -tags reconstruction -list .
  - Result: found reconstruction tests:
    - TestFullReconstructFromBridge
    - TestFullReconstructFromFulls
    - TestFullReconstructFromLights

### 3.3 Integration execution

- Command: go test ./nodebuilder/tests -tags p2p -run 'TestBridgeNodeAsBootstrapper|TestFullDiscoveryViaBootstrapper|TestRestartNodeDiscovery|TestRDA_GridAutoSize|TestRDA_GridDistribution_MockNet|TestRDA_BridgeNode_Service|TestRDA_Discovery_BootstrapAndRendezvous|TestRDA_Discovery_DHT_PeerFinding' -count=1
- Result: PASS

- Command: go test ./nodebuilder/tests -tags reconstruction -run 'TestFullReconstructFromBridge|TestFullReconstructFromFulls|TestFullReconstructFromLights' -count=1
- Result: PASS

- Command: go test ./nodebuilder/tests -tags share -run '^TestShareModule$' -count=1
- Result: PASS

## 4. Important note about build tags

- [nodebuilder/tests/share_test.go](nodebuilder/tests/share_test.go#L1) has build constraint: share || integration.
- Running without tag (example: go test -run '^TestShareModule$' ./nodebuilder/tests) gives:
  - PASS [no tests to run]
- This is a false positive because the file is excluded unless -tags share (or -tags integration) is set.

## 5. TestShareModule status after stabilization

- Test now passes on Linux with the stabilized test harness timeout configuration.
- Build-tag note remains important: run with `-tags share` (or `-tags integration`) to include [nodebuilder/tests/share_test.go](nodebuilder/tests/share_test.go#L1).

## 6. Build and unit suite status

- Command: make build
- Result: PASS (explicit BUILD_OK confirmation)

- Command: make test-unit
- Result: PASS

## 7. Current confidence snapshot (Linux)

- API auth gap: cleared (PASS).
- RDA/discovery integration (p2p): PASS.
- Reconstruction integration: PASS.
- share-tag integration (`TestShareModule`): PASS.
- Build and unit confidence: PASS in current Linux rerun.

## 8. Recommended immediate next steps

1. Update checklist state in [RDA_aware_sampilng.md](RDA_aware_sampilng.md) for Linux integration reruns that are now complete.
2. Consider running optional full confidence sweep:
   - make build
   - make test-unit
   - optional: go test ./... -count=1
3. Continue with remaining rollout/operational gates (Sections 13 and 15) once integration evidence is accepted.
