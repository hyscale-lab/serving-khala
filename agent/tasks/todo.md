# Khala Integration Review Task Log

## Goal and acceptance criteria

- [x] Restate goal + acceptance criteria
  - Goal: implement documentation plan for Khala integration review across baseline/intermediate/newest commit points.
  - Acceptance:
    - `docs/khala-integration.md` exists and includes architecture evolution, current flow, breaker analysis, findings, and remediation roadmap.
    - `agent/tasks/todo.md` and `agent/tasks/lessons.md` exist with auditable progress and lessons.
    - Verification evidence is captured with concrete command outcomes.

## Checklist

- [x] Locate existing implementation / patterns
- [x] Design: minimal approach + key decisions
- [x] Implement smallest safe slice
- [x] Add/adjust tests
  - No code changes in this pass; no new tests added.
- [x] Run verification (lint/tests/build/manual repro)
- [x] Summarize changes + verification story
- [x] Record lessons (if any)

## Working notes

- Scope is documentation-only in this pass.
- Commit comparison centered on:
  - `59626f893fb731540cef2658eb093bb23d9c6485`
  - `1999676af52b74edad15d679591be86773477719`
  - `76656d477650d49ba3739813e1d7ee222114d4a4`
- Confirmed active regressions relevant to findings:
  - Autoscaler stats send path disabled in activator.
  - Hard `InClusterConfig` dependency in throttler bootstrap.
- Locked defaults from plan:
  - safety-first create pacing,
  - fail-closed on invalid/missing `khala/max-scale`,
  - autoscaler bypass treated as temporary but explicit architecture.
- Decision refinement (2026-02-25, follow-up):
  - keep request queueing in activator,
  - do not start `khalaBreaker` at `maxScale`,
  - use warm-started breaker initial capacity (`min(initialScale + 1, maxScale)`),
  - cap concurrent VM creates at `3` per node by default (tunable `2-5` per node).
- Breaker model refinement (implemented):
  - replaced one-time ramp to `maxScale` with dynamic `UpdateConcurrency` based on VM lifecycle signals.
- Phase 4 scale validation decision (implemented):
  - `khala/max-scale` is required and fail-closed.
  - `khala/initial-scale > khala/max-scale` is clamped with explicit warning.

## Verification log

- [x] `go test -v ./pkg/activator/net -run TestPodAssignmentFinite -count=1`
  - Result: **fail**
  - Key output: fatal `InClusterConfig` missing `KUBERNETES_SERVICE_HOST` and `KUBERNETES_SERVICE_PORT`.

- [x] `go test ./pkg/activator -run TestReportStats -count=1`
  - Result: **fail**
  - Key output: `TestReportStats` timed out waiting for stat messages.

- [x] `go test ./pkg/activator/net -run TestThrottlerTry -count=1`
  - Result: **pass with no matching tests**
  - Key output: `ok ... [no tests to run]`.

## Results

- Added review document: `docs/khala-integration.md`.
- Updated roadmap with concrete breaker/create-concurrency policy.
- Added auditable task record: `agent/tasks/todo.md`.
- Added lessons record: `agent/tasks/lessons.md`.
- No production Go files were modified in this pass.

---

# New TODO - Khala Stability and Correctness Fixes

## Goal and acceptance criteria

- [ ] Restate goal + acceptance criteria
  - Goal: implement production-safe Khala activation behavior under burst load while preserving request correctness and explicit architecture contracts.
  - Done when:
    - burst traffic does not trigger unbounded VM creation,
    - request timeout does not cancel VM creation,
    - counter/accounting drift risks are closed,
    - autoscaler bypass is explicit/config-gated,
    - missing/invalid `khala/max-scale` fails closed with clear signal,
    - non-cluster tests no longer fail due to hard in-cluster dependency.

## Phase 1 - Breaker and VM creation control

- [x] Set safer khala breaker startup policy in `newRevisionThrottler`
  - `MaxConcurrency = maxScale` (hard ceiling).
  - `InitialCapacity = min(initialScale + 1, maxScale)` (warm capacity).
  - No one-time ramp; dynamic updates now control breaker capacity after startup.
  - Do not initialize breaker capacity at full `maxScale` by default.
- [x] Add explicit per-revision VM create-inflight limiter in `VMList`
  - Default create concurrency = 3 per node.
  - Effective per-revision create permits = `createConcurrency * nodeCount`.
  - Tunable range target: 2-5 per node.
  - Keep request queueing path active while create permits are exhausted.
- [x] Add tests for burst behavior
  - Verify max concurrent creates never exceeds configured limit.
  - Verify waiting requests are queued (not dropped) when create slots are full.
- Phase 1 verification (2026-02-25):
  - `GOCACHE=/tmp/go-build go test ./pkg/activator/net/khala -count=1` -> pass
  - `GOCACHE=/tmp/go-build go test ./pkg/activator/net -run TestComputeKhalaInitialCapacity -count=1` -> pass
  - `GOCACHE=/tmp/go-build go test ./pkg/activator/net -run TestInfiniteBreakerCreation -count=1` -> pass

## Phase 2 - Timeout semantics and request behavior

- [x] Decouple VM create lifecycle from request context lifecycle
  - Request timeout may stop waiting request, but in-flight VM creation must continue.
  - Created VM must be returned to pool even if original request timed out.
- [x] Preserve routing correctness under mixed timing
  - Later request should use already-available VM while earlier request is still waiting on create.
- [x] Add tests for timeout/cold-start ordering
  - timed-out request + successful async create + subsequent request reuse.
- Phase 2 verification (2026-02-25):
  - `GOCACHE=/tmp/go-build go test -v ./pkg/activator/net/khala -count=1` -> pass
  - includes:
    - `TestAcquireVMTimeoutDoesNotCancelCreate`
    - `TestAcquireVMMixedOrderingReq1WaitsReq2Req3UseAvailable`

## Phase 2.5 - Breaker capacity tracking

- [x] Replace one-time breaker ramp with dynamic `UpdateConcurrency`
  - Update breaker capacity when effective VM capacity changes (create success/failure rollback, invalidate/remove, LRU cleanup).
  - Implemented model: `capacity = min(maxScale, totalVMs + createInFlight + warmBuffer)`.
- [x] Keep scale-from-zero behavior explicit
  - warm startup (`initialScale + 1`) remains,
  - warm buffer keeps admission at >=1 when `maxScale > 0`.
- [x] Add tests for dynamic breaker behavior
  - capacity increases/decreases with VM lifecycle events.
  - no static max-capacity jump after first VM.
- Phase 2.5 verification (2026-02-25):
  - `GOCACHE=/tmp/go-build go test -v ./pkg/activator/net/khala -count=1` -> pass
  - includes:
    - `TestDynamicAdmissionCapacityTracksVMLifecycle`

## Phase 3 - Counter correctness and reconciliation

- [x] Harden `NodeVMCount` / `TotalVMCount` update rules
  - Mutate counts only on confirmed orchestrator success for create/delete.
  - Do not decrement counts on delete error or unsuccessful response.
- [x] Add reconciliation mechanism
  - event-driven counter sanity pass on create/remove/invalidate/cleanup transitions.
  - emit warning logs on drift correction.
- [x] Add tests for create/delete error paths
  - simulated RPC failures and partial failures must not corrupt counts.
- Phase 3 verification (2026-02-25):
  - `GOCACHE=/tmp/go-build go test -v ./pkg/activator/net/khala -count=1` -> pass
  - includes:
    - `TestCreateVMErrorDoesNotCorruptCounts`
    - `TestInvalidateVMRemoveFailureDoesNotDecrementCounts`

## Phase 4 - Config and fail-closed validation

- [x] Implement strict scale annotation parsing/validation
  - Missing/invalid `khala/max-scale` must fail closed.
  - Return explicit revision-level error path and clear logs/events.
- [x] Validate scale invariants
  - `maxScale >= 1`
  - `initialScale >= 0`
  - `minScale >= 0`
  - `minScale <= maxScale`
  - `initialScale <= maxScale` (clamp with explicit warning).
- [x] Add tests for invalid and missing annotation scenarios.
- Phase 4 verification (2026-02-25):
  - `GOCACHE=/tmp/go-build go test -v ./pkg/activator/net -run 'TestComputeKhalaInitialCapacity|TestGetRevScaleValidation|TestRevisionThrottlerTryFailClosedOnInvalidKhalaScale|TestInfiniteBreakerCreation' -count=1` -> pass
  - `GOCACHE=/tmp/go-build go test ./pkg/activator/net/khala -count=1` -> pass
  - `GOCACHE=/tmp/go-build go test -v ./pkg/activator/net -run TestThrottlerErrorNoRevision -count=1` -> fail (expected known blocker: `InClusterConfig` fatal path; tracked in Phase 6)

## Phase 5 - Explicit autoscaler bypass contract

- [ ] Replace implicit stat-send comment-out behavior with explicit mode gate
  - config flag/env for bypass mode.
  - log chosen mode at startup.
- [ ] Keep stat reporter behavior testable in both modes
  - bypass mode test: no send expected.
  - enabled mode test: send expected.
- [ ] Document bypass expectations in `docs/khala-integration.md`.
- Phase 5 status (2026-02-25):
  - deferred/skipped by user request.
  - current behavior remains implicit bypass.
  - check: `GOCACHE=/tmp/go-build go test ./pkg/activator -run TestReportStats -count=1` -> fail (expected while deferred).

## Phase 6 - Throttler initialization robustness

- [x] Remove hard `Fatalf` dependency on `InClusterConfig` for node discovery
  - make node source injectable for tests.
  - degrade gracefully (explicit error return/log) instead of process fatal in non-cluster test contexts.
- [x] Add test coverage for non-cluster initialization path.
- Phase 6 verification (2026-02-25):
  - `GOCACHE=/tmp/go-build go test -v ./pkg/activator/net -run TestPodAssignmentFinite -count=1` -> pass
  - `GOCACHE=/tmp/go-build go test -v ./pkg/activator/net -run 'TestNewThrottlerUsesInjectedNodeSource|TestNewThrottlerNodeDiscoveryFailureUsesEmptyNodeList' -count=1` -> pass

## Phase 7 - Observability and operational guardrails

- [x] Add metrics:
  - create attempts / successes / failures
  - create in-flight gauge
  - request wait latency histogram
  - breaker in-flight and queued depth
  - counter drift/reconciliation corrections
- [x] Add structured logs for major transitions:
  - request queued, create started/finished, timeout while waiting, reconciliation correction.
- Phase 7 verification (2026-02-25):
  - `GOCACHE=/tmp/go-build go test -v ./pkg/activator/net/khala -count=1` -> pass
  - `GOCACHE=/tmp/go-build go test -v ./pkg/activator/net -run 'TestComputeKhalaInitialCapacity|TestGetRevScaleValidation|TestRevisionThrottlerTryFailClosedOnInvalidKhalaScale|TestPodAssignmentFinite|TestNewThrottlerUsesInjectedNodeSource|TestNewThrottlerNodeDiscoveryFailureUsesEmptyNodeList' -count=1` -> pass

## Verification plan

- [ ] Unit tests
  - `go test ./pkg/activator/net/...`
  - `go test ./pkg/activator/...`
- [ ] Focused regressions
  - `go test -v ./pkg/activator/net -run TestPodAssignmentFinite -count=1`
  - `go test ./pkg/activator -run TestReportStats -count=1`
  - add and run new VM-path timeout/create concurrency tests.
- [ ] Lint/build
  - run repository-standard checks used by current CI path for modified packages.

## Working assumptions (locked for implementation)

- [ ] Keep request queueing in activator.
- [ ] Use synchronous create for permit holders only; cap create-inflight.
- [ ] Default create cap = 3 per node (operator-tunable, recommended 2-5 per node).
- [ ] Missing/invalid `khala/max-scale` fails closed.
- [ ] Autoscaler bypass remains supported but explicit and reversible.
