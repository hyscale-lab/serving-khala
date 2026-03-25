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

---

# Planning - 2026-03-25 Khala Correctness Follow-up

## Goal and acceptance criteria

- [ ] Restate goal + acceptance criteria
  - Goal: resolve the four reviewed Khala correctness findings with the smallest safe changes, one approval-gated step at a time.
  - Done when:
    - scale-down never removes below `minScale` in a single cleanup pass,
    - `AddVM` responses are validated before VM counters/routing are updated,
    - request/proxy failures only invalidate VMs for explicitly unhealthy cases,
    - equal-load node selection no longer pins to the first node,
    - each change lands with focused regression coverage and explicit verification evidence.

## Task 1 - Cleanup must respect `minScale` across a full scan

- [x] Task 1.1: re-read the cleanup path and write down the exact invariant
  - Scope:
    - inspect `RemoveVMsWithLeastRecentUse`,
    - confirm the current overshoot path,
    - lock the invariant: "a cleanup pass must never schedule removal that would take `TotalVMCount` below `MinScale`."
  - Approval gate:
    - wait for your explicit approval before starting this step.
  - Result (2026-03-25):
    - Overshoot confirmed: the cleanup scan selects idle VMs against an unchanged `TotalVMCount`, then decrements counts later during removal, so one pass can schedule more removals than the `MinScale` floor allows.
    - Locked invariant: in a single cleanup pass, planned removals must never exceed `max(0, TotalVMCount - MinScale)`, and only currently idle VMs in the available pool may be considered removable.

- [x] Task 1.2: implement the minimal cleanup fix
  - Scope:
    - keep the current cleanup structure,
    - make the removal selection account for already-selected removals in the same pass,
    - avoid unrelated refactors or policy changes.
  - Approval gate:
    - wait for your explicit approval before editing code for this step.
  - Result (2026-03-25):
    - Implemented a per-pass `removableBudget` in `RemoveVMsWithLeastRecentUse`.
    - Cleanup now schedules at most `TotalVMCount - MinScale` idle VMs for removal in a scan, while preserving the existing post-scan removal flow.

- [x] Task 1.3: add one focused regression test
  - Scope:
    - add a test proving multiple idle VMs do not scale below `minScale` in one tick,
    - keep the test local to `pkg/activator/net/khala`.
  - Approval gate:
    - wait for your explicit approval before adding or changing tests for this step.
  - Result (2026-03-25):
    - Added `TestCleanupDoesNotScaleBelowMinScaleInSinglePass` in `pkg/activator/net/khala/khala_test.go`.
    - The test seeds three idle VMs with `MinScale=1` and asserts cleanup settles at exactly one remaining available VM.

- [x] Task 1.4: run focused verification and record the result
  - Scope:
    - run the smallest relevant Khala test target,
    - record pass/fail evidence in this file.
  - Approval gate:
    - wait for your explicit approval before running verification for this step.
  - Verification (2026-03-25):
    - `GOCACHE=/tmp/go-build go test ./pkg/activator/net/khala -run TestCleanupDoesNotScaleBelowMinScaleInSinglePass -count=1`
    - Result: pass

## Task 2 - `AddVM` must validate RPC success before counting or routing

- [x] Task 2.1: lock the validation contract for `AddVMResponse`
  - Scope:
    - confirm which response fields are required for a usable VM,
    - document the intended behavior for `success=false`, empty `vm_id`, empty `ip`, or empty `rpc_port`.
  - Approval gate:
    - wait for your explicit approval before starting this step.
  - Result (2026-03-25):
    - Locked validation contract for `CreateVM`:
      - require a non-nil `AddVMResponse`,
      - require `success=true`,
      - require non-empty `vm_id`, `ip`, and `rpc_port` before returning or counting a VM.
    - Locked failure behavior:
      - if `success=false`, `vm_id` is empty, `ip` is empty, `rpc_port` is empty, or the response is nil, treat the call as a create failure,
      - do not increment `NodeVMCount` or `TotalVMCount`,
      - follow the same rollback/waiter-wakeup path as transport-level create failure.
    - `workload_name` is informational and not required for routing, so it stays out of the minimal validation set.

- [x] Task 2.2: implement the minimal `CreateVM` validation
  - Scope:
    - keep `CreateVM` structure intact,
    - reject unsuccessful or incomplete responses before incrementing VM counts,
    - treat invalid responses like create failures.
  - Approval gate:
    - wait for your explicit approval before editing code for this step.
  - Result (2026-03-25):
    - Added a small `validateAddVMResponse` helper in `pkg/activator/net/khala/khala.go`.
    - `CreateVM` now validates the RPC payload before constructing/counting a VM and reuses the existing create-failure rollback path for invalid responses.

- [x] Task 2.3: add one focused regression test
  - Scope:
    - add a test covering unsuccessful or incomplete `AddVM` responses,
    - assert counters remain correct and no unusable VM is returned.
  - Approval gate:
    - wait for your explicit approval before adding or changing tests for this step.
  - Result (2026-03-25):
    - Added `TestCreateVMInvalidResponseDoesNotCorruptCounts` in `pkg/activator/net/khala/khala_test.go`.
    - Added a minimal fake-client hook to return a custom `AddVMResponse` so the test can exercise the non-transport failure path.

- [x] Task 2.4: run focused verification and record the result
  - Scope:
    - run the smallest relevant Khala test target,
    - record pass/fail evidence in this file.
  - Approval gate:
    - wait for your explicit approval before running verification for this step.
  - Verification (2026-03-25):
    - `GOCACHE=/tmp/go-build go test ./pkg/activator/net/khala -run TestCreateVMInvalidResponseDoesNotCorruptCounts -count=1`
    - Result: pass

## Task 3 - Invalidate only on explicit unhealthy backend failures

- [x] Task 3.1: map the current request/proxy error flow and define the unhealthy error class
  - Scope:
    - trace `handler -> throttler -> proxy`,
    - list which failures should invalidate a VM,
    - list which failures must return the VM to pool, including likely timeout/overload cases.
  - Approval gate:
    - wait for your explicit approval before starting this step.
  - Result (2026-03-25):
    - Current flow confirmed:
      - `activationHandler.ServeHTTP` calls `throttler.Try` with a callback that always returns `nil` after `proxyRequest`,
      - `proxyRequest` installs `ReverseProxy.ErrorHandler`, which handles proxy transport failures locally instead of returning them to `Try`,
      - therefore proxy transport errors do not currently reach `revisionThrottler.try`, and the existing `ret != nil && ctx.Err() == nil` invalidation branch does not classify real proxy failures today.
    - Locked minimal unhealthy error class for the follow-up implementation:
      - only invalidate when proxy transport returns an explicit endpoint-unreachable error proving the selected VM cannot be dialed or routed,
      - initial narrow set: connection refused, no route to host, host unreachable, or network unreachable.
    - Locked non-unhealthy class:
      - request context cancellation or deadline,
      - breaker queue full / admission timeout,
      - VM create wait timeout,
      - backend HTTP status codes,
      - generic transport timeout / overload / temporary congestion scenarios that can happen on a healthy but crowded node.
    - Implementation implication:
      - Task 3.2 should add a small error-return path from proxy transport failures back into `throttler.try`, plus a narrow predicate that only maps the explicit endpoint-unreachable class to `InvalidateVM`.

- [x] Task 3.2: implement the smallest error-propagation change
  - Scope:
    - preserve the current request flow,
    - propagate proxy transport errors back into `Try` only as needed,
    - add a narrow predicate for unhealthy backend failures,
    - keep generic timeout/load errors from calling `InvalidateVM`.
  - Approval gate:
    - wait for your explicit approval before editing code for this step.
  - Result (2026-03-25):
    - `proxyRequest` now returns handled proxy transport errors back to `ServeHTTP`, and the handler forwards them into `throttler.Try`.
    - Added a small handled-error wrapper so `ServeHTTP` does not write a second HTTP error after `ReverseProxy.ErrorHandler` already handled the response.
    - `revisionThrottler.try` now invalidates only for the explicit endpoint-unreachable class (`ECONNREFUSED`, `EHOSTUNREACH`, `ENETUNREACH`) and returns the VM to pool for all other failure cases.

- [x] Task 3.3: add focused regression coverage
  - Scope:
    - add a test for an explicitly unhealthy failure that invalidates,
    - add a test for a likely healthy failure path that does not invalidate.
  - Approval gate:
    - wait for your explicit approval before adding or changing tests for this step.
  - Result (2026-03-25):
    - Added `TestRevisionThrottlerTryInvalidatesOnExplicitEndpointUnreachable` in `pkg/activator/net/throttler_test.go`.
    - Added `TestRevisionThrottlerTryDoesNotInvalidateOnTimeoutLikeFailure` in `pkg/activator/net/throttler_test.go`.
    - The two tests exercise the exact `revisionThrottler.try` invalidation decision using wrapped endpoint-unreachable vs wrapped timeout-like errors.

- [x] Task 3.4: run focused verification and record the result
  - Scope:
    - run the smallest relevant activator and Khala test targets,
    - record pass/fail evidence in this file.
  - Approval gate:
    - wait for your explicit approval before running verification for this step.
  - Verification (2026-03-25):
    - `GOCACHE=/tmp/go-build go test ./pkg/activator/net -run 'TestRevisionThrottlerTryInvalidatesOnExplicitEndpointUnreachable|TestRevisionThrottlerTryDoesNotInvalidateOnTimeoutLikeFailure' -count=1`
    - Result: pass
    - `GOCACHE=/tmp/go-build go test ./pkg/activator/handler -run '^$' -count=1`
    - Result: pass (`[no tests to run]`, compile check only)

## Task 4 - Equal-load placement needs a fair tie-breaker

- [x] Task 4.1: choose the smallest maintainable tie-breaker
  - Scope:
    - compare a rotating index vs pseudo-random tie-break,
    - pick the simpler option that avoids permanent first-node bias,
    - keep behavior deterministic enough for tests.
  - Approval gate:
    - wait for your explicit approval before starting this step.
  - Result (2026-03-25):
    - Chosen tie-breaker: rotating selection among equal lowest-load candidate nodes.
    - Reason:
      - avoids permanent `node[0]` bias,
      - reuses the existing `nextNodeIndex` field already present in `VMList`,
      - stays deterministic and test-friendly,
      - is smaller and easier to maintain than introducing pseudo-random selection and seed management.
    - Locked implementation shape for Task 4.2:
      - keep the current least-loaded policy,
      - gather the nodes tied for minimum `(NodeVMCount + NodeCreateInFlight)`,
      - choose among those tied nodes by rotating `nextNodeIndex` under `vml.Lock`,
      - leave non-tie behavior unchanged.

- [x] Task 4.2: implement the minimal selection change
  - Scope:
    - touch only the node-selection logic in `CreateVM`,
    - avoid changing the overall least-loaded policy.
  - Approval gate:
    - wait for your explicit approval before editing code for this step.
  - Result (2026-03-25):
    - Updated `CreateVM` in `pkg/activator/net/khala/khala.go` to gather the equal minimum-load candidate nodes and rotate across that tie set using `nextNodeIndex`.
    - Non-tie behavior is unchanged: a single least-loaded node is still selected directly.
  - Follow-up adjustment (2026-03-25, approved as Task 4.2b):
    - Seeded `nextNodeIndex` once in `NewRevVMList` using a UnixNano-based helper so the first create for a new revision no longer defaults to `node[0]`.
    - Kept the rotating tie-breaker for later equal-load selections and avoided adding a UUID dependency.

- [x] Task 4.3: add one focused distribution test
  - Scope:
    - prove equal-count candidates do not always pick the first node,
    - keep the test narrow and stable.
  - Approval gate:
    - wait for your explicit approval before adding or changing tests for this step.
  - Result (2026-03-25):
    - Added `TestCreateVMFirstTieUsesSeededInitialNodeOffset` in `pkg/activator/net/khala/khala_test.go`.
    - The test overrides the seed helper deterministically, builds a two-node `VMList`, and asserts the first `CreateVM` on an equal-load tie can select the second node instead of defaulting to `node[0]`.

- [x] Task 4.4: run focused verification and record the result
  - Scope:
    - run the smallest relevant Khala test target,
    - record pass/fail evidence in this file.
  - Approval gate:
    - wait for your explicit approval before running verification for this step.
  - Verification (2026-03-25):
    - `GOCACHE=/tmp/go-build go test ./pkg/activator/net/khala -run TestCreateVMFirstTieUsesSeededInitialNodeOffset -count=1`
    - Result: pass

## Execution rule

- [ ] Only one step may be active at a time.
- [ ] No implementation, test edit, or verification command starts until you explicitly approve that specific step.
- [ ] Keep each code change minimal, local, and easy to maintain.
