# Khala Integration Review

## Scope and context

This document reviews Khala integration changes across three points:

- Baseline: `59626f893fb731540cef2658eb093bb23d9c6485`
- Intermediate: `1999676af52b74edad15d679591be86773477719`
- Newest: `76656d477650d49ba3739813e1d7ee222114d4a4`

Goal: explain implementation rationale, identify flaws, and provide a decision-complete improvement roadmap for VM-based activation where VMs are not pod-sandboxed containers.

## Architecture evolution

### Baseline (`59626f8`)

- Activator request path used standard Knative flow in `revisionThrottler.try`.
- Requests passed through `rt.breaker.Maybe(...)`.
- Destination selection relied on pod tracker/clusterIP tracker (`acquireDest`), then proxied to pod endpoints.
- Autoscaler stat forwarding was normal activator behavior.

### Intermediate (`1999676`)

- Added Khala VM integration:
  - `VMList` in `pkg/activator/net/khala/khala.go`.
  - Khala annotations in `pkg/apis/khala/annotations.go`.
  - gRPC client/proto for VM orchestrator.
- `revisionThrottler.try` switched to VM route allocation:
  - `rt.khalaBreaker.Maybe(...)`
  - `rt.vmList.AcquireVM(...)`
  - Route by `vm.Node + ":" + vm.RPCPort`
- Legacy pod tracker machinery remained in the type, but request routing bypassed it.
- `stat_reporter` was changed to marshal stats but not send them to autoscaler.

### Newest (`76656d4`)

- Refined Lambda-like inline create behavior:
  - `AcquireVM` now performs direct inline `CreateVM` when below max scale.
  - `khalaBreaker` initial capacity opened to full `MaxScale`.
- Timeout handling changed:
  - Request context passed to `CreateVM`/`AddVM`.
  - Context timeout no longer auto-invalidates VM in `try` defer branch.
- Signaling logic changed from channel-close broadcast to buffered token wakeups.

## Current request and VM lifecycle

1. Activator receives request and enters `Throttler.Try`.
2. `revisionThrottler.try` uses `khalaBreaker.Maybe(ctx, ...)`.
3. `AcquireVM(ctx)` attempts:
   - MRU pop (`PopVM`)
   - if empty and not at max scale: inline `CreateVM(ctx)`
   - else wait on `vmAvailableChan`.
4. Request executes against selected VM endpoint.
5. Completion path:
   - success or context cancellation: release VM back via `PushVM`
   - non-context error: `InvalidateVM`, retries up to 3 before delete.
6. Cleanup path:
   - periodic LRU scale-down for unused VMs beyond keepalive window.

This preserves MRU allocation and LRU scale-down policy.

## Breaker role analysis

### Current behavior

- `rt.breaker` is still part of `revisionThrottler`, but `try` does not use pod tracker reservations for request routing anymore.
- `rt.khalaBreaker` controls request admission to the VM route.
- `khalaBreaker` capacity is independent from actual create-inflight pressure.

### Practical implication

- Admission is decoupled from VM creation pacing.
- During burst traffic, many admitted requests can enter inline creation path simultaneously and overload workers.

## Findings (ordered by severity)

1. **Autoscaler short-circuit is active and implicit**
   - Evidence: `pkg/activator/stat_reporter.go` marshals but does not call `sink.SendRaw`.
   - Impact: autoscaler stats stream is effectively disabled without explicit contract/config.
   - Verification: `go test ./pkg/activator -run TestReportStats -count=1` fails (`Did not receive results after 2 seconds`).

2. **Hard in-cluster dependency crashes tests and non-cluster runs**
   - Evidence: `getNodes` uses `rest.InClusterConfig()` and `logger.Fatalf` in `pkg/activator/net/throttler.go`.
   - Impact: unit tests and local non-cluster executions fail hard.
   - Verification: `go test -v ./pkg/activator/net -run TestPodAssignmentFinite -count=1` fails at `throttler.go:571`.

3. **Spike amplification risk**
   - Evidence: `khalaBreaker` starts with `InitialCapacity = MaxScale`.
   - Impact: many concurrent requests can each initiate inline create path, causing burst VM creation and worker pressure.

4. **Timeout-vs-create semantic mismatch**
   - Evidence: `CreateVM(ctx)` passes request context into `AddVM(ctx, ...)`.
   - Impact: request timeout can cancel VM creation, conflicting with requirement to finish creation and return VM to pool.

5. **Counter/accounting drift risk**
   - Evidence: delete paths do not validate remove response success before decrementing counters.
   - Impact: `TotalVMCount` and `NodeVMCount` can drift from real orchestrator state.

6. **Missing/invalid `khala/max-scale` can deadlock admission**
   - Evidence: annotation parsing can leave scale values as zero, and khala breaker max concurrency then becomes zero.
   - Impact: no requests admitted for affected revision.

7. **Legacy tracker path remains while request path bypasses it**
   - Evidence: pod tracker assignment/capacity code still exists, but `try` routes exclusively via VM list.
   - Impact: maintenance ambiguity and increased cognitive load/risk of accidental regressions.

## Improvement roadmap (decision-complete)

### 1. Safety-first VM creation pacing

- **Recommended breaker usage policy**
  - Keep request queueing in activator (do not remove queue path).
  - Keep `khalaBreaker.MaxConcurrency = khala/max-scale` as hard upper bound.
  - Set `khalaBreaker.InitialCapacity` to warm-capacity, not max-capacity:
    - target `min(initialScale + 1, maxScale)`,
    - and never start at `maxScale` unless explicitly required.
- Add explicit per-revision VM create-inflight limiter:
  - default start value: `3`,
  - operationally tunable in `2-5` range based on node stability.
- Decouple request admission from VM create-inflight:
  - requests may queue while create slots are full,
  - only goroutines with create permits execute synchronous create calls.
- Keep synchronous create behavior for permit holders (Lambda-like), but bound the number of concurrent creators.
- Add jittered retry with bounded backoff on create failures.

### 2. Fail-closed scale policy

- Require valid `khala/max-scale`.
- If missing/invalid: reject revision activation path for that revision with explicit log/event.
- Do not silently fall back to zero-capacity behavior.

### 3. Timeout semantics fix

- Detach VM creation lifecycle from request cancellation.
- If request times out while create continues, finish VM creation and push VM back for later requests.
- Preserve request timeout behavior for the original caller.

### 4. Counter correctness

- Mutate `NodeVMCount`/`TotalVMCount` only after confirmed orchestrator success.
- Handle remove failures with retry/reconciliation instead of unconditional decrement.
- Add periodic reconciliation hook against orchestrator truth.

### 5. Autoscaler bypass hygiene

- Keep bypass for VM-outside-pod model, but make it explicit and config-gated.
- Document bypass contract and required future re-enable path.

### 6. Operational guardrails

- Add observability for:
  - create attempts
  - create in-flight
  - create failures
  - request wait latency
  - counter drift/reconciliation events
  - breaker in-flight / queued depth

## Test scenarios and acceptance criteria

1. **Request ordering**
   - Later request should route to available VM even while earlier request is waiting for cold-start.

2. **Timeout behavior**
   - Request timeout must not cancel VM creation; completed VM must be returned to pool.

3. **Burst control**
   - Small spike should not exceed configured create-inflight cap.

4. **Counter integrity**
   - Add/remove failures must not corrupt in-memory VM counters.

5. **Non-cluster safety**
   - Throttler tests should run without mandatory in-cluster kube env.

6. **Autoscaler bypass mode**
   - Bypass state must be explicit/configurable and testable.

## Public API and interface impact

### This documentation pass

- No production API/type changes.

### Likely future interface changes

- Config knobs for VM create concurrency and autoscaler bypass mode.
- Stronger validation contract for Khala scale annotations.

## Verification evidence captured in this review

1. `go test -v ./pkg/activator/net -run TestPodAssignmentFinite -count=1`
   - Historical state: failed due in-cluster config hard dependency.
   - Current state (post-Phase 6): passes with injectable/non-fatal node discovery path.

2. `go test ./pkg/activator -run TestReportStats -count=1`
   - Fails: `TestReportStats` timeout waiting for sent stats.

3. `go test ./pkg/activator/net -run TestThrottlerTry -count=1`
   - Reports `ok ... [no tests to run]`.
   - This is a coverage gap for VM-path semantics.

## Assumptions locked for this plan

1. Creation policy: **Safety-first cap**.
2. Missing/invalid `khala/max-scale`: **Fail closed**.
3. Autoscaler short-circuit is currently required by VM-outside-pod constraints, but should be explicit and reversible.
4. Keep existing LRU (>60s idle) scale-down and MRU allocation behavior unless explicitly revised.
