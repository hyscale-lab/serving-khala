# Lessons Learned

## 2026-02-25 - Khala integration review

### 1) Cluster-only initialization in request-path dependencies

- Failure mode:
  - Activator throttler initialization calls `rest.InClusterConfig()` and `Fatalf`, breaking unit tests and non-cluster execution.
- Detection signal:
  - `go test -v ./pkg/activator/net -run TestPodAssignmentFinite -count=1` fatal at throttler node discovery.
- Prevention rule:
  - Never hard-fail process startup for environment-specific discovery in shared/runtime-test code paths; provide injectable/fallback node source and return errors instead of `Fatalf`.

### 2) Implicit behavior bypass is risky

- Failure mode:
  - Autoscaler stat path was silently disabled by commenting out send logic while retaining marshaling.
- Detection signal:
  - `go test ./pkg/activator -run TestReportStats -count=1` timeout waiting for sent stats.
- Prevention rule:
  - Avoid hidden bypasses; gate behavior behind explicit config flags and keep tests aligned with intended mode.

### 3) Admission concurrency is not create concurrency

- Failure mode:
  - Request admission breaker capacity can be high while VM create-inflight is unbounded enough to trigger burst VM provisioning.
- Detection signal:
  - Design inspection of `khalaBreaker` initialization and inline `CreateVM` request path.
- Prevention rule:
  - Separate and explicitly cap VM create-inflight per revision (and optionally per node); do not infer create safety from request admission limits.

### 4) Request timeout lifecycle must be decoupled from infrastructure lifecycle

- Failure mode:
  - Request context cancellation can terminate VM creation path when requirement is to finish creation and return VM to pool.
- Detection signal:
  - `CreateVM(ctx)` and downstream `AddVM(ctx, ...)` tied directly to request context.
- Prevention rule:
  - Model VM creation as infrastructure work item with its own lifecycle context; request cancellation should not necessarily cancel infrastructure provisioning.

### 5) Prefer narrow verification targets when VM/orchestrator deps are live

- Failure mode:
  - Running broad throttler tests can trigger noisy retry loops against unavailable VM orchestrator endpoints and obscure the intended regression signal.
- Detection signal:
  - `go test -v ./pkg/activator/net -run TestThrottlerErrorNoRevision|...` emitted sustained gRPC dial failures and did not converge promptly.
- Prevention rule:
  - For infra-decoupling changes, run focused tests that isolate the target behavior (e.g., node discovery and constructor paths) before broader suites.
