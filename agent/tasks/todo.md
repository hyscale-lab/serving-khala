# Task: Phased Khala Node-Aware Balancing

## Checklist

- [x] Step 1: Add shared `NodeLoadTracker` with atomic per-node counters
- [x] Verify Step 1
- [x] Approval gate after Step 1
- [x] Step 2: Refactor revision VM pool to per-node idle pools
- [x] Verify Step 2
- [x] Approval gate after Step 2
- [x] Step 3: Use shared tracker for `CreateVM` and `InitialScaleUp`
- [x] Verify Step 3
- [x] Approval gate after Step 3
- [x] Step 4: Use shared tracker for node-aware `AcquireVM`
- [x] Verify Step 4
- [x] Approval gate after Step 4
- [x] Step 5: Make keepalive cleanup node-aware
- [x] Verify Step 5
- [x] Approval gate after Step 5
- [x] Step 6: Run final correctness and performance validation
- [x] Summarize residual limitations

## Current Focus

- Implementation complete; awaiting review

## Acceptance Criteria

- Single-node and multi-node both work on the same code path.
- Cross-revision placement spreads VMs by node pressure.
- Request routing avoids hot-node skew without destroying MRU-based cleanup.
- Hot-path shared state avoids a global lock.

## Working Notes

- One activator process is authoritative for shared node state.
- Step 1 must add the shared tracker without changing current scheduling behavior.
- Shared hot-path state should use atomics; revision-local VM pool locking stays revision-local.

## Results

- Step 1 complete: added a shared activator-global `NodeLoadTracker` with atomic per-node counters.
- Step 1 complete: wired the tracker through `Throttler`, `revisionThrottler`, and `khala.VMList` without changing scheduling behavior.
- Step 1 verified with focused tests in `./pkg/activator/net/khala` and `./pkg/activator/net`.
- Step 2 complete: replaced the flat revision-local idle VM slice with per-node idle pools plus a temporary global idle order helper to preserve current pop behavior.
- Step 2 verified with focused Khala tests covering push/pop parity, single-node parity, and cleanup/invalidation behavior.
- Step 3 complete: `CreateVM` and `InitialScaleUp` now place using the shared node tracker (`live_vms + create_inflight`, then `active_requests`, then rotating tie-break).
- Step 3 verified with focused Khala tests covering cross-revision spread, initial scale-up placement, and shared counter rollback on create failure paths.
- Step 4 complete: `AcquireVM` now chooses among this revision's idle nodes using shared `active_requests`, then shared placement pressure, and preserves MRU within the chosen node.
- Step 4 complete: shared `active_requests` now increments on checkout and decrements on release or invalidate.
- Step 4 verified with focused Khala tests covering node-aware acquire, active request bookkeeping, and single-node parity.
- Step 5 complete: keepalive cleanup now plans removals by node pressure, preferring the most overprovisioned nodes first while respecting `MinScale`.
- Step 5 verified with focused Khala tests covering multi-node cleanup preference, cleanup failure behavior, and single-node min-scale preservation.
- Step 6 complete: added a parallel acquire/release drift test across 1, 4, and 8 nodes plus a small hot-path benchmark for `AcquireVM`/release.
- Step 6 verified with the full `./pkg/activator/net/khala` suite, targeted throttler tests for shared tracker wiring and khala request behavior, and a short benchmark run.
- Residual limitation: the broad `./pkg/activator/net` suite still contains older tests that assume non-khala routing and can hang without fake khala VM clients; targeted throttler validation was used instead of treating that package-wide sweep as a release gate.
