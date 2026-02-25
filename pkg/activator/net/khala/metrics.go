package khala

import (
	"context"
	"sync"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	pkgmetrics "knative.dev/pkg/metrics"
)

var (
	khalaVMCreateAttemptCountM = stats.Int64(
		"khala_vm_create_attempt_count",
		"The number of VM create attempts issued by the activator.",
		stats.UnitDimensionless)
	khalaVMCreateSuccessCountM = stats.Int64(
		"khala_vm_create_success_count",
		"The number of successful VM create operations.",
		stats.UnitDimensionless)
	khalaVMCreateFailureCountM = stats.Int64(
		"khala_vm_create_failure_count",
		"The number of failed VM create operations.",
		stats.UnitDimensionless)
	khalaVMCreateInflightM = stats.Int64(
		"khala_vm_create_inflight",
		"The current number of in-flight VM create operations.",
		stats.UnitDimensionless)
	khalaVMWaitLatencyMsM = stats.Float64(
		"khala_vm_wait_latency_ms",
		"Time spent waiting for a VM to become available.",
		stats.UnitMilliseconds)
	khalaBreakerInFlightM = stats.Int64(
		"khala_breaker_inflight",
		"The current number of requests tracked by the Khala breaker (active + queued).",
		stats.UnitDimensionless)
	khalaBreakerQueuedDepthM = stats.Int64(
		"khala_breaker_queued_depth",
		"The current number of requests queued in the Khala breaker.",
		stats.UnitDimensionless)
	khalaReconcileCorrectionCountM = stats.Int64(
		"khala_reconcile_correction_count",
		"The number of VM counter reconciliation corrections performed.",
		stats.UnitDimensionless)

	khalaDefaultLatencyDistribution = view.Distribution(
		5, 10, 20, 40, 60, 80, 100, 150, 200, 250, 300, 350, 400, 450, 500, 600,
		700, 800, 900, 1000, 2000, 5000, 10000, 20000, 50000, 100000)

	registerKhalaMetricsOnce sync.Once
)

func init() {
	registerKhalaMetrics()
}

func registerKhalaMetrics() {
	registerKhalaMetricsOnce.Do(func() {
		if err := pkgmetrics.RegisterResourceView(
			&view.View{
				Description: "The number of VM create attempts issued by the activator.",
				Measure:     khalaVMCreateAttemptCountM,
				Aggregation: view.Count(),
			},
			&view.View{
				Description: "The number of successful VM create operations.",
				Measure:     khalaVMCreateSuccessCountM,
				Aggregation: view.Count(),
			},
			&view.View{
				Description: "The number of failed VM create operations.",
				Measure:     khalaVMCreateFailureCountM,
				Aggregation: view.Count(),
			},
			&view.View{
				Description: "The current number of in-flight VM create operations.",
				Measure:     khalaVMCreateInflightM,
				Aggregation: view.LastValue(),
			},
			&view.View{
				Description: "Time spent waiting for a VM to become available.",
				Measure:     khalaVMWaitLatencyMsM,
				Aggregation: khalaDefaultLatencyDistribution,
			},
			&view.View{
				Description: "The current number of requests tracked by the Khala breaker (active + queued).",
				Measure:     khalaBreakerInFlightM,
				Aggregation: view.LastValue(),
			},
			&view.View{
				Description: "The current number of requests queued in the Khala breaker.",
				Measure:     khalaBreakerQueuedDepthM,
				Aggregation: view.LastValue(),
			},
			&view.View{
				Description: "The number of VM counter reconciliation corrections performed.",
				Measure:     khalaReconcileCorrectionCountM,
				Aggregation: view.Count(),
			},
		); err != nil {
			panic(err)
		}
	})
}

func recordVMCreateAttempt() {
	stats.Record(context.Background(), khalaVMCreateAttemptCountM.M(1))
}

func recordVMCreateSuccess() {
	stats.Record(context.Background(), khalaVMCreateSuccessCountM.M(1))
}

func recordVMCreateFailure() {
	stats.Record(context.Background(), khalaVMCreateFailureCountM.M(1))
}

func recordVMCreateInFlight(count int) {
	stats.Record(context.Background(), khalaVMCreateInflightM.M(int64(count)))
}

func recordVMWaitLatency(wait time.Duration) {
	stats.Record(context.Background(), khalaVMWaitLatencyMsM.M(float64(wait.Milliseconds())))
}

func recordReconcileCorrection() {
	stats.Record(context.Background(), khalaReconcileCorrectionCountM.M(1))
}

func queuedDepth(inFlight, capacity int) int {
	if inFlight <= capacity {
		return 0
	}
	return inFlight - capacity
}

// RecordBreakerDepth records a point-in-time view of khala breaker pressure.
func RecordBreakerDepth(inFlight, capacity int) {
	stats.Record(context.Background(),
		khalaBreakerInFlightM.M(int64(inFlight)),
		khalaBreakerQueuedDepthM.M(int64(queuedDepth(inFlight, capacity))))
}
