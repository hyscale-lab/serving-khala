package net

import (
	"context"
	"sync"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	pkgmetrics "knative.dev/pkg/metrics"
	"knative.dev/serving/pkg/activator/net/khala"
	servingmetrics "knative.dev/serving/pkg/metrics"
)

const deployedVMNodeLabel = "node"

var (
	deployedVMNodeKey = tag.MustNewKey(deployedVMNodeLabel)

	revisionDeployedVMCountM = stats.Int64(
		"activator_revision_throttler_deployed_vm_count",
		"The current number of deployed VMs owned by a revision throttler.",
		stats.UnitDimensionless)
	revisionNodeDeployedVMCountM = stats.Int64(
		"activator_revision_throttler_node_deployed_vm_count",
		"The current number of deployed VMs owned by a revision throttler on a specific node.",
		stats.UnitDimensionless)
	clusterNodeLiveVMCountM = stats.Int64(
		"activator_cluster_node_live_vm_count",
		"The current number of live VMs on a specific cluster node across all revisions.",
		stats.UnitDimensionless)
	clusterNodeCreateInFlightCountM = stats.Int64(
		"activator_cluster_node_create_inflight_count",
		"The current number of in-flight VM creates on a specific cluster node across all revisions.",
		stats.UnitDimensionless)
	clusterNodeActiveRequestCountM = stats.Int64(
		"activator_cluster_node_active_request_count",
		"The current number of active requests on a specific cluster node across all revisions.",
		stats.UnitDimensionless)

	registerRevisionDeployedVMMetricOnce sync.Once
)

func init() {
	registerRevisionDeployedVMMetrics()
}

func registerRevisionDeployedVMMetrics() {
	registerRevisionDeployedVMMetricOnce.Do(func() {
		if err := pkgmetrics.RegisterResourceView(
			&view.View{
				Description: "The current number of deployed VMs owned by a revision throttler.",
				Measure:     revisionDeployedVMCountM,
				Aggregation: view.LastValue(),
			},
			&view.View{
				Description: "The current number of deployed VMs owned by a revision throttler on a specific node.",
				Measure:     revisionNodeDeployedVMCountM,
				Aggregation: view.LastValue(),
				TagKeys:     []tag.Key{deployedVMNodeKey},
			},
			&view.View{
				Description: "The current number of live VMs on a specific cluster node across all revisions.",
				Measure:     clusterNodeLiveVMCountM,
				Aggregation: view.LastValue(),
				TagKeys:     []tag.Key{deployedVMNodeKey},
			},
			&view.View{
				Description: "The current number of in-flight VM creates on a specific cluster node across all revisions.",
				Measure:     clusterNodeCreateInFlightCountM,
				Aggregation: view.LastValue(),
				TagKeys:     []tag.Key{deployedVMNodeKey},
			},
			&view.View{
				Description: "The current number of active requests on a specific cluster node across all revisions.",
				Measure:     clusterNodeActiveRequestCountM,
				Aggregation: view.LastValue(),
				TagKeys:     []tag.Key{deployedVMNodeKey},
			},
		); err != nil {
			panic(err)
		}
	})
}

type deployedVMCountReporter interface {
	Report(total int, perNode map[string]int)
}

type nodeLoadReporter interface {
	Report(node string, load khala.NodeLoad)
}

type revisionVMCountReporter struct {
	baseCtx context.Context

	mu       sync.Mutex
	nodeCtxs map[string]context.Context
}

func newRevisionVMCountReporter(ns, svc, cfg, rev string) *revisionVMCountReporter {
	return &revisionVMCountReporter{
		baseCtx:  servingmetrics.RevisionContext(ns, svc, cfg, rev),
		nodeCtxs: make(map[string]context.Context),
	}
}

func (r *revisionVMCountReporter) Report(total int, perNode map[string]int) {
	if r == nil {
		return
	}

	pkgmetrics.Record(r.baseCtx, revisionDeployedVMCountM.M(int64(total)))
	for node, count := range perNode {
		pkgmetrics.Record(r.nodeContext(node), revisionNodeDeployedVMCountM.M(int64(count)))
	}
}

func (r *revisionVMCountReporter) nodeContext(node string) context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()

	if ctx, ok := r.nodeCtxs[node]; ok {
		return ctx
	}

	ctx, err := tag.New(r.baseCtx, tag.Upsert(deployedVMNodeKey, node))
	if err != nil {
		return r.baseCtx
	}
	r.nodeCtxs[node] = ctx
	return ctx
}

type clusterNodeLoadReporter struct {
	mu       sync.Mutex
	nodeCtxs map[string]context.Context
}

func newClusterNodeLoadReporter() *clusterNodeLoadReporter {
	return &clusterNodeLoadReporter{
		nodeCtxs: make(map[string]context.Context),
	}
}

func (r *clusterNodeLoadReporter) Report(node string, load khala.NodeLoad) {
	if r == nil {
		return
	}

	ctx := r.nodeContext(node)
	pkgmetrics.Record(ctx, clusterNodeLiveVMCountM.M(load.LiveVMs))
	pkgmetrics.Record(ctx, clusterNodeCreateInFlightCountM.M(load.CreateInFlight))
	pkgmetrics.Record(ctx, clusterNodeActiveRequestCountM.M(load.ActiveRequests))
}

func (r *clusterNodeLoadReporter) nodeContext(node string) context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()

	if ctx, ok := r.nodeCtxs[node]; ok {
		return ctx
	}

	ctx, err := tag.New(context.Background(), tag.Upsert(deployedVMNodeKey, node))
	if err != nil {
		return context.Background()
	}
	r.nodeCtxs[node] = ctx
	return ctx
}
