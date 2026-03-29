package khala

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	pb "knative.dev/serving/pkg/proto"
)

type fakeKhalaClient struct {
	addDelay   time.Duration
	activeAdds int64
	maxAdds    int64
	seq        int64
	onAddStart func()
	addErr     error
	addResp    *pb.AddVMResponse
	removeErr  error
	removeOK   bool
	removeSet  bool
}

func (f *fakeKhalaClient) AddVM(ctx context.Context, _ *pb.AddVMRequest, _ ...grpc.CallOption) (*pb.AddVMResponse, error) {
	if f.addErr != nil {
		return nil, f.addErr
	}
	if f.onAddStart != nil {
		f.onAddStart()
	}
	cur := atomic.AddInt64(&f.activeAdds, 1)
	for {
		prev := atomic.LoadInt64(&f.maxAdds)
		if cur <= prev || atomic.CompareAndSwapInt64(&f.maxAdds, prev, cur) {
			break
		}
	}
	defer atomic.AddInt64(&f.activeAdds, -1)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(f.addDelay):
	}

	id := atomic.AddInt64(&f.seq, 1)
	if f.addResp != nil {
		return f.addResp, nil
	}
	return &pb.AddVMResponse{
		Success: true,
		VmId:    fmt.Sprintf("vm-%d", id),
		Ip:      "10.0.0.1",
		RpcPort: "8080",
	}, nil
}

func (f *fakeKhalaClient) GetVMMetrics(context.Context, *pb.GetVMMetricsRequest, ...grpc.CallOption) (*pb.GetVMMetricsResponse, error) {
	return &pb.GetVMMetricsResponse{}, nil
}

func (f *fakeKhalaClient) RemoveVM(context.Context, *pb.RemoveVMRequest, ...grpc.CallOption) (*pb.RemoveVMResponse, error) {
	if f.removeErr != nil {
		return nil, f.removeErr
	}
	if !f.removeSet {
		return &pb.RemoveVMResponse{Success: true}, nil
	}
	if !f.removeOK {
		return &pb.RemoveVMResponse{Success: false, Message: "failed"}, nil
	}
	return &pb.RemoveVMResponse{Success: true}, nil
}

func (f *fakeKhalaClient) DestroyAll(context.Context, *pb.DestroyAllRequest, ...grpc.CallOption) (*pb.DestroyAllResponse, error) {
	return &pb.DestroyAllResponse{Success: true}, nil
}

func (f *fakeKhalaClient) CreateSnapshot(context.Context, *pb.CreateSnapshotRequest, ...grpc.CallOption) (*pb.CreateSnapshotResponse, error) {
	return &pb.CreateSnapshotResponse{Success: true}, nil
}

func (f *fakeKhalaClient) MaxConcurrentAdds() int64 {
	return atomic.LoadInt64(&f.maxAdds)
}

func newTestVMList(maxScale, createConcurrency int, client pb.KhalaKnativeIntegrationClient) *VMList {
	return newTestVMListWithCallback(maxScale, createConcurrency, client, nil)
}

func newTestVMListWithCallback(maxScale, createConcurrency int, client pb.KhalaKnativeIntegrationClient, onCapacity func(int)) *VMList {
	if maxScale < 1 {
		maxScale = 1
	}
	createPermits := make(chan struct{}, createConcurrency)
	for i := 0; i < createConcurrency; i++ {
		createPermits <- struct{}{}
	}
	return &VMList{
		idleVMsByNode:        map[string][]*VMMetadata{"node-1": make([]*VMMetadata, 0)},
		Workload:             "test-workload",
		Nodes:                []string{"node-1"},
		NodeVMCount:          map[string]int{"node-1": 0},
		NodeCreateInFlight:   map[string]int{"node-1": 0},
		khalaGrpcClient:      map[string]pb.KhalaKnativeIntegrationClient{"node-1": client},
		keepaliveDurationSec: 60,
		updateIntervalSec:    5,
		logger:               zap.NewNop().Sugar(),
		revScale:             RevisionScaleInfo{MaxScale: maxScale},
		vmAvailableChan:      make(chan struct{}, maxScale),
		createConcurrency:    createConcurrency,
		createPermits:        createPermits,
		createTimeoutSec:     5,
		warmBuffer:           1,
		capacityUpdateFunc:   onCapacity,
		nodeLoadTracker:      NewNodeLoadTracker([]string{"node-1"}),
	}
}

func newMultiNodeTestVMList(nodes []string, revScale RevisionScaleInfo, tracker *NodeLoadTracker, clients map[string]pb.KhalaKnativeIntegrationClient) *VMList {
	vml := NewRevVMList("test-workload", nodes, revScale, zap.NewNop().Sugar(), nil, tracker)
	vml.khalaGrpcClient = clients
	return vml
}

func testNodeNames(nodeCount int) []string {
	nodes := make([]string, 0, nodeCount)
	for i := 1; i <= nodeCount; i++ {
		nodes = append(nodes, fmt.Sprintf("node-%d", i))
	}
	return nodes
}

func testClientsForNodes(nodes []string) map[string]pb.KhalaKnativeIntegrationClient {
	clients := make(map[string]pb.KhalaKnativeIntegrationClient, len(nodes))
	for _, node := range nodes {
		clients[node] = &fakeKhalaClient{}
	}
	return clients
}

func seedIdleVMsForTest(vml *VMList, perNode int) int {
	total := 0

	vml.Lock.Lock()
	defer vml.Lock.Unlock()

	nowMs := time.Now().UnixMilli()
	for _, node := range vml.Nodes {
		for i := 0; i < perNode; i++ {
			vm := &VMMetadata{
				ID:             fmt.Sprintf("%s-vm-%d", node, i),
				Node:           node,
				RPCPort:        "8080",
				LastTimeUsedMs: nowMs,
			}
			vml.appendIdleVMLocked(vm)
		}
		vml.NodeVMCount[node] += perNode
		if vml.nodeLoadTracker != nil {
			vml.nodeLoadTracker.AddLiveVM(node, int64(perNode))
		}
		total += perNode
	}
	vml.TotalVMCount += total
	vml.reconcileCountsLocked("seed-idle-vms")
	return total
}

func TestPopVMChoosesLeastBusyNodeAndPreservesMRUWithinNode(t *testing.T) {
	tracker := NewNodeLoadTracker([]string{"node-1", "node-2"})
	vml := newMultiNodeTestVMList(
		[]string{"node-1", "node-2"},
		RevisionScaleInfo{MaxScale: 5},
		tracker,
		map[string]pb.KhalaKnativeIntegrationClient{
			"node-1": &fakeKhalaClient{},
			"node-2": &fakeKhalaClient{},
		},
	)

	tracker.AddActiveRequests("node-1", 2)

	vml.PushVM(&VMMetadata{ID: "node1-vm", Node: "node-1", RPCPort: "8080"}, true)
	vml.PushVM(&VMMetadata{ID: "node2-vm-1", Node: "node-2", RPCPort: "8080"}, true)
	vml.PushVM(&VMMetadata{ID: "node2-vm-2", Node: "node-2", RPCPort: "8080"}, true)

	got := vml.PopVM()
	if got == nil {
		t.Fatal("PopVM() = nil, want vm")
	}
	if got.Node != "node-2" || got.ID != "node2-vm-2" {
		t.Fatalf("PopVM() = (%q,%q), want (%q,%q)", got.Node, got.ID, "node-2", "node2-vm-2")
	}

	load, ok := tracker.Load("node-2")
	if !ok {
		t.Fatal("tracker missing node-2")
	}
	if load.ActiveRequests != 1 {
		t.Fatalf("node-2 active requests = %d, want 1 after PopVM()", load.ActiveRequests)
	}
}

func TestSingleNodePushPopParityUsesPerNodeIdlePool(t *testing.T) {
	vml := newTestVMList(5, 2, &fakeKhalaClient{})

	vml.PushVM(&VMMetadata{ID: "vm-1", Node: "node-1", RPCPort: "8080"}, true)
	vml.PushVM(&VMMetadata{ID: "vm-2", Node: "node-1", RPCPort: "8080"}, true)

	got := vml.PopVM()
	if got == nil {
		t.Fatal("PopVM() = nil, want vm")
	}
	if got.ID != "vm-2" {
		t.Fatalf("PopVM() ID = %q, want %q", got.ID, "vm-2")
	}
}

func TestAcquireVMTracksActiveRequestsOnReleaseAndInvalidate(t *testing.T) {
	tracker := NewNodeLoadTracker([]string{"node-1"})
	vml := newMultiNodeTestVMList(
		[]string{"node-1"},
		RevisionScaleInfo{MaxScale: 5},
		tracker,
		map[string]pb.KhalaKnativeIntegrationClient{
			"node-1": &fakeKhalaClient{},
		},
	)

	vml.PushVM(&VMMetadata{ID: "vm-release", Node: "node-1", RPCPort: "8080"}, true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	release, vm := vml.AcquireVM(ctx)
	if vm == nil {
		t.Fatal("AcquireVM() returned nil vm")
	}
	load, ok := tracker.Load("node-1")
	if !ok {
		t.Fatal("tracker missing node-1")
	}
	if load.ActiveRequests != 1 {
		t.Fatalf("active requests after AcquireVM = %d, want 1", load.ActiveRequests)
	}
	release()
	load, _ = tracker.Load("node-1")
	if load.ActiveRequests != 0 {
		t.Fatalf("active requests after release = %d, want 0", load.ActiveRequests)
	}

	vml.PushVM(&VMMetadata{ID: "vm-invalidate", Node: "node-1", RPCPort: "8080"}, true)
	release2, vm2 := vml.AcquireVM(ctx)
	if vm2 == nil {
		t.Fatal("second AcquireVM() returned nil vm")
	}
	if release2 != nil {
		// The invalidate path should own the return logic for this checkout.
		release2 = nil
	}
	vml.InvalidateVM(vm2)
	load, _ = tracker.Load("node-1")
	if load.ActiveRequests != 0 {
		t.Fatalf("active requests after invalidate = %d, want 0", load.ActiveRequests)
	}
	vml.Lock.RLock()
	defer vml.Lock.RUnlock()
	if got := vml.idleVMCountLocked(); got != 2 {
		t.Fatalf("idle VM count after release+invalidate = %d, want 2", got)
	}
}

func TestNewNodeLoadTrackerInitializesZeroOneAndManyNodes(t *testing.T) {
	tests := []struct {
		name  string
		nodes []string
	}{{
		name:  "zero nodes",
		nodes: nil,
	}, {
		name:  "one node",
		nodes: []string{"node-1"},
	}, {
		name:  "many nodes",
		nodes: []string{"node-1", "node-2", "node-3"},
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tracker := NewNodeLoadTracker(tc.nodes)
			if tracker == nil {
				t.Fatal("NewNodeLoadTracker() returned nil")
			}
			if got := tracker.NodeCount(); got != len(tc.nodes) {
				t.Fatalf("NodeCount() = %d, want %d", got, len(tc.nodes))
			}
			if diff := cmp.Diff(tc.nodes, tracker.Nodes()); diff != "" {
				t.Fatalf("Nodes mismatch (-want,+got):\n%s", diff)
			}
			for _, node := range tc.nodes {
				load, ok := tracker.Load(node)
				if !ok {
					t.Fatalf("Load(%q) missing node", node)
				}
				if load != (NodeLoad{}) {
					t.Fatalf("Load(%q) = %#v, want zero load", node, load)
				}
			}
		})
	}
}

func TestNodeLoadTrackerCounterUpdates(t *testing.T) {
	tracker := NewNodeLoadTracker([]string{"node-1", "node-2"})

	if ok := tracker.AddLiveVM("node-1", 2); !ok {
		t.Fatal("AddLiveVM(node-1) = false, want true")
	}
	if ok := tracker.AddCreateInFlight("node-1", 1); !ok {
		t.Fatal("AddCreateInFlight(node-1) = false, want true")
	}
	if ok := tracker.AddActiveRequests("node-1", 3); !ok {
		t.Fatal("AddActiveRequests(node-1) = false, want true")
	}
	if ok := tracker.AddActiveRequests("node-1", -1); !ok {
		t.Fatal("AddActiveRequests(node-1, -1) = false, want true")
	}

	load, ok := tracker.Load("node-1")
	if !ok {
		t.Fatal("Load(node-1) = missing node")
	}
	want := NodeLoad{
		LiveVMs:        2,
		CreateInFlight: 1,
		ActiveRequests: 2,
	}
	if diff := cmp.Diff(want, load); diff != "" {
		t.Fatalf("Load(node-1) mismatch (-want,+got):\n%s", diff)
	}

	if ok := tracker.AddLiveVM("missing", 1); ok {
		t.Fatal("AddLiveVM(missing) = true, want false")
	}
	if ok := tracker.AddCreateInFlight("missing", 1); ok {
		t.Fatal("AddCreateInFlight(missing) = true, want false")
	}
	if ok := tracker.AddActiveRequests("missing", 1); ok {
		t.Fatal("AddActiveRequests(missing) = true, want false")
	}
	if _, ok := tracker.Load("missing"); ok {
		t.Fatal("Load(missing) = found, want missing")
	}
}

func TestNodeLoadTrackerUpdateCallbackReportsCurrentAndUpdatedLoads(t *testing.T) {
	tracker := NewNodeLoadTracker([]string{"node-1", "node-2"})

	type report struct {
		node string
		load NodeLoad
	}
	var (
		mu      sync.Mutex
		reports []report
	)

	tracker.SetLoadUpdateFunc(func(node string, load NodeLoad) {
		mu.Lock()
		reports = append(reports, report{node: node, load: load})
		mu.Unlock()
	})

	mu.Lock()
	initialReports := append([]report(nil), reports...)
	mu.Unlock()
	if len(initialReports) != 2 {
		t.Fatalf("initial report count = %d, want 2", len(initialReports))
	}
	if initialReports[0].node != "node-1" || initialReports[0].load != (NodeLoad{}) {
		t.Fatalf("initial report[0] = %#v, want node-1 zero load", initialReports[0])
	}
	if initialReports[1].node != "node-2" || initialReports[1].load != (NodeLoad{}) {
		t.Fatalf("initial report[1] = %#v, want node-2 zero load", initialReports[1])
	}

	if ok := tracker.AddLiveVM("node-1", 2); !ok {
		t.Fatal("AddLiveVM(node-1) = false, want true")
	}
	if ok := tracker.AddCreateInFlight("node-1", 1); !ok {
		t.Fatal("AddCreateInFlight(node-1) = false, want true")
	}
	if ok := tracker.AddActiveRequests("node-1", 3); !ok {
		t.Fatal("AddActiveRequests(node-1) = false, want true")
	}

	mu.Lock()
	lastReport := reports[len(reports)-1]
	mu.Unlock()
	if lastReport.node != "node-1" {
		t.Fatalf("last report node = %q, want %q", lastReport.node, "node-1")
	}
	if diff := cmp.Diff(NodeLoad{
		LiveVMs:        2,
		CreateInFlight: 1,
		ActiveRequests: 3,
	}, lastReport.load); diff != "" {
		t.Fatalf("last report load mismatch (-want,+got):\n%s", diff)
	}
}

func TestNewRevVMListUsesProvidedNodeLoadTracker(t *testing.T) {
	tracker := NewNodeLoadTracker([]string{"node-1", "node-2"})
	vml := NewRevVMList("test-workload", []string{"node-1", "node-2"}, RevisionScaleInfo{MaxScale: 5}, zap.NewNop().Sugar(), nil, tracker)
	if vml.nodeLoadTracker != tracker {
		t.Fatal("NewRevVMList() did not retain provided node load tracker")
	}
}

func TestClampCreateConcurrency(t *testing.T) {
	tests := []struct {
		in   int
		want int
	}{{
		in:   0,
		want: 2,
	}, {
		in:   2,
		want: 2,
	}, {
		in:   3,
		want: 3,
	}, {
		in:   10,
		want: 5,
	}}
	for _, tc := range tests {
		if got := clampCreateConcurrency(tc.in); got != tc.want {
			t.Fatalf("clampCreateConcurrency(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestComputeCreateConcurrency(t *testing.T) {
	tests := []struct {
		name     string
		perNode  int
		nodeCnt  int
		expected int
	}{{
		name:     "single node",
		perNode:  3,
		nodeCnt:  1,
		expected: 3,
	}, {
		name:     "three nodes",
		perNode:  3,
		nodeCnt:  3,
		expected: 9,
	}, {
		name:     "zero nodes falls back to one",
		perNode:  3,
		nodeCnt:  0,
		expected: 3,
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := computeCreateConcurrency(tc.perNode, tc.nodeCnt); got != tc.expected {
				t.Fatalf("computeCreateConcurrency(%d,%d) = %d, want %d",
					tc.perNode, tc.nodeCnt, got, tc.expected)
			}
		})
	}
}

func TestQueuedDepth(t *testing.T) {
	tests := []struct {
		name     string
		inFlight int
		capacity int
		want     int
	}{{
		name:     "no queue when below capacity",
		inFlight: 2,
		capacity: 5,
		want:     0,
	}, {
		name:     "no queue when equal to capacity",
		inFlight: 3,
		capacity: 3,
		want:     0,
	}, {
		name:     "queue when above capacity",
		inFlight: 9,
		capacity: 4,
		want:     5,
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := queuedDepth(tc.inFlight, tc.capacity); got != tc.want {
				t.Fatalf("queuedDepth(%d,%d) = %d, want %d",
					tc.inFlight, tc.capacity, got, tc.want)
			}
		})
	}
}

func TestAcquireVMRespectsCreateConcurrencyLimit(t *testing.T) {
	createConcurrency := 3
	client := &fakeKhalaClient{addDelay: 40 * time.Millisecond}
	vml := newTestVMList(200, createConcurrency, client)

	const requests = 18
	start := make(chan struct{})
	errCh := make(chan error, requests)
	var wg sync.WaitGroup

	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			<-start
			release, vm := vml.AcquireVM(ctx)
			if vm == nil {
				errCh <- fmt.Errorf("AcquireVM returned nil vm")
				return
			}
			if release != nil {
				release()
			}
			errCh <- nil
		}()
	}

	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	if got := client.MaxConcurrentAdds(); got > int64(createConcurrency) {
		t.Fatalf("max concurrent creates = %d, want <= %d", got, createConcurrency)
	}
}

func TestAcquireVMQueuesWhenCreatePermitsAreBusy(t *testing.T) {
	started := make(chan struct{}, 1)
	client := &fakeKhalaClient{
		addDelay: 120 * time.Millisecond,
		onAddStart: func() {
			select {
			case started <- struct{}{}:
			default:
			}
		},
	}
	vml := newTestVMList(20, 1, client)

	holdFirstRelease := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()

		release, vm := vml.AcquireVM(ctx)
		if vm == nil {
			firstDone <- fmt.Errorf("first AcquireVM returned nil vm")
			return
		}
		<-holdFirstRelease
		if release != nil {
			release()
		}
		firstDone <- nil
	}()

	select {
	case <-started:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for first create to start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	begin := time.Now()
	releaseSecond, secondVM := vml.AcquireVM(ctx)
	waited := time.Since(begin)
	if secondVM == nil {
		t.Fatal("second AcquireVM returned nil vm")
	}
	if waited < 100*time.Millisecond {
		t.Fatalf("second request did not queue as expected, waited only %v", waited)
	}
	if releaseSecond != nil {
		releaseSecond()
	}

	close(holdFirstRelease)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}

func TestAcquireVMTimeoutDoesNotCancelCreate(t *testing.T) {
	client := &fakeKhalaClient{addDelay: 120 * time.Millisecond}
	vml := newTestVMList(20, 1, client)

	shortCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	release, vm := vml.AcquireVM(shortCtx)
	if release != nil {
		release()
	}
	if vm != nil {
		t.Fatalf("expected first request to time out waiting for VM, got vm %v", vm.ID)
	}

	time.Sleep(160 * time.Millisecond)

	longCtx, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	start := time.Now()
	release2, vm2 := vml.AcquireVM(longCtx)
	if vm2 == nil {
		t.Fatal("expected detached create to return VM to pool")
	}
	if took := time.Since(start); took > 80*time.Millisecond {
		t.Fatalf("expected near-immediate VM acquisition after detached create, took %v", took)
	}
	if release2 != nil {
		release2()
	}
}

func TestAcquireVMMixedOrderingReq1WaitsReq2Req3UseAvailable(t *testing.T) {
	addStarted := make(chan struct{}, 1)
	client := &fakeKhalaClient{
		addDelay: 200 * time.Millisecond,
		onAddStart: func() {
			select {
			case addStarted <- struct{}{}:
			default:
			}
		},
	}
	vml := newTestVMList(20, 1, client)

	type reqResult struct {
		who string
		id  string
		at  time.Time
		err error
	}
	results := make(chan reqResult, 3)
	startReq := func(who string) {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			release, vm := vml.AcquireVM(ctx)
			if vm == nil {
				results <- reqResult{who: who, err: fmt.Errorf("%s got nil vm", who)}
				return
			}
			if release != nil {
				release()
			}
			results <- reqResult{who: who, id: vm.ID, at: time.Now()}
		}()
	}

	startReq("req1")
	select {
	case <-addStarted:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for req1 create to start")
	}

	// Make two VMs available while req1 is waiting on its own create.
	vml.PushVM(&VMMetadata{ID: "pre-1", Node: "node-1", RPCPort: "8080"}, true)
	vml.PushVM(&VMMetadata{ID: "pre-2", Node: "node-1", RPCPort: "8080"}, true)

	startReq("req2")
	startReq("req3")

	got := make(map[string]reqResult, 3)
	for i := 0; i < 3; i++ {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		got[r.who] = r
	}

	if got["req1"].id == "pre-1" || got["req1"].id == "pre-2" {
		t.Fatalf("req1 should wait for created VM, got preexisting VM %q", got["req1"].id)
	}
	validPre := map[string]struct{}{"pre-1": {}, "pre-2": {}}
	if _, ok := validPre[got["req2"].id]; !ok {
		t.Fatalf("req2 should use a preexisting available VM, got %q", got["req2"].id)
	}
	if _, ok := validPre[got["req3"].id]; !ok {
		t.Fatalf("req3 should use a preexisting available VM, got %q", got["req3"].id)
	}
	if !got["req2"].at.Before(got["req1"].at) || !got["req3"].at.Before(got["req1"].at) {
		t.Fatalf("req2/req3 should complete before req1: req1=%v req2=%v req3=%v",
			got["req1"].at, got["req2"].at, got["req3"].at)
	}
}

func TestDynamicAdmissionCapacityTracksVMLifecycle(t *testing.T) {
	client := &fakeKhalaClient{addDelay: 10 * time.Millisecond}
	var lastCapacity int64
	vml := newTestVMListWithCallback(5, 2, client, func(cap int) {
		atomic.StoreInt64(&lastCapacity, int64(cap))
	})

	vml.Lock.Lock()
	vml.updateAdmissionCapacityLocked()
	vml.Lock.Unlock()
	if got := atomic.LoadInt64(&lastCapacity); got != 1 {
		t.Fatalf("initial admission capacity = %d, want 1", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	vm, err := vml.CreateVM(ctx)
	if err != nil {
		t.Fatalf("CreateVM() err = %v", err)
	}
	vml.PushVM(vm, true)
	if got := atomic.LoadInt64(&lastCapacity); got != 2 {
		t.Fatalf("admission capacity after first VM = %d, want 2", got)
	}

	vm2, err := vml.CreateVM(ctx)
	if err != nil {
		t.Fatalf("second CreateVM() err = %v", err)
	}
	if got := atomic.LoadInt64(&lastCapacity); got != 3 {
		t.Fatalf("admission capacity after second VM = %d, want 3", got)
	}

	vm2.RetryCount = 3
	client.removeSet = true
	client.removeOK = true
	vml.InvalidateVM(vm2)
	eventually(t, 2*time.Second, 10*time.Millisecond, func() bool {
		vml.Lock.RLock()
		defer vml.Lock.RUnlock()
		return vml.TotalVMCount == 1
	})
	if got := atomic.LoadInt64(&lastCapacity); got != 2 {
		t.Fatalf("admission capacity after remove = %d, want 2", got)
	}
}

func TestAcquireVMReleaseParallelMaintainsTrackerCounts(t *testing.T) {
	for _, nodeCount := range []int{1, 4, 8} {
		t.Run(fmt.Sprintf("%d-nodes", nodeCount), func(t *testing.T) {
			nodes := testNodeNames(nodeCount)
			tracker := NewNodeLoadTracker(nodes)
			perNodeIdle := 16
			totalVMs := nodeCount * perNodeIdle
			vml := newMultiNodeTestVMList(
				nodes,
				RevisionScaleInfo{MaxScale: totalVMs * 2},
				tracker,
				testClientsForNodes(nodes),
			)
			seedIdleVMsForTest(vml, perNodeIdle)

			const iterationsPerWorker = 32
			workers := nodeCount * 8
			start := make(chan struct{})
			errCh := make(chan error, workers)
			var wg sync.WaitGroup

			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()

					<-start
					for j := 0; j < iterationsPerWorker; j++ {
						release, vm := vml.AcquireVM(ctx)
						if vm == nil {
							errCh <- fmt.Errorf("AcquireVM returned nil vm")
							return
						}
						if release == nil {
							errCh <- fmt.Errorf("AcquireVM returned nil release func for vm %s", vm.ID)
							return
						}
						release()
					}
					errCh <- nil
				}()
			}

			close(start)
			wg.Wait()
			close(errCh)

			for err := range errCh {
				if err != nil {
					t.Fatal(err)
				}
			}

			vml.Lock.RLock()
			if got := vml.TotalVMCount; got != totalVMs {
				vml.Lock.RUnlock()
				t.Fatalf("TotalVMCount = %d, want %d", got, totalVMs)
			}
			if got := vml.idleVMCountLocked(); got != totalVMs {
				vml.Lock.RUnlock()
				t.Fatalf("idle VM count = %d, want %d", got, totalVMs)
			}
			for _, node := range nodes {
				if got := vml.NodeVMCount[node]; got != perNodeIdle {
					vml.Lock.RUnlock()
					t.Fatalf("NodeVMCount[%s] = %d, want %d", node, got, perNodeIdle)
				}
			}
			vml.Lock.RUnlock()

			for _, node := range nodes {
				load, ok := tracker.Load(node)
				if !ok {
					t.Fatalf("tracker missing %s", node)
				}
				want := NodeLoad{LiveVMs: int64(perNodeIdle)}
				if diff := cmp.Diff(want, load); diff != "" {
					t.Fatalf("%s tracker load mismatch (-want,+got):\n%s", node, diff)
				}
			}
		})
	}
}

func BenchmarkAcquireVMRelease(b *testing.B) {
	for _, nodeCount := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("%d-nodes", nodeCount), func(b *testing.B) {
			nodes := testNodeNames(nodeCount)
			tracker := NewNodeLoadTracker(nodes)
			perNodeIdle := runtime.GOMAXPROCS(0) * 16
			if perNodeIdle < 256 {
				perNodeIdle = 256
			}
			vml := newMultiNodeTestVMList(
				nodes,
				RevisionScaleInfo{MaxScale: nodeCount * perNodeIdle * 2},
				tracker,
				testClientsForNodes(nodes),
			)
			totalVMs := seedIdleVMsForTest(vml, perNodeIdle)
			ctx := context.Background()

			b.ReportAllocs()
			b.SetParallelism(4)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					release, vm := vml.AcquireVM(ctx)
					if vm == nil || release == nil {
						panic("AcquireVM returned nil during benchmark")
					}
					release()
				}
			})
			b.StopTimer()

			vml.Lock.RLock()
			if got := vml.TotalVMCount; got != totalVMs {
				vml.Lock.RUnlock()
				b.Fatalf("TotalVMCount = %d, want %d", got, totalVMs)
			}
			if got := vml.idleVMCountLocked(); got != totalVMs {
				vml.Lock.RUnlock()
				b.Fatalf("idle VM count = %d, want %d", got, totalVMs)
			}
			vml.Lock.RUnlock()

			for _, node := range nodes {
				load, ok := tracker.Load(node)
				if !ok {
					b.Fatalf("tracker missing %s", node)
				}
				if load.ActiveRequests != 0 || load.CreateInFlight != 0 {
					b.Fatalf("%s tracker active/create load = (%d,%d), want (0,0)", node, load.ActiveRequests, load.CreateInFlight)
				}
			}
		})
	}
}

func TestCreateVMUsesSharedNodeTrackerAcrossRevisions(t *testing.T) {
	prev := initialNextNodeIndexFunc
	initialNextNodeIndexFunc = func(int) int { return 0 }
	defer func() {
		initialNextNodeIndexFunc = prev
	}()

	tracker := NewNodeLoadTracker([]string{"node-1", "node-2"})
	clients := map[string]pb.KhalaKnativeIntegrationClient{
		"node-1": &fakeKhalaClient{},
		"node-2": &fakeKhalaClient{},
	}
	vml1 := newMultiNodeTestVMList([]string{"node-1", "node-2"}, RevisionScaleInfo{MaxScale: 10}, tracker, clients)
	vml2 := newMultiNodeTestVMList([]string{"node-1", "node-2"}, RevisionScaleInfo{MaxScale: 10}, tracker, clients)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	vm1, err := vml1.CreateVM(ctx)
	if err != nil {
		t.Fatalf("first CreateVM() err = %v", err)
	}
	vm2, err := vml2.CreateVM(ctx)
	if err != nil {
		t.Fatalf("second CreateVM() err = %v", err)
	}
	if vm1.Node == vm2.Node {
		t.Fatalf("shared tracker placed both VMs on %q, want spread across nodes", vm1.Node)
	}

	load1, ok := tracker.Load("node-1")
	if !ok {
		t.Fatal("tracker missing node-1")
	}
	load2, ok := tracker.Load("node-2")
	if !ok {
		t.Fatal("tracker missing node-2")
	}
	if load1.LiveVMs != 1 || load2.LiveVMs != 1 {
		t.Fatalf("shared live VM counts = (%d,%d), want (1,1)", load1.LiveVMs, load2.LiveVMs)
	}
	if load1.CreateInFlight != 0 || load2.CreateInFlight != 0 {
		t.Fatalf("shared create-inflight counts = (%d,%d), want (0,0)", load1.CreateInFlight, load2.CreateInFlight)
	}
}

func TestInitialScaleUpUsesSharedNodeTrackerPlacement(t *testing.T) {
	prev := initialNextNodeIndexFunc
	initialNextNodeIndexFunc = func(int) int { return 0 }
	defer func() {
		initialNextNodeIndexFunc = prev
	}()

	tracker := NewNodeLoadTracker([]string{"node-1", "node-2"})
	clients := map[string]pb.KhalaKnativeIntegrationClient{
		"node-1": &fakeKhalaClient{},
		"node-2": &fakeKhalaClient{},
	}
	vml := newMultiNodeTestVMList([]string{"node-1", "node-2"}, RevisionScaleInfo{MaxScale: 10, InitialScale: 2}, tracker, clients)

	vml.InitialScaleUp()

	load1, ok := tracker.Load("node-1")
	if !ok {
		t.Fatal("tracker missing node-1")
	}
	load2, ok := tracker.Load("node-2")
	if !ok {
		t.Fatal("tracker missing node-2")
	}
	if load1.LiveVMs != 1 || load2.LiveVMs != 1 {
		t.Fatalf("shared live VM counts after InitialScaleUp = (%d,%d), want (1,1)", load1.LiveVMs, load2.LiveVMs)
	}
}

func TestCreateVMErrorDoesNotCorruptCounts(t *testing.T) {
	client := &fakeKhalaClient{addErr: fmt.Errorf("create failed")}
	vml := newTestVMList(5, 2, client)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := vml.CreateVM(ctx); err == nil {
		t.Fatal("CreateVM() err = nil, want error")
	}

	vml.Lock.RLock()
	defer vml.Lock.RUnlock()
	if got := vml.TotalVMCount; got != 0 {
		t.Fatalf("TotalVMCount = %d, want 0", got)
	}
	if got := vml.NodeVMCount["node-1"]; got != 0 {
		t.Fatalf("NodeVMCount[node-1] = %d, want 0", got)
	}
	if got := vml.CreateInFlight; got != 0 {
		t.Fatalf("CreateInFlight = %d, want 0", got)
	}
	if got := vml.NodeCreateInFlight["node-1"]; got != 0 {
		t.Fatalf("NodeCreateInFlight[node-1] = %d, want 0", got)
	}
	load, ok := vml.nodeLoadTracker.Load("node-1")
	if !ok {
		t.Fatal("nodeLoadTracker missing node-1")
	}
	if diff := cmp.Diff(NodeLoad{}, load); diff != "" {
		t.Fatalf("shared node load mismatch (-want,+got):\n%s", diff)
	}
}

func TestCreateVMFirstTieUsesSeededInitialNodeOffset(t *testing.T) {
	prev := initialNextNodeIndexFunc
	initialNextNodeIndexFunc = func(nodeCount int) int {
		if nodeCount != 2 {
			t.Fatalf("initialNextNodeIndexFunc got nodeCount=%d, want 2", nodeCount)
		}
		return 1
	}
	defer func() {
		initialNextNodeIndexFunc = prev
	}()

	vml := NewRevVMList("test-workload", []string{"node-1", "node-2"}, RevisionScaleInfo{MaxScale: 5}, zap.NewNop().Sugar(), nil, nil)
	vml.khalaGrpcClient = map[string]pb.KhalaKnativeIntegrationClient{
		"node-1": &fakeKhalaClient{},
		"node-2": &fakeKhalaClient{},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	vm, err := vml.CreateVM(ctx)
	if err != nil {
		t.Fatalf("CreateVM() err = %v", err)
	}
	if vm.Node != "node-2" {
		t.Fatalf("first CreateVM() node = %q, want %q", vm.Node, "node-2")
	}
}

func TestCreateVMInvalidResponseDoesNotCorruptCounts(t *testing.T) {
	client := &fakeKhalaClient{
		addResp: &pb.AddVMResponse{
			Success: false,
		},
	}
	vml := newTestVMList(5, 2, client)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := vml.CreateVM(ctx); err == nil {
		t.Fatal("CreateVM() err = nil, want error for invalid AddVM response")
	}

	vml.Lock.RLock()
	defer vml.Lock.RUnlock()
	if got := vml.TotalVMCount; got != 0 {
		t.Fatalf("TotalVMCount = %d, want 0", got)
	}
	if got := vml.NodeVMCount["node-1"]; got != 0 {
		t.Fatalf("NodeVMCount[node-1] = %d, want 0", got)
	}
	if got := vml.CreateInFlight; got != 0 {
		t.Fatalf("CreateInFlight = %d, want 0", got)
	}
	if got := vml.NodeCreateInFlight["node-1"]; got != 0 {
		t.Fatalf("NodeCreateInFlight[node-1] = %d, want 0", got)
	}
	load, ok := vml.nodeLoadTracker.Load("node-1")
	if !ok {
		t.Fatal("nodeLoadTracker missing node-1")
	}
	if diff := cmp.Diff(NodeLoad{}, load); diff != "" {
		t.Fatalf("shared node load mismatch (-want,+got):\n%s", diff)
	}
}

func TestVMCountUpdateCallbackReportsCreateAndRemoveCounts(t *testing.T) {
	client := &fakeKhalaClient{removeSet: true, removeOK: true}
	vml := newTestVMList(5, 2, client)

	type vmCountReport struct {
		total   int
		perNode map[string]int
	}

	var (
		mu      sync.Mutex
		reports []vmCountReport
	)
	lastReport := func() vmCountReport {
		mu.Lock()
		defer mu.Unlock()
		return reports[len(reports)-1]
	}

	vml.SetVMCountUpdateFunc(func(total int, perNode map[string]int) {
		copied := make(map[string]int, len(perNode))
		for node, count := range perNode {
			copied[node] = count
		}
		mu.Lock()
		reports = append(reports, vmCountReport{total: total, perNode: copied})
		mu.Unlock()
	})

	if got := lastReport(); got.total != 0 || got.perNode["node-1"] != 0 {
		t.Fatalf("initial VM count report = %#v, want zero counts", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	vm, err := vml.CreateVM(ctx)
	if err != nil {
		t.Fatalf("CreateVM() err = %v", err)
	}

	if got := lastReport(); got.total != 1 || got.perNode["node-1"] != 1 {
		t.Fatalf("post-create VM count report = %#v, want total=1 node-1=1", got)
	}

	vm.RetryCount = 3
	vml.InvalidateVM(vm)

	if got := lastReport(); got.total != 0 || got.perNode["node-1"] != 0 {
		t.Fatalf("post-invalidate VM count report = %#v, want zero counts", got)
	}
}

func TestInvalidateVMRemoveFailureTreatsVMAsDead(t *testing.T) {
	prevRetryDelay := removeVMRetryDelay
	prevRemoveTimeout := removeVMTimeout
	removeVMRetryDelay = 5 * time.Millisecond
	removeVMTimeout = 20 * time.Millisecond
	defer func() {
		removeVMRetryDelay = prevRetryDelay
		removeVMTimeout = prevRemoveTimeout
	}()

	client := &fakeKhalaClient{removeErr: fmt.Errorf("remove rpc failed")}
	vml := newTestVMList(5, 2, client)

	vm := &VMMetadata{ID: "vm-bad", Node: "node-1", RPCPort: "8080", RetryCount: 3}
	vml.Lock.Lock()
	vml.TotalVMCount = 1
	vml.NodeVMCount["node-1"] = 1
	vml.Lock.Unlock()

	vml.InvalidateVM(vm)
	eventually(t, 500*time.Millisecond, 10*time.Millisecond, func() bool {
		vml.Lock.RLock()
		defer vml.Lock.RUnlock()
		return vml.TotalVMCount == 0 && vml.NodeVMCount["node-1"] == 0
	})

	vml.Lock.RLock()
	defer vml.Lock.RUnlock()
	if got := vml.TotalVMCount; got != 0 {
		t.Fatalf("TotalVMCount = %d, want 0", got)
	}
	if got := vml.NodeVMCount["node-1"]; got != 0 {
		t.Fatalf("NodeVMCount[node-1] = %d, want 0", got)
	}
	if got := vml.idleVMCountLocked(); got != 0 {
		t.Fatalf("idle VM count = %d, want 0", got)
	}
}

func TestCleanupRemoveFailureTreatsVMAsDead(t *testing.T) {
	prevRetryDelay := removeVMRetryDelay
	prevRemoveTimeout := removeVMTimeout
	removeVMRetryDelay = 5 * time.Millisecond
	removeVMTimeout = 20 * time.Millisecond
	defer func() {
		removeVMRetryDelay = prevRetryDelay
		removeVMTimeout = prevRemoveTimeout
	}()

	client := &fakeKhalaClient{removeErr: fmt.Errorf("remove rpc failed")}
	vml := newTestVMList(5, 2, client)
	vml.keepaliveDurationSec = 1
	vml.updateIntervalSec = 1

	vm := &VMMetadata{
		ID:             "vm-idle",
		Node:           "node-1",
		RPCPort:        "8080",
		LastTimeUsedMs: time.Now().Add(-2 * time.Second).UnixMilli(),
	}

	vml.Lock.Lock()
	vml.appendIdleVMLocked(vm)
	vml.TotalVMCount = 1
	vml.NodeVMCount["node-1"] = 1
	vml.Lock.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vml.RemoveVMsWithLeastRecentUse(ctx)

	eventually(t, 1500*time.Millisecond, 10*time.Millisecond, func() bool {
		vml.Lock.RLock()
		defer vml.Lock.RUnlock()
		return vml.TotalVMCount == 0 && vml.NodeVMCount["node-1"] == 0 && vml.idleVMCountLocked() == 0
	})
}

func TestCleanupPrefersMostOverprovisionedNode(t *testing.T) {
	tracker := NewNodeLoadTracker([]string{"node-1", "node-2"})
	clients := map[string]pb.KhalaKnativeIntegrationClient{
		"node-1": &fakeKhalaClient{removeSet: true, removeOK: true},
		"node-2": &fakeKhalaClient{removeSet: true, removeOK: true},
	}
	vml := newMultiNodeTestVMList(
		[]string{"node-1", "node-2"},
		RevisionScaleInfo{MaxScale: 5, MinScale: 1},
		tracker,
		clients,
	)
	vml.keepaliveDurationSec = 1
	vml.updateIntervalSec = 1

	idleAt := time.Now().Add(-2 * time.Second).UnixMilli()
	vmNode1 := &VMMetadata{
		ID:             "vm-node-1",
		Node:           "node-1",
		RPCPort:        "8080",
		LastTimeUsedMs: idleAt,
	}
	vmNode2 := &VMMetadata{
		ID:             "vm-node-2",
		Node:           "node-2",
		RPCPort:        "8080",
		LastTimeUsedMs: idleAt,
	}

	vml.Lock.Lock()
	vml.appendIdleVMLocked(vmNode1)
	vml.appendIdleVMLocked(vmNode2)
	vml.TotalVMCount = 2
	vml.NodeVMCount["node-1"] = 1
	vml.NodeVMCount["node-2"] = 1
	vml.Lock.Unlock()

	tracker.AddLiveVM("node-1", 4)
	tracker.AddLiveVM("node-2", 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vml.RemoveVMsWithLeastRecentUse(ctx)

	eventually(t, 1500*time.Millisecond, 10*time.Millisecond, func() bool {
		vml.Lock.RLock()
		defer vml.Lock.RUnlock()
		return vml.TotalVMCount == 1 && vml.NodeVMCount["node-1"] == 0 && vml.NodeVMCount["node-2"] == 1
	})

	vml.Lock.RLock()
	defer vml.Lock.RUnlock()
	if got := len(vml.idleVMsByNode["node-1"]); got != 0 {
		t.Fatalf("idle VM count on node-1 = %d, want 0", got)
	}
	if got := len(vml.idleVMsByNode["node-2"]); got != 1 {
		t.Fatalf("idle VM count on node-2 = %d, want 1", got)
	}
	if got := vml.idleVMsByNode["node-2"][0].ID; got != "vm-node-2" {
		t.Fatalf("remaining idle VM = %q, want %q", got, "vm-node-2")
	}

	loadNode1, ok := tracker.Load("node-1")
	if !ok {
		t.Fatal("tracker missing node-1")
	}
	if loadNode1.LiveVMs != 3 {
		t.Fatalf("node-1 live VMs = %d, want 3 after cleanup", loadNode1.LiveVMs)
	}
	loadNode2, ok := tracker.Load("node-2")
	if !ok {
		t.Fatal("tracker missing node-2")
	}
	if loadNode2.LiveVMs != 1 {
		t.Fatalf("node-2 live VMs = %d, want 1 after cleanup", loadNode2.LiveVMs)
	}
}

func TestCleanupDoesNotScaleBelowMinScaleInSinglePass(t *testing.T) {
	client := &fakeKhalaClient{removeSet: true, removeOK: true}
	vml := newTestVMList(5, 2, client)
	vml.revScale.MinScale = 1
	vml.keepaliveDurationSec = 1
	vml.updateIntervalSec = 1

	idleAt := time.Now().Add(-2 * time.Second).UnixMilli()
	vms := []*VMMetadata{{
		ID:             "vm-idle-1",
		Node:           "node-1",
		RPCPort:        "8080",
		LastTimeUsedMs: idleAt,
	}, {
		ID:             "vm-idle-2",
		Node:           "node-1",
		RPCPort:        "8080",
		LastTimeUsedMs: idleAt,
	}, {
		ID:             "vm-idle-3",
		Node:           "node-1",
		RPCPort:        "8080",
		LastTimeUsedMs: idleAt,
	}}

	vml.Lock.Lock()
	for _, vm := range vms {
		vml.appendIdleVMLocked(vm)
	}
	vml.TotalVMCount = len(vms)
	vml.NodeVMCount["node-1"] = len(vms)
	vml.Lock.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vml.RemoveVMsWithLeastRecentUse(ctx)

	eventually(t, 1500*time.Millisecond, 10*time.Millisecond, func() bool {
		vml.Lock.RLock()
		defer vml.Lock.RUnlock()
		return vml.TotalVMCount == 1 && vml.NodeVMCount["node-1"] == 1 && vml.idleVMCountLocked() == 1
	})
}

func eventually(t *testing.T, timeout, interval time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(interval)
	}
	t.Fatal("condition not met before timeout")
}
