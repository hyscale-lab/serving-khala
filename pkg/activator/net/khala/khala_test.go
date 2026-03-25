package khala

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
		VMs:                  make([]*VMMetadata, 0),
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

	vml := NewRevVMList("test-workload", []string{"node-1", "node-2"}, RevisionScaleInfo{MaxScale: 5}, zap.NewNop().Sugar(), nil)
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
	if got := len(vml.VMs); got != 0 {
		t.Fatalf("len(VMs) = %d, want 0", got)
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
	vml.VMs = append(vml.VMs, vm)
	vml.TotalVMCount = 1
	vml.NodeVMCount["node-1"] = 1
	vml.Lock.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vml.RemoveVMsWithLeastRecentUse(ctx)

	eventually(t, 1500*time.Millisecond, 10*time.Millisecond, func() bool {
		vml.Lock.RLock()
		defer vml.Lock.RUnlock()
		return vml.TotalVMCount == 0 && vml.NodeVMCount["node-1"] == 0 && len(vml.VMs) == 0
	})
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
	vml.VMs = append(vml.VMs, vms...)
	vml.TotalVMCount = len(vms)
	vml.NodeVMCount["node-1"] = len(vms)
	vml.Lock.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vml.RemoveVMsWithLeastRecentUse(ctx)

	eventually(t, 1500*time.Millisecond, 10*time.Millisecond, func() bool {
		vml.Lock.RLock()
		defer vml.Lock.RUnlock()
		return vml.TotalVMCount == 1 && vml.NodeVMCount["node-1"] == 1 && len(vml.VMs) == 1
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
