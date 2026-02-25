package khala

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	khala "knative.dev/serving/pkg/proto"
)

// VMMetadata holds metadata for a single VM
// including its IP and the last time it was used (in milliseconds)
type VMMetadata struct {
	ID             string
	IP             string
	RPCPort        string
	Node           string
	LastTimeUsedMs int64
	RetryCount     int
}

type RevisionScaleInfo struct {
	MinScale     int
	MaxScale     int
	InitialScale int
}

type VMList struct {
	// UPDATED: Changed from a flat slice to a map of slices, keyed by node name.
	VMs                  []*VMMetadata
	Workload             string
	Nodes                []string
	NodeVMCount          map[string]int
	NodeCreateInFlight   map[string]int
	TotalVMCount         int
	CreateInFlight       int
	nextNodeIndex        int
	khalaGrpcClient      map[string]khala.KhalaKnativeIntegrationClient
	keepaliveDurationSec int
	updateIntervalSec    int
	Lock                 sync.RWMutex
	logger               *zap.SugaredLogger
	runClenaupOnce       sync.Once
	revScale             RevisionScaleInfo
	vmAvailableChan      chan struct{}
	createConcurrency    int
	createPermits        chan struct{}
	createTimeoutSec     int
	warmBuffer           int
	capacityUpdateFunc   func(int)
}

func NewRevVMList(extractedName string, nodes []string, revScale RevisionScaleInfo, logger *zap.SugaredLogger, capacityUpdateFunc func(int)) *VMList {
	logger.Infof("khala: initializing VMList with nodes: %v", nodes)

	keepAlive := GetEnv("KEEPALIVE_DURATION", 60)
	updateInt := GetEnv("UPDATE_INTERVAL", 5)
	createConcurrencyPerNode := clampCreateConcurrency(GetEnv("CREATE_CONCURRENCY", 3))
	createConcurrency := computeCreateConcurrency(createConcurrencyPerNode, len(nodes))
	createTimeoutSec := GetEnv("CREATE_TIMEOUT_SECONDS", 300)
	if createTimeoutSec <= 0 {
		createTimeoutSec = 300
	}
	logger.Infof("khala: keepalive duration: %v", keepAlive)
	logger.Infof("khala: update interval: %v", updateInt)
	logger.Infof("khala: create concurrency per node: %v", createConcurrencyPerNode)
	logger.Infof("khala: total create concurrency: %v", createConcurrency)
	logger.Infof("khala: create timeout sec: %v", createTimeoutSec)

	khalaGrpcClient := make(map[string]khala.KhalaKnativeIntegrationClient)
	for _, node := range nodes {
		orchestratorConn, err := grpc.Dial(node+":8000", grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			logger.Errorf("khala: failed to connect to vm-orchestrator on node %s: %v", node, err)
			// Continue instead of fatally exiting to make the system more resilient
			continue
		}
		khalaGrpcClient[node] = khala.NewKhalaKnativeIntegrationClient(orchestratorConn)
	}

	initialCap := revScale.InitialScale
	if initialCap < 1 {
		initialCap = 1
	}
	chanBuf := revScale.MaxScale
	if chanBuf < 1 {
		chanBuf = 1
	}
	VMs := make([]*VMMetadata, 0, initialCap)

	NodeVMCount := make(map[string]int)
	NodeCreateInFlight := make(map[string]int)
	for _, node := range nodes {
		NodeVMCount[node] = 0
		NodeCreateInFlight[node] = 0
	}
	createPermits := make(chan struct{}, createConcurrency)
	for i := 0; i < createConcurrency; i++ {
		createPermits <- struct{}{}
	}

	vml := &VMList{
		VMs:                  VMs,
		Workload:             extractedName,
		Nodes:                nodes,
		NodeVMCount:          NodeVMCount,
		NodeCreateInFlight:   NodeCreateInFlight,
		nextNodeIndex:        0,
		khalaGrpcClient:      khalaGrpcClient,
		keepaliveDurationSec: keepAlive,
		updateIntervalSec:    updateInt,
		logger:               logger,
		runClenaupOnce:       sync.Once{},
		revScale:             revScale,
		vmAvailableChan:      make(chan struct{}, chanBuf),
		createConcurrency:    createConcurrency,
		createPermits:        createPermits,
		createTimeoutSec:     createTimeoutSec,
		warmBuffer:           1,
		capacityUpdateFunc:   capacityUpdateFunc,
	}
	vml.Lock.Lock()
	vml.updateAdmissionCapacityLocked()
	vml.Lock.Unlock()
	return vml
}

func clampCreateConcurrency(v int) int {
	if v < 2 {
		return 2
	}
	if v > 5 {
		return 5
	}
	return v
}

func computeCreateConcurrency(perNode, nodeCount int) int {
	if nodeCount < 1 {
		nodeCount = 1
	}
	return perNode * nodeCount
}

func (vml *VMList) desiredAdmissionCapacityLocked() int {
	if vml.revScale.MaxScale <= 0 {
		return 0
	}
	desired := vml.TotalVMCount + vml.CreateInFlight + vml.warmBuffer
	if desired < 1 {
		desired = 1
	}
	if desired > vml.revScale.MaxScale {
		desired = vml.revScale.MaxScale
	}
	return desired
}

func (vml *VMList) updateAdmissionCapacityLocked() {
	if vml.capacityUpdateFunc == nil {
		return
	}
	vml.capacityUpdateFunc(vml.desiredAdmissionCapacityLocked())
}

func (vml *VMList) reconcileCountsLocked(reason string) {
	availableByNode := make(map[string]int, len(vml.NodeVMCount))
	for _, vm := range vml.VMs {
		availableByNode[vm.Node]++
	}

	sumNode := 0
	for node, count := range vml.NodeVMCount {
		if count < 0 {
			vml.logger.Warnf("khala: reconcile(%s) clamping negative NodeVMCount on %s: %d", reason, node, count)
			count = 0
			vml.NodeVMCount[node] = 0
		}
		avail := availableByNode[node]
		if count < avail {
			vml.logger.Warnf("khala: reconcile(%s) NodeVMCount[%s]=%d < available=%d, correcting", reason, node, count, avail)
			count = avail
			vml.NodeVMCount[node] = count
		}
		sumNode += count
	}
	for node, avail := range availableByNode {
		if _, ok := vml.NodeVMCount[node]; !ok {
			vml.logger.Warnf("khala: reconcile(%s) adding missing node counter for %s=%d", reason, node, avail)
			vml.NodeVMCount[node] = avail
			sumNode += avail
		}
	}

	if vml.TotalVMCount != sumNode {
		vml.logger.Warnf("khala: reconcile(%s) TotalVMCount=%d, sum(NodeVMCount)=%d, correcting",
			reason, vml.TotalVMCount, sumNode)
		vml.TotalVMCount = sumNode
	}

	sumCreate := 0
	for node, count := range vml.NodeCreateInFlight {
		if count < 0 {
			vml.logger.Warnf("khala: reconcile(%s) clamping negative NodeCreateInFlight on %s: %d", reason, node, count)
			count = 0
			vml.NodeCreateInFlight[node] = 0
		}
		sumCreate += count
	}
	if vml.CreateInFlight != sumCreate {
		vml.logger.Warnf("khala: reconcile(%s) CreateInFlight=%d, sum(NodeCreateInFlight)=%d, correcting",
			reason, vml.CreateInFlight, sumCreate)
		vml.CreateInFlight = sumCreate
	}
}

func (vml *VMList) removeVMFromOrchestrator(vm *VMMetadata) bool {
	vml.Lock.RLock()
	client, ok := vml.khalaGrpcClient[vm.Node]
	vml.Lock.RUnlock()
	if !ok {
		vml.logger.Errorf("khala: gRPC client not found for node %s", vm.Node)
		return false
	}
	resp, err := client.RemoveVM(context.Background(), &khala.RemoveVMRequest{VmId: vm.ID})
	if err != nil {
		vml.logger.Warnf("khala: RemoveVM RPC failed for %s: %v", vm.ID, err)
		return false
	}
	if resp == nil || !resp.Success {
		vml.logger.Warnf("khala: RemoveVM unsuccessful for %s: %#v", vm.ID, resp)
		return false
	}
	return true
}

// AcquireVM returns an available VM, creating one inline if none exists (Lambda model).
// Each request is its own VM allocation agent: it checks the MRU pool, and if
// empty and below MaxScale, blocks directly on the gRPC CreateVM call.
// At MaxScale it queues on vmAvailableChan waiting for a PushVM signal.
func (vml *VMList) AcquireVM(ctx context.Context) (func(), *VMMetadata) {
	for {
		// 1. Try to get an idle VM from the MRU pool.
		if vm := vml.PopVM(); vm != nil {
			return func() { vml.PushVM(vm, true) }, vm
		}

		// 2. Capture the wait channel before reading atMax to avoid missing
		//    a PushVM signal that arrives between the atMax check and the select.
		waitCh := vml.vmAvailableChan

		vml.Lock.RLock()
		atMax := vml.revScale.MaxScale != 0 && vml.TotalVMCount >= vml.revScale.MaxScale
		vml.Lock.RUnlock()

		if !atMax {
			// 3. Below MaxScale, only permit holders are allowed to create VMs.
			// Requests that cannot acquire a permit wait in queue. Permit holders
			// wait for their own create result. If the request times out first,
			// create continues detached and returns VM to pool when complete.
			if vml.tryAcquireCreatePermit() {
				vm, detached := vml.createForRequest(ctx)
				if detached {
					return nil, nil
				}
				if vm != nil {
					return func() { vml.PushVM(vm, true) }, vm
				}
				// Fall through to wait/retry on create failure.
			}
		}

		// 4. At MaxScale (or creation failed): wait for a VM to be released.
		select {
		case <-ctx.Done():
			return nil, nil
		case <-waitCh:
			// A VM was returned or a slot opened. Loop to try PopVM again.
		}
	}
}

type createResult struct {
	vm  *VMMetadata
	err error
}

func (vml *VMList) createForRequest(ctx context.Context) (*VMMetadata, bool) {
	createDone := make(chan createResult, 1)
	go func() {
		createCtx, cancel := context.WithTimeout(context.Background(), time.Duration(vml.createTimeoutSec)*time.Second)
		defer cancel()
		vm, err := vml.CreateVM(createCtx)
		createDone <- createResult{vm: vm, err: err}
	}()

	select {
	case <-ctx.Done():
		// Request timed out; continue create asynchronously and return VM to pool.
		go func() {
			res := <-createDone
			vml.releaseCreatePermit()
			vml.signalWaiters()
			if res.err != nil {
				vml.logger.Errorf("khala: detached VM create failed: %v", res.err)
				return
			}
			vml.logger.Infof("khala: detached VM create completed: %v", res.vm)
			vml.PushVM(res.vm, true)
		}()
		return nil, true
	case res := <-createDone:
		vml.releaseCreatePermit()
		vml.signalWaiters()
		if res.err != nil {
			vml.logger.Errorf("khala: failed to create VM: %v", res.err)
			return nil, false
		}
		vml.logger.Infof("khala: created VM inline: %v", res.vm)
		return res.vm, false
	}
}

func (vml *VMList) tryAcquireCreatePermit() bool {
	select {
	case <-vml.createPermits:
		return true
	default:
		return false
	}
}

func (vml *VMList) releaseCreatePermit() {
	select {
	case vml.createPermits <- struct{}{}:
	default:
		vml.logger.Warn("khala: create permit release dropped because limiter is already full")
	}
}

// PopVM finds and pops the most recently used VM from the next node in the round-robin sequence.
func (vml *VMList) PopVM() *VMMetadata {
	vml.Lock.Lock()
	defer vml.Lock.Unlock()

	if len(vml.Nodes) == 0 {
		return nil
	}

	if len(vml.VMs) > 0 {
		// Pop the last element (which is the most recently used).
		lastIndex := len(vml.VMs) - 1
		vm := vml.VMs[lastIndex]

		// Update the slice for this node.
		vml.VMs = vml.VMs[:lastIndex]
		vml.logger.Debugf("khala: popped VM %s from node %s", vm.ID, vm.Node)
		return vm
	}

	// No VM available on the selected node.
	return nil
}

// PushVM adds a VM back to its node-specific list.
func (vml *VMList) PushVM(vm *VMMetadata, resetRetryCount bool) {
	vml.Lock.Lock()
	defer vml.Lock.Unlock()

	vm.LastTimeUsedMs = time.Now().UnixMilli()
	if resetRetryCount {
		vm.RetryCount = 0
	}
	vml.VMs = append(vml.VMs, vm)
	vml.logger.Debugf("khala: pushed VM %s", vm.ID)

	// Signal exactly one waiter that a VM is available.
	vml.signalWaiters()
}

// signalWaiters wakes one goroutine blocked in AcquireVM so it can
// re-evaluate whether a VM is available or a creation slot has opened.
// Safe to call without holding vml.Lock.
func (vml *VMList) signalWaiters() {
	select {
	case vml.vmAvailableChan <- struct{}{}:
	default:
	}
}

// CreateVM creates a new VM on the node with the fewest active VMs.
func (vml *VMList) CreateVM(ctx context.Context) (*VMMetadata, error) {
	vml.Lock.Lock()

	if len(vml.Nodes) == 0 {
		vml.Lock.Unlock()
		return nil, fmt.Errorf("no nodes available to create a VM")
	}

	if vml.revScale.MaxScale != 0 && (vml.TotalVMCount+vml.CreateInFlight) >= vml.revScale.MaxScale {
		vml.Lock.Unlock()
		return nil, fmt.Errorf("maximum scale reached: %d", vml.revScale.MaxScale)
	}

	minNode := vml.Nodes[0]
	minCount := vml.NodeVMCount[minNode] + vml.NodeCreateInFlight[minNode]

	for _, node := range vml.Nodes {
		count := vml.NodeVMCount[node] + vml.NodeCreateInFlight[node]
		if count < minCount {
			minCount = count
			minNode = node
		}
	}

	client, ok := vml.khalaGrpcClient[minNode]
	if !ok {
		vml.Lock.Unlock()
		return nil, fmt.Errorf("gRPC client not found for node %s", minNode)
	}

	// Track in-flight create separately; VM counters are updated only on
	// confirmed create success.
	vml.NodeCreateInFlight[minNode]++
	vml.CreateInFlight++
	vml.updateAdmissionCapacityLocked()
	vml.Lock.Unlock()

	resp, err := client.AddVM(ctx, &khala.AddVMRequest{VmName: vml.Workload})
	if err != nil {
		vml.Lock.Lock()
		vml.NodeCreateInFlight[minNode]--
		vml.CreateInFlight--
		vml.reconcileCountsLocked("create-failure")
		vml.updateAdmissionCapacityLocked()
		vml.Lock.Unlock()
		// Wake a waiter: TotalVMCount dropped, so atMax may now be false.
		vml.signalWaiters()
		return nil, err
	}

	newVM := &VMMetadata{
		ID:             resp.VmId,
		IP:             resp.Ip,
		RPCPort:        resp.RpcPort,
		Node:           minNode,
		LastTimeUsedMs: time.Now().UnixMilli(),
		RetryCount:     0,
	}

	vml.Lock.Lock()
	vml.NodeCreateInFlight[minNode]--
	vml.CreateInFlight--
	vml.NodeVMCount[minNode]++
	vml.TotalVMCount++
	vml.reconcileCountsLocked("create-success")
	vml.updateAdmissionCapacityLocked()
	vml.Lock.Unlock()

	vml.logger.Infof("khala: created VM: %v", newVM)

	return newVM, nil
}

func (vml *VMList) InitialScaleUp() {
	if vml.revScale.InitialScale <= 0 {
		return
	}
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	concurrencyLimiter := make(chan struct{}, 5)

	var wg sync.WaitGroup
	for i := 0; i < vml.revScale.InitialScale; i++ {
		wg.Add(1)
		concurrencyLimiter <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-concurrencyLimiter }()

			for {
				if ctx.Err() != nil {
					vml.logger.Errorf("khala: context deadline exceeded, stopping VM creation")
					return
				}

				vm, err := vml.CreateVM(ctx)
				if err == nil {
					vml.PushVM(vm, true)
					return
				}

				vml.logger.Warnf("khala: failed to create VM: %v, retrying in 2-3s", err)

				select {
				case <-ctx.Done():
					return
				case <-time.After(1*time.Second + time.Duration(time.Now().UnixNano()%2000)*time.Millisecond):
				}
			}
		}()
	}

	wg.Wait()
	vml.logger.Infof("khala: completed initial scale-up for workload %s", vml.Workload)
}

// Periodically removes VMs that have not been used recently.
func (vml *VMList) RemoveVMsWithLeastRecentUse(ctx context.Context) {
	// ensure this goroutine runs only once
	vml.runClenaupOnce.Do(func() {
		go func() {
			var ticker *time.Ticker

			vml.logger.Infof("khala: starting RemoveVMsWithLeastRecentUse for workload %s", vml.Workload)
			ticker = time.NewTicker(time.Duration(vml.updateIntervalSec) * time.Second)

			defer ticker.Stop()
			// cancel for loop when ctx.Done is closed

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					currentTimeMs := time.Now().UnixMilli()
					vml.Lock.Lock()
					keptVMs := make([]*VMMetadata, 0, len(vml.VMs))
					vmsToRemove := make([]*VMMetadata, 0)
					for _, vm := range vml.VMs {
						if currentTimeMs-vm.LastTimeUsedMs > int64(vml.keepaliveDurationSec*1000) {
							if vml.revScale.MinScale > 0 && vml.TotalVMCount <= vml.revScale.MinScale {
								keptVMs = append(keptVMs, vm)
								continue
							}
							vmsToRemove = append(vmsToRemove, vm)
						} else {
							keptVMs = append(keptVMs, vm)
						}
					}
					vml.VMs = keptVMs
					vml.reconcileCountsLocked("cleanup-scan")
					vml.Lock.Unlock()

					for _, vm := range vmsToRemove {
						if !vml.removeVMFromOrchestrator(vm) {
							vml.logger.Warnf("khala: failed to remove idle VM %s, returning to pool", vm.ID)
							vml.PushVM(vm, true)
							continue
						}

						vml.Lock.Lock()
						if vml.NodeVMCount[vm.Node] > 0 {
							vml.NodeVMCount[vm.Node]--
						}
						if vml.TotalVMCount > 0 {
							vml.TotalVMCount--
						}
						vml.reconcileCountsLocked("cleanup-remove-success")
						vml.updateAdmissionCapacityLocked()
						vml.Lock.Unlock()

						vml.signalWaiters()
						vml.logger.Infof("khala: removed VM due to inactivity: %v", vm)
					}
				}
			}
		}()
	})
}

func (vml *VMList) InvalidateVM(vm *VMMetadata) {
	vml.logger.Infof("khala: vm failed to serve request, invalidating VM: %v", vm)
	if vm.RetryCount < 3 {
		vm.RetryCount++
		vml.logger.Debugf("khala: VM %s retry count increased to %d", vm.ID, vm.RetryCount)
		vml.PushVM(vm, false)
	} else {
		go func() {
			if !vml.removeVMFromOrchestrator(vm) {
				vml.logger.Warnf("khala: failed to remove invalid VM %s, returning to pool", vm.ID)
				vml.PushVM(vm, false)
				return
			}

			vml.Lock.Lock()
			if vml.NodeVMCount[vm.Node] > 0 {
				vml.NodeVMCount[vm.Node]--
			}
			if vml.TotalVMCount > 0 {
				vml.TotalVMCount--
			}
			vml.reconcileCountsLocked("invalidate-remove-success")
			vml.updateAdmissionCapacityLocked()
			vml.Lock.Unlock()

			vml.signalWaiters()
			vml.logger.Infof("khala: invalidated VM: %v", vm)
		}()
	}
}
