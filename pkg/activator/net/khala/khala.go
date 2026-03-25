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

const removeVMMaxAttempts = 3

var (
	removeVMRetryDelay       = 2 * time.Second
	removeVMTimeout          = 5 * time.Second
	initialNextNodeIndexFunc = func(nodeCount int) int {
		if nodeCount <= 1 {
			return 0
		}
		return int(time.Now().UnixNano() % int64(nodeCount))
	}
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
		nextNodeIndex:        initialNextNodeIndexFunc(len(nodes)),
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
			vml.logger.Warnw("khala: reconcile corrected negative node VM count",
				"reason", reason, "node", node, "count", count)
			recordReconcileCorrection()
			count = 0
			vml.NodeVMCount[node] = 0
		}
		avail := availableByNode[node]
		if count < avail {
			vml.logger.Warnw("khala: reconcile corrected node VM count below available pool",
				"reason", reason, "node", node, "count", count, "available", avail)
			recordReconcileCorrection()
			count = avail
			vml.NodeVMCount[node] = count
		}
		sumNode += count
	}
	for node, avail := range availableByNode {
		if _, ok := vml.NodeVMCount[node]; !ok {
			vml.logger.Warnw("khala: reconcile added missing node VM counter",
				"reason", reason, "node", node, "available", avail)
			recordReconcileCorrection()
			vml.NodeVMCount[node] = avail
			sumNode += avail
		}
	}

	if vml.TotalVMCount != sumNode {
		vml.logger.Warnw("khala: reconcile corrected total VM count",
			"reason", reason, "total_vm_count", vml.TotalVMCount, "sum_node_vm_count", sumNode)
		recordReconcileCorrection()
		vml.TotalVMCount = sumNode
	}

	sumCreate := 0
	for node, count := range vml.NodeCreateInFlight {
		if count < 0 {
			vml.logger.Warnw("khala: reconcile corrected negative node create-inflight count",
				"reason", reason, "node", node, "count", count)
			recordReconcileCorrection()
			count = 0
			vml.NodeCreateInFlight[node] = 0
		}
		sumCreate += count
	}
	if vml.CreateInFlight != sumCreate {
		vml.logger.Warnw("khala: reconcile corrected total create-inflight count",
			"reason", reason, "create_inflight", vml.CreateInFlight, "sum_node_create_inflight", sumCreate)
		recordReconcileCorrection()
		vml.CreateInFlight = sumCreate
		recordVMCreateInFlight(vml.CreateInFlight)
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
	ctx, cancel := context.WithTimeout(context.Background(), removeVMTimeout)
	defer cancel()
	resp, err := client.RemoveVM(ctx, &khala.RemoveVMRequest{VmId: vm.ID})
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

func (vml *VMList) removeVMFromOrchestratorWithRetry(vm *VMMetadata, reason string) {
	for attempt := 1; attempt <= removeVMMaxAttempts; attempt++ {
		if vml.removeVMFromOrchestrator(vm) {
			vml.logger.Infof("khala: removed %s VM: %v", reason, vm)
			return
		}
		if attempt == removeVMMaxAttempts {
			break
		}
		time.Sleep(removeVMRetryDelay)
	}

	vml.logger.Warnf("khala: failed to remove %s VM %s after %d attempts; treating it as dead",
		reason, vm.ID, removeVMMaxAttempts)
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

		gotPermit := false
		if !atMax {
			// 3. Below MaxScale, only permit holders are allowed to create VMs.
			// Requests that cannot acquire a permit wait in queue. Permit holders
			// wait for their own create result. If the request times out first,
			// create continues detached and returns VM to pool when complete.
			if vml.tryAcquireCreatePermit() {
				gotPermit = true
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
		waitReason := "create-failed-or-race"
		if atMax {
			waitReason = "at-max-scale"
		} else if !gotPermit {
			waitReason = "create-permit-exhausted"
		}
		vml.Lock.RLock()
		totalVMCount := vml.TotalVMCount
		createInFlight := vml.CreateInFlight
		maxScale := vml.revScale.MaxScale
		vml.Lock.RUnlock()
		vml.logger.Debugw("khala: request queued waiting for VM availability",
			"workload", vml.Workload,
			"reason", waitReason,
			"total_vms", totalVMCount,
			"create_inflight", createInFlight,
			"max_scale", maxScale)

		waitStart := time.Now()
		select {
		case <-ctx.Done():
			waitLatency := time.Since(waitStart)
			recordVMWaitLatency(waitLatency)
			vml.logger.Infow("khala: request timed out while waiting for VM",
				"workload", vml.Workload,
				"reason", waitReason,
				"wait_ms", waitLatency.Milliseconds(),
				"error", ctx.Err())
			return nil, nil
		case <-waitCh:
			recordVMWaitLatency(time.Since(waitStart))
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
	waitStart := time.Now()
	go func() {
		createCtx, cancel := context.WithTimeout(context.Background(), time.Duration(vml.createTimeoutSec)*time.Second)
		defer cancel()
		vm, err := vml.CreateVM(createCtx)
		createDone <- createResult{vm: vm, err: err}
	}()

	select {
	case <-ctx.Done():
		waitLatency := time.Since(waitStart)
		recordVMWaitLatency(waitLatency)
		vml.logger.Infow("khala: request timed out while waiting for VM create",
			"workload", vml.Workload,
			"wait_ms", waitLatency.Milliseconds(),
			"error", ctx.Err())
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
		recordVMWaitLatency(time.Since(waitStart))
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

func validateAddVMResponse(resp *khala.AddVMResponse) error {
	if resp == nil {
		return fmt.Errorf("AddVM returned nil response")
	}
	if !resp.Success {
		return fmt.Errorf("AddVM unsuccessful")
	}
	if resp.VmId == "" {
		return fmt.Errorf("AddVM returned empty vm_id")
	}
	if resp.Ip == "" {
		return fmt.Errorf("AddVM returned empty ip")
	}
	if resp.RpcPort == "" {
		return fmt.Errorf("AddVM returned empty rpc_port")
	}
	return nil
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

	candidateNodes := make([]string, 0, len(vml.Nodes))
	minCount := 0
	for i, node := range vml.Nodes {
		count := vml.NodeVMCount[node] + vml.NodeCreateInFlight[node]
		if i == 0 || count < minCount {
			minCount = count
			candidateNodes = candidateNodes[:0]
			candidateNodes = append(candidateNodes, node)
		} else if count == minCount {
			candidateNodes = append(candidateNodes, node)
		}
	}

	minNode := candidateNodes[0]
	if len(candidateNodes) > 1 {
		minNode = candidateNodes[vml.nextNodeIndex%len(candidateNodes)]
		vml.nextNodeIndex++
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
	recordVMCreateInFlight(vml.CreateInFlight)
	vml.Lock.Unlock()

	recordVMCreateAttempt()
	vml.logger.Infow("khala: VM create started",
		"workload", vml.Workload,
		"node", minNode)

	resp, err := client.AddVM(ctx, &khala.AddVMRequest{VmName: vml.Workload})
	if err == nil {
		err = validateAddVMResponse(resp)
	}
	if err != nil {
		vml.Lock.Lock()
		vml.NodeCreateInFlight[minNode]--
		vml.CreateInFlight--
		vml.reconcileCountsLocked("create-failure")
		vml.updateAdmissionCapacityLocked()
		recordVMCreateInFlight(vml.CreateInFlight)
		vml.Lock.Unlock()
		recordVMCreateFailure()
		// Wake a waiter: TotalVMCount dropped, so atMax may now be false.
		vml.signalWaiters()
		vml.logger.Warnw("khala: VM create failed",
			"workload", vml.Workload,
			"node", minNode,
			"error", err)
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
	recordVMCreateInFlight(vml.CreateInFlight)
	vml.Lock.Unlock()
	recordVMCreateSuccess()

	vml.logger.Infow("khala: VM create succeeded",
		"workload", vml.Workload,
		"vm_id", newVM.ID,
		"node", newVM.Node,
		"ip", newVM.IP,
		"rpc_port", newVM.RPCPort)

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
					// Limit this cleanup pass so we never schedule removals below minScale.
					removableBudget := vml.TotalVMCount - vml.revScale.MinScale
					if removableBudget < 0 {
						removableBudget = 0
					}
					for _, vm := range vml.VMs {
						if currentTimeMs-vm.LastTimeUsedMs > int64(vml.keepaliveDurationSec*1000) {
							if removableBudget > 0 {
								vmsToRemove = append(vmsToRemove, vm)
								removableBudget--
								continue
							}
						}
						keptVMs = append(keptVMs, vm)
					}
					vml.VMs = keptVMs
					vml.reconcileCountsLocked("cleanup-scan")
					vml.Lock.Unlock()

					for _, vm := range vmsToRemove {
						vml.Lock.Lock()
						if vml.NodeVMCount[vm.Node] > 0 {
							vml.NodeVMCount[vm.Node]--
						}
						if vml.TotalVMCount > 0 {
							vml.TotalVMCount--
						}
						vml.reconcileCountsLocked("cleanup-remove")
						vml.updateAdmissionCapacityLocked()
						vml.Lock.Unlock()

						vml.signalWaiters()
						go vml.removeVMFromOrchestratorWithRetry(vm, "idle")
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
		vml.Lock.Lock()
		if vml.NodeVMCount[vm.Node] > 0 {
			vml.NodeVMCount[vm.Node]--
		}
		if vml.TotalVMCount > 0 {
			vml.TotalVMCount--
		}
		vml.reconcileCountsLocked("invalidate-remove")
		vml.updateAdmissionCapacityLocked()
		vml.Lock.Unlock()

		vml.signalWaiters()
		go func() {
			vml.removeVMFromOrchestratorWithRetry(vm, "invalid")
		}()
	}
}
