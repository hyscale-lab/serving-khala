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
	TotalVMCount         int
	nextNodeIndex        int
	khalaGrpcClient      map[string]khala.KhalaKnativeIntegrationClient
	keepaliveDurationSec int
	updateIntervalSec    int
	Lock                 sync.RWMutex
	logger               *zap.SugaredLogger
	runClenaupOnce       sync.Once
	NoScaleDown          bool
	revScale             RevisionScaleInfo
	CapacityUpdateFunc   func(int)
}

func NewRevVMList(extractedName string, nodes []string, revScale RevisionScaleInfo, logger *zap.SugaredLogger, capacityUpdateFunc func(int)) *VMList {
	logger.Infof("khala: initializing VMList with nodes: %v", nodes)
	// We support no-coldstart mode for testing purposes
	// In general, we use Initscale to we pre-create VMs up to the initial scale
	// and never scale down - not running RemoveVMsWithLeastRecentUse

	NoScaleDown := GetBoolEnv("NO_SCALEDOWN", false)
	keepAlive := GetEnv("KEEPALIVE_DURATION", 60)
	updateInt := GetEnv("UPDATE_INTERVAL", 5)
	logger.Infof("khala: no-scale-down mode: %v", NoScaleDown)
	logger.Infof("khala: keepalive duration: %v", keepAlive)
	logger.Infof("khala: update interval: %v", updateInt)

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
	VMs := make([]*VMMetadata, 0, initialCap)

	NodeVMCount := make(map[string]int)
	for _, node := range nodes {
		NodeVMCount[node] = 0
	}

	return &VMList{
		VMs:                  VMs,
		Workload:             extractedName,
		Nodes:                nodes,
		NodeVMCount:          NodeVMCount,
		nextNodeIndex:        0,
		khalaGrpcClient:      khalaGrpcClient,
		keepaliveDurationSec: keepAlive,
		updateIntervalSec:    updateInt,
		logger:               logger,
		runClenaupOnce:       sync.Once{},
		NoScaleDown:          NoScaleDown,
		revScale:             revScale,
		CapacityUpdateFunc:   capacityUpdateFunc,
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
	node := vm.Node
	vml.VMs = append(vml.VMs, vm)

	vml.logger.Debugf("khala: pushed VM %s back to node %s", vm.ID, node)
}

// CreateVM creates a new VM on the node with the fewest active VMs.
func (vml *VMList) CreateVM() (*VMMetadata, error) {
	vml.Lock.Lock()

	if len(vml.Nodes) == 0 {
		vml.Lock.Unlock()
		return nil, fmt.Errorf("no nodes available to create a VM")
	}

	if vml.revScale.MaxScale != 0 && vml.TotalVMCount >= vml.revScale.MaxScale {
		vml.Lock.Unlock()
		return nil, fmt.Errorf("maximum scale reached: %d", vml.revScale.MaxScale)
	}

	minNode := vml.Nodes[0]
	minCount := vml.NodeVMCount[minNode]

	for _, node := range vml.Nodes {
		count := vml.NodeVMCount[node]
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
	vml.Lock.Unlock()

	resp, err := client.AddVM(context.Background(), &khala.AddVMRequest{VmName: vml.Workload})
	if err != nil {
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
	defer vml.Lock.Unlock()

	// vml.VMs = append(vml.VMs, newVM)
	vml.NodeVMCount[minNode]++
	vml.TotalVMCount++
	if vml.CapacityUpdateFunc != nil {
		vml.CapacityUpdateFunc(vml.TotalVMCount)
	}
	vml.logger.Infof("khala: created VM: %v", newVM)

	return newVM, nil
}

func (vml *VMList) InitialScaleUp() {
	if vml.revScale.InitialScale <= 0 {
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(vml.revScale.InitialScale)
	concurrencyLimiter := make(chan struct{}, 5)

	for i := 0; i < vml.revScale.InitialScale; i++ {
		concurrencyLimiter <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			// random sleep to avoid cohort creation
			randGen := time.Now().UnixNano() % 100
			time.Sleep(time.Duration(randGen) * time.Millisecond)
			vm, err := vml.CreateVM()
			if err != nil {
				vml.logger.Errorf("khala: failed to create VM during initial scale-up: %v", err)
				<-concurrencyLimiter
				return
			}
			vml.PushVM(vm, true)
			<-concurrencyLimiter
			vml.logger.Debugf("khala: initial scale-up created VM %s", vm.ID)
		}(i)
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
			if vml.NoScaleDown {
				vml.logger.Infof("khala: no-scaledown mode enabled, skipping RemoveVMsWithLeastRecentUse for workload %s", vml.Workload)
				ticker = time.NewTicker(time.Duration(vml.updateIntervalSec*4) * time.Second)
			} else {
				vml.logger.Infof("khala: starting RemoveVMsWithLeastRecentUse for workload %s", vml.Workload)
				ticker = time.NewTicker(time.Duration(vml.updateIntervalSec) * time.Second)
			}

			defer ticker.Stop()
			// cancel for loop when ctx.Done is closed

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					currentTimeMs := time.Now().UnixMilli()
					vml.Lock.Lock()
					TotalVMs := 0
					for _, count := range vml.NodeVMCount {
						TotalVMs += count
					}

					if vml.NoScaleDown {
						vml.Lock.Unlock()
						vml.logger.Infof("khala: vm scale workload: %s, scale: %d, skipping scale down", vml.Workload, TotalVMs)
					} else {
						keptVMsByNode := make([]*VMMetadata, 0)
						vmsToRemove := make([]*VMMetadata, 0)

						for _, vm := range vml.VMs {
							if currentTimeMs-vm.LastTimeUsedMs > int64(vml.keepaliveDurationSec*1000) {
								vmsToRemove = append(vmsToRemove, vm)
								vml.NodeVMCount[vm.Node]--
								vml.TotalVMCount--

							} else {
								keptVMsByNode = append(keptVMsByNode, vm)
							}
						}

						if vml.CapacityUpdateFunc != nil {
							vml.CapacityUpdateFunc(vml.TotalVMCount)
						}

						vml.VMs = keptVMsByNode
						vmsToRemoveCount := len(vmsToRemove)

						vml.Lock.Unlock()

						if vmsToRemoveCount > 0 {
							vml.logger.Infof("khala: workload %s cleaned up %d / %d", vml.Workload, vmsToRemoveCount, TotalVMs)
						}

						if len(vmsToRemove) > 0 {
							var wg sync.WaitGroup
							for _, vm := range vmsToRemove {
								client, ok := vml.khalaGrpcClient[vm.Node]
								if !ok {
									vml.logger.Errorf("gRPC client not found for node %s", vm.Node)
									continue
								}
								wg.Add(1)
								go func(vmToRemove *VMMetadata, log *zap.SugaredLogger) {
									defer wg.Done()
									client.RemoveVM(context.Background(), &khala.RemoveVMRequest{VmId: vmToRemove.ID})
									log.Infof("khala: removed VM due to inactivity: %v", vmToRemove)
								}(vm, vml.logger)
							}
							wg.Wait()
						}
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
			vml.Lock.Lock()
			client, ok := vml.khalaGrpcClient[vm.Node]
			if !ok {
				vml.Lock.Unlock()
				vml.logger.Errorf("gRPC client not found for node %s", vm.Node)
				return
			}
			vml.Lock.Unlock()
			client.RemoveVM(context.Background(), &khala.RemoveVMRequest{VmId: vm.ID})

			vml.Lock.Lock()
			vml.NodeVMCount[vm.Node]--
			vml.TotalVMCount--
			if vml.CapacityUpdateFunc != nil {
				vml.CapacityUpdateFunc(vml.TotalVMCount)
			}
			vml.Lock.Unlock()

			vml.logger.Infof("khala: invalidated VM: %v", vm)
		}()
	}
}
