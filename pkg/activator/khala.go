package activator

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
}

type VMList struct {
	// UPDATED: Changed from a flat slice to a map of slices, keyed by node name.
	VMs                  []*VMMetadata
	Workload             string
	Nodes                []string
	nextNodeIndex        int
	khalaGrpcClient      map[string]khala.KhalaKnativeIntegrationClient
	keepaliveDurationSec int
	updateIntervalSec    int
	Lock                 sync.RWMutex
	logger               *zap.SugaredLogger
	runClenaupOnce       sync.Once
}

func NewRevVMList(extractedName string, nodes []string, logger *zap.SugaredLogger) *VMList {
	logger.Infof("khala: initializing VMList with nodes: %v", nodes)
	keepAlive := GetEnv("KEEPALIVE_DURATION", 60)
	updateInt := GetEnv("UPDATE_INTERVAL", 5)
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

	// Initialize the map and pre-allocate a slice for each node.
	VMs := make([]*VMMetadata, 0)

	// run once lock to avoid race

	return &VMList{
		VMs:                  VMs,
		Workload:             extractedName,
		Nodes:                nodes,
		nextNodeIndex:        0,
		khalaGrpcClient:      khalaGrpcClient,
		keepaliveDurationSec: keepAlive,
		updateIntervalSec:    updateInt,
		logger:               logger,
		runClenaupOnce:       sync.Once{},
	}
}

// PopVM finds and pops the most recently used VM from the next node in the round-robin sequence.
func (vml *VMList) PopVM() *VMMetadata {
	vml.Lock.Lock()
	defer vml.Lock.Unlock()

	if len(vml.Nodes) == 0 {
		return nil
	}

	// 3. Check for available VMs on that specific node.
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
func (vml *VMList) PushVM(vm *VMMetadata) {
	vml.Lock.Lock()
	defer vml.Lock.Unlock()

	vm.LastTimeUsedMs = time.Now().UnixMilli()

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

	minNode := vml.Nodes[0]
	minCount := len(vml.VMs)

	NodeVMCount := make(map[string]int)
	for _, vm := range vml.VMs {
		NodeVMCount[vm.Node]++
	}

	for _, node := range vml.Nodes {
		count := NodeVMCount[node]
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
	}

	vml.Lock.Lock()
	defer vml.Lock.Unlock()

	vml.VMs = append(vml.VMs, newVM)
	vml.logger.Infof("khala: created VM: %v", newVM)

	return newVM, nil
}

// Periodically removes VMs that have not been used recently.
func (vml *VMList) RemoveVMsWithLeastRecentUse(ctx context.Context) {
	// ensure this goroutine runs only once
	vml.runClenaupOnce.Do(func() {
		go func() {
			vml.logger.Infof("khala: starting RemoveVMsWithLeastRecentUse for workload %s", vml.Workload)
			ticker := time.NewTicker(time.Duration(vml.updateIntervalSec) * time.Second)
			defer ticker.Stop()
			// cancel for loop when ctx.Done is closed

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					currentTimeMs := time.Now().UnixMilli()
					vml.Lock.Lock()

					keptVMsByNode := make([]*VMMetadata, 0)
					vmsToRemove := make([]*VMMetadata, 0)

					// Iterate through each node's VM list in the map.
					for _, vm := range vml.VMs {
						if currentTimeMs-vm.LastTimeUsedMs > int64(vml.keepaliveDurationSec*1000) {
							vmsToRemove = append(vmsToRemove, vm)
						} else {
							keptVMsByNode = append(keptVMsByNode, vm)
						}
					}

					vml.VMs = keptVMsByNode

					vml.logger.Infof("khala: workload %s cleaned up %s / %s, current VMs: %d", vml.Workload, len(vmsToRemove), len(vmsToRemove)+len(keptVMsByNode), len(keptVMsByNode))

					vml.Lock.Unlock()

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
		}()
	})
}
