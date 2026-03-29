package khala

import "sync/atomic"

// NodeLoad captures the shared per-node pressure signals used by the activator.
type NodeLoad struct {
	LiveVMs        int64
	CreateInFlight int64
	ActiveRequests int64
}

type nodeLoadUpdateFunc func(node string, load NodeLoad)

func noopNodeLoadUpdate(string, NodeLoad) {}

// NodeLoadTracker keeps activator-global node pressure counters without a hot-path mutex.
type NodeLoadTracker struct {
	nodes          []string
	nodeIndex      map[string]int
	liveVMs        []atomic.Int64
	createInFlight []atomic.Int64
	activeRequests []atomic.Int64
	loadUpdateFunc atomic.Value
}

func NewNodeLoadTracker(nodes []string) *NodeLoadTracker {
	copiedNodes := append([]string(nil), nodes...)
	tracker := &NodeLoadTracker{
		nodes:          copiedNodes,
		nodeIndex:      make(map[string]int, len(copiedNodes)),
		liveVMs:        make([]atomic.Int64, len(copiedNodes)),
		createInFlight: make([]atomic.Int64, len(copiedNodes)),
		activeRequests: make([]atomic.Int64, len(copiedNodes)),
	}
	for i, node := range copiedNodes {
		tracker.nodeIndex[node] = i
	}
	tracker.loadUpdateFunc.Store(nodeLoadUpdateFunc(noopNodeLoadUpdate))
	return tracker
}

func (nlt *NodeLoadTracker) Nodes() []string {
	if nlt == nil {
		return nil
	}
	return append([]string(nil), nlt.nodes...)
}

func (nlt *NodeLoadTracker) NodeCount() int {
	if nlt == nil {
		return 0
	}
	return len(nlt.nodes)
}

func (nlt *NodeLoadTracker) Index(node string) (int, bool) {
	if nlt == nil {
		return 0, false
	}
	idx, ok := nlt.nodeIndex[node]
	return idx, ok
}

func (nlt *NodeLoadTracker) AddLiveVM(node string, delta int64) bool {
	idx, ok := nlt.Index(node)
	if !ok {
		return false
	}
	nlt.liveVMs[idx].Add(delta)
	nlt.emitLoad(node, idx)
	return true
}

func (nlt *NodeLoadTracker) AddCreateInFlight(node string, delta int64) bool {
	idx, ok := nlt.Index(node)
	if !ok {
		return false
	}
	nlt.createInFlight[idx].Add(delta)
	nlt.emitLoad(node, idx)
	return true
}

func (nlt *NodeLoadTracker) AddActiveRequests(node string, delta int64) bool {
	idx, ok := nlt.Index(node)
	if !ok {
		return false
	}
	nlt.activeRequests[idx].Add(delta)
	nlt.emitLoad(node, idx)
	return true
}

func (nlt *NodeLoadTracker) Load(node string) (NodeLoad, bool) {
	idx, ok := nlt.Index(node)
	if !ok {
		return NodeLoad{}, false
	}
	return nlt.loadByIndex(idx), true
}

func (nlt *NodeLoadTracker) SetLoadUpdateFunc(fn func(node string, load NodeLoad)) {
	if nlt == nil {
		return
	}

	emitCurrent := fn != nil
	if fn == nil {
		fn = noopNodeLoadUpdate
	}
	nlt.loadUpdateFunc.Store(nodeLoadUpdateFunc(fn))

	if !emitCurrent {
		return
	}
	for i, node := range nlt.nodes {
		fn(node, nlt.loadByIndex(i))
	}
}

func (nlt *NodeLoadTracker) loadByIndex(idx int) NodeLoad {
	return NodeLoad{
		LiveVMs:        nlt.liveVMs[idx].Load(),
		CreateInFlight: nlt.createInFlight[idx].Load(),
		ActiveRequests: nlt.activeRequests[idx].Load(),
	}
}

func (nlt *NodeLoadTracker) emitLoad(node string, idx int) {
	if nlt == nil {
		return
	}
	fn, _ := nlt.loadUpdateFunc.Load().(nodeLoadUpdateFunc)
	if fn == nil {
		return
	}
	fn(node, nlt.loadByIndex(idx))
}
