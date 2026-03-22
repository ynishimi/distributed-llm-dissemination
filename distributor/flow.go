package distributor

import (
	"fmt"
	"math"

	"github.com/rs/zerolog/log"
)

type flowNodeKind int

const (
	kindSource flowNodeKind = iota
	kindSender
	kindLayer
	kindReceiver
	kindSink
)

type flowNode struct {
	kind    flowNodeKind
	nodeID  NodeID
	layerID LayerID
}

type flowJobInfo struct {
	senderID NodeID
	layerID  LayerID
	dataSize int64
	offset   int64
}

func (job *flowJobInfo) String() string {
	return fmt.Sprintf("s%d -> (l%d: %dB), offset:%d", job.senderID, job.layerID, job.dataSize, job.offset)
}

type flowJobInfosMap map[NodeID][]flowJobInfo

type flowGraph struct {
	adjMatrix          [][]int64
	assignment         Assignment
	status             status
	layers             Layers
	assignmentLayerIDs LayerIDs
	idx                map[flowNode]int
	numVertex          int
	maxFlow            int64
}

func (frleader *FlowRetransmitLeaderNode) newFlowGraph() *flowGraph {
	// counts num of assignmentLayerIDs across the assignment
	assignmentLayerIDs := make(LayerIDs)

	for _, layerIDs := range frleader.assignment {
		for layerID := range layerIDs {
			if meta, ok := assignmentLayerIDs[layerID]; !ok {
				assignmentLayerIDs[layerID] = meta
			}
		}
	}

	// idx of nodes
	idx := make(map[flowNode]int)
	n := 0
	addIndex := func(node flowNode) {
		if registeredIdx, ok := idx[node]; !ok {
			idx[node] = n
			n++
		} else {
			log.Debug().Int("idx", registeredIdx).Msg("node already registered to idx")
		}
	}

	// numVertex = src + sink + num of nodes + num of layers
	numVertex := n

	// add idx
	// 1. src to sender
	src := flowNode{kind: kindSource}
	addIndex(src)

	// 2. sender to layer
	for nodeID := range frleader.status {
		sender := flowNode{kind: kindSender, nodeID: nodeID}
		addIndex(sender)
	}
	for layerID := range assignmentLayerIDs {
		layer := flowNode{kind: kindLayer, layerID: layerID}
		addIndex(layer)
	}

	// 3. layer to receiver
	for nodeID := range frleader.assignment {
		receiver := flowNode{kind: kindReceiver, nodeID: nodeID}
		addIndex(receiver)
	}

	// 4. receiver to sink
	addIndex(flowNode{kind: kindSink})

	g := flowGraph{
		adjMatrix:          make([][]int64, numVertex),
		assignment:         frleader.assignment,
		status:             frleader.status,
		layers:             frleader.layers,
		assignmentLayerIDs: assignmentLayerIDs,
		idx:                idx,
		numVertex:          numVertex,
		maxFlow:            0,
	}

	return &g
}

func (g *flowGraph) getJobAssignment() (int64, flowJobInfosMap) {
	requiredFlow := int64(0)

	for layerID := range g.assignmentLayerIDs {
		requiredFlow += g.layers[layerID].DataSize
	}

	// first, attain the upper bound of execution time
	tUpper := int64(1)
	for {
		maxFlow := g.updateMaxFlow(tUpper)
		if maxFlow >= requiredFlow {
			break
		}
		tUpper *= 2
	}

	// find the minimum time by bisect
	l := int64(1)
	r := tUpper

	t := tUpper

	for l <= r {
		m := l + (r-l)/2
		if maxFlow := g.updateMaxFlow(m); maxFlow < requiredFlow {
			// l too small, look for a longer time
			l = m + 1
		} else {
			// update the value with the new flow (=time)
			t = min(t, maxFlow)
			// r too big, look for a shorter time
			r = m - 1
		}
	}

	// t is the value we want to get; update the flow with the obtained time t.
	g.updateMaxFlow(t)

	flowJobs := make(flowJobInfosMap)

	layerOffset := make(map[LayerID]int64)

	for senderID, layerIDs := range g.status {
		for layerID := range layerIDs {
			sender := flowNode{kind: kindSender, nodeID: senderID}
			layer := flowNode{kind: kindLayer, layerID: layerID}
			flow := g.adjMatrix[g.idx[sender]][g.idx[layer]]
			if flow > 0 {
				offset := layerOffset[layerID]
				flowJobs[senderID] = append(flowJobs[senderID], flowJobInfo{senderID, layerID, flow, offset})
				layerOffset[layerID] += flow
			}
		}
	}

	return t, flowJobs
}

func (g *flowGraph) buildEdgeCapacity(time int64) {

	// todo: get network b/w of sender
	networkBW := int64(math.Pow(2, 30))

	// todo: get each layer's size

	// add edges
	// 1. src to sender
	src := flowNode{kind: kindSource}
	for nodeID := range g.status {
		sender := flowNode{kind: kindSender, nodeID: nodeID}
		g.addEdge(g.idx[src], g.idx[sender], networkBW*time)
	}

	// 2. sender to layer
	for nodeID, layerIDs := range g.status {
		sender := flowNode{kind: kindSender, nodeID: nodeID}
		for layerID, meta := range layerIDs {
			layer := flowNode{kind: kindLayer, layerID: layerID}
			g.addEdge(g.idx[sender], g.idx[layer], meta.LimitRate*time)
		}
	}

	// 3. layer to receiver
	for nodeID, layerIDs := range g.assignment {
		receiver := flowNode{kind: kindReceiver, nodeID: nodeID}
		for layerID := range layerIDs {
			layer := flowNode{kind: kindLayer, layerID: layerID}
			g.addEdge(g.idx[layer], g.idx[receiver], g.layers[layerID].DataSize)
		}

		// 4. receiver to sink
		sink := flowNode{kind: kindSink}
		g.addEdge(g.idx[receiver], g.idx[sink], networkBW*time)
	}
}

func (g *flowGraph) addEdge(src, dest int, capacity int64) {
	g.adjMatrix[src][dest] = capacity
}

// bfs checks if there is a path from src to dest.
// if the path is found, them it returns the shortest path, creating the map of parent for each vertex.
func (g *flowGraph) bfs(src, dest int) (parent []int, ok bool) {
	visited := make([]bool, g.numVertex)
	parent = make([]int, g.numVertex)
	var queue []int
	queue = append(queue, src)
	visited[src] = true

	for len(queue) != 0 {
		u, queue := queue[0], queue[1:]

		for ind, val := range g.adjMatrix[u] {
			if !visited[ind] && val > 0 {
				queue = append(queue, ind)
				visited[ind] = true
				parent[ind] = u

				if ind == dest {
					// a shortest path was found
					return parent, true
				}
			}
		}
	}
	// a path src -> dst is not available
	return parent, false
}

// updateMaxFlow calculates the max flow of given time.
// This is essentially an implementation of the Edmonds-Karp Algorithm.
func (g *flowGraph) updateMaxFlow(time int64) (maxFlow int64) {
	g.buildEdgeCapacity(time)

	g.maxFlow = 0

	src := g.idx[flowNode{kind: kindSource}]
	sink := g.idx[flowNode{kind: kindSink}]

	for {
		// get the shortest path from src to sink
		parent, ok := g.bfs(src, sink)
		if !ok {
			return g.maxFlow
		}

		// calc potential flow toward this path
		pathFlow := int64(math.MaxInt64)
		s := sink
		for s != src {
			pathFlow = min(pathFlow, g.adjMatrix[parent[s]][s])
			s = parent[s]
		}
		g.maxFlow += pathFlow

		// modify residue network
		v := sink
		for v != src {
			g.adjMatrix[parent[v]][v] -= pathFlow
			g.adjMatrix[v][parent[v]] += pathFlow
			v = parent[v]
		}
	}
}
