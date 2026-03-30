package distributor

import (
	"fmt"
	"maps"
	"math"
	"slices"

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
	return fmt.Sprintf("s%d -> (l%d: %d Bytes), offset: %d", job.senderID, job.layerID, job.dataSize, job.offset)
}

type flowJobInfosMap map[NodeID][]flowJobInfo

type flowGraph struct {
	adjMatrix          [][]int64
	assignment         Assignment
	status             status
	layers             Layers
	nodeNetworkBW      map[NodeID]int64
	assignmentLayerIDs LayerIDs
	idx                map[flowNode]int
	numVertex          int
	maxFlow            int64
}

func (frleader *FlowRetransmitLeaderNode) newFlowGraph(assignment Assignment) *flowGraph {
	// counts num of assignmentLayerIDs across the assignment
	assignmentLayerIDs := make(LayerIDs)

	for _, layerIDs := range assignment {
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

	// add idx
	// 1. src to sender
	src := flowNode{kind: kindSource}
	addIndex(src)

	// 2. sender to layer
	for _, nodeID := range slices.Sorted(maps.Keys(frleader.status)) {
		sender := flowNode{kind: kindSender, nodeID: nodeID}
		addIndex(sender)
	}
	for _, layerID := range slices.Sorted(maps.Keys(assignmentLayerIDs)) {
		layer := flowNode{kind: kindLayer, layerID: layerID}
		addIndex(layer)
	}

	// 3. layer to receiver
	for _, nodeID := range slices.Sorted(maps.Keys(assignment)) {
		receiver := flowNode{kind: kindReceiver, nodeID: nodeID}
		addIndex(receiver)
	}

	// 4. receiver to sink
	addIndex(flowNode{kind: kindSink})

	// numVertex = src + sink + num of nodes + num of layers
	numVertex := n
	adjMatrix := make([][]int64, numVertex)
	for i := range numVertex {
		adjMatrix[i] = make([]int64, numVertex)
	}

	g := flowGraph{
		adjMatrix:          adjMatrix,
		assignment:         assignment,
		status:             frleader.status,
		layers:             frleader.layers,
		nodeNetworkBW:      frleader.NodeNetworkBW,
		assignmentLayerIDs: assignmentLayerIDs,
		idx:                idx,
		numVertex:          numVertex,
		maxFlow:            0,
	}

	return &g
}

func (g *flowGraph) getJobAssignment() (int64, flowJobInfosMap) {
	requiredFlow := int64(0)

	log.Info().Msg("assigning a job...")
	for layerID := range g.assignmentLayerIDs {
		requiredFlow += g.layers[layerID].DataSize
	}

	// first, attain the upper bound of execution time
	tUpper := int64(1)
	for {
		// log.Debug().Int64("tUpper", tUpper).Msg("searching tUpper")
		maxFlow := g.updateMaxFlow(tUpper)
		if maxFlow >= requiredFlow {
			break
		}
		if tUpper > math.MaxInt64/2 {
			log.Error().Msg("tUpper not found")
			break
		}
		tUpper *= 2
	}
	log.Debug().Int64("tUpper", tUpper).Msg("tUpper found")

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
			t = min(t, m)
			// r too big, look for a shorter time
			r = m - 1
		}
	}

	// t is the value we want to get; update the flow with the obtained time t.
	// log.Debug().Int64("t", t).Msg("minimum t found")
	g.updateMaxFlow(t)

	flowJobs := make(flowJobInfosMap)

	layerOffset := make(map[LayerID]int64)

	for senderID, layerIDs := range g.status {
		for layerID := range layerIDs {
			sender := flowNode{kind: kindSender, nodeID: senderID}
			layer := flowNode{kind: kindLayer, layerID: layerID}
			flow := g.adjMatrix[g.idx[layer]][g.idx[sender]]
			if flow > 0 {
				offset := layerOffset[layerID]
				flowJobs[senderID] = append(flowJobs[senderID], flowJobInfo{senderID, layerID, flow, offset})
				layerOffset[layerID] += flow
			}
		}
	}

	log.Info().
		Int64("required minimum time(s)", t).
		Msg("job assignment calculated")

	return t, flowJobs
}

func (g *flowGraph) buildEdgeCapacity(time int64) {

	// reset adjMatrix
	for i := range g.adjMatrix {
		for j := range g.adjMatrix[i] {
			g.adjMatrix[i][j] = 0
		}
	}

	// add edges
	// 1. src to sender
	src := flowNode{kind: kindSource}
	for nodeID := range g.status {
		sender := flowNode{kind: kindSender, nodeID: nodeID}
		senderNetworkBW := g.getNodeNetworkBW(nodeID)
		g.addEdge(g.idx[src], g.idx[sender], senderNetworkBW*time)
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
		receiverNetworkBW := g.getNodeNetworkBW(nodeID)
		g.addEdge(g.idx[receiver], g.idx[sink], receiverNetworkBW*time)
	}
	// log.Debug().Msg("built edge capacity")
}

func (g *flowGraph) getNodeNetworkBW(nodeID NodeID) int64 {
	return g.nodeNetworkBW[nodeID]
}

func (g *flowGraph) addEdge(src, dest int, capacity int64) {
	g.adjMatrix[src][dest] = capacity
	// log.Debug().Int("src", src).Int("dest", dest).Int64("capacity", capacity).Send()
}

// bfs checks if there is a path from src to dest.
// if the path is found, them it returns the shortest path, creating the map of parent for each vertex.
func (g *flowGraph) bfs(src, dest int) (parent []int, ok bool) {

	// log.Debug().Int("src", src).Int("dest", dest).Send()
	visited := make([]bool, g.numVertex)
	parent = make([]int, g.numVertex)
	var queue []int
	queue = append(queue, src)
	visited[src] = true

	for len(queue) != 0 {
		u := queue[0]
		queue = queue[1:]
		// log.Debug().Int("u", u).Send()

		for ind, val := range g.adjMatrix[u] {
			if !visited[ind] && val > 0 {
				// log.Debug().Int("ind", ind).Int64("val", val).Send()
				queue = append(queue, ind)
				visited[ind] = true
				parent[ind] = u

				if ind == dest {
					// a shortest path was found
					// log.Debug().Msg("shortest path found")
					return parent, true
				}
			}
		}
	}
	// a path src -> dst is not available
	// log.Debug().Msg("shortest path not found")
	return parent, false
}

// updateMaxFlow calculates the max flow of given time.
// This is essentially an implementation of the Edmonds-Karp Algorithm.
func (g *flowGraph) updateMaxFlow(time int64) int64 {
	// log.Debug().Msg("updateMaxFlow")
	g.buildEdgeCapacity(time)

	g.maxFlow = 0

	src := g.idx[flowNode{kind: kindSource}]
	sink := g.idx[flowNode{kind: kindSink}]

	for {
		// get the shortest path from src to sink
		parent, ok := g.bfs(src, sink)
		if !ok {
			// log.Debug().Int64("maxFlow", g.maxFlow).Send()
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
