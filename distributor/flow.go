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
	kindClient
	kindLayer
	kindReceiver
	kindSink
)

type flowNode struct {
	kind       flowNodeKind
	nodeID     NodeID
	layerID    LayerID
	sourceType SourceType
}

type job struct {
	// sender of the job
	SenderID NodeID
	// layer
	LayerID LayerID
	// rate at which the sender transmits the blocks
	Rate int64
	// Size of data which the sender is expected to send
	DataSize int64
}

func (job *job) String() string {
	return fmt.Sprintf("s%d: l%d, rate=%d", job.SenderID, job.LayerID, job.Rate)
}

// key: dest, val: list of jobs
type destJobs map[NodeID][]job

type flowGraph struct {
	adjMatrix          [][]int64
	assignment         Assignment
	status             status
	layers             LayersSrc
	nodeNetworkBW      map[NodeID]int64
	assignmentLayerIDs LayerIDs
	idx                map[flowNode]int
	numVertex          int
	maxFlow            int64
}

func newFlowGraph(assignment Assignment, status status, layers LayersSrc, NodeNetworkBW map[NodeID]int64) *flowGraph {
	// counts num of assignmentLayerIDs across the assignment
	assignmentLayerIDs := make(LayerIDs)

	for _, layerIDs := range assignment {
		for layerID, meta := range layerIDs {
			if _, ok := assignmentLayerIDs[layerID]; !ok {
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
	// 1. src
	src := flowNode{kind: kindSource}
	addIndex(src)

	// 2. sender
	for _, nodeID := range slices.Sorted(maps.Keys(status)) {
		sender := flowNode{kind: kindSender, nodeID: nodeID}
		addIndex(sender)
	}

	// 3. client(source) for each sender
	for _, nodeID := range slices.Sorted(maps.Keys(status)) {
		sourceSet := make(map[SourceType]struct{})
		for _, meta := range status[nodeID] {
			sourceSet[meta.SourceType] = struct{}{}
		}

		sources := make([]SourceType, 0, len(sourceSet))
		for sourceType := range sourceSet {
			sources = append(sources, sourceType)
		}
		slices.Sort(sources)

		for _, sourceType := range sources {
			client := flowNode{kind: kindClient, nodeID: nodeID, sourceType: sourceType}
			addIndex(client)
		}
	}

	// 4. layer
	for _, layerID := range slices.Sorted(maps.Keys(assignmentLayerIDs)) {
		layer := flowNode{kind: kindLayer, layerID: layerID}
		addIndex(layer)
	}

	// 5. receiver
	for _, nodeID := range slices.Sorted(maps.Keys(assignment)) {
		receiver := flowNode{kind: kindReceiver, nodeID: nodeID}
		addIndex(receiver)
	}

	// 6. sink
	addIndex(flowNode{kind: kindSink})

	// numVertex = src + sink + sender + sender-source(client) + layer + receiver
	numVertex := n
	adjMatrix := make([][]int64, numVertex)
	for i := range numVertex {
		adjMatrix[i] = make([]int64, numVertex)
	}

	g := flowGraph{
		adjMatrix:          adjMatrix,
		assignment:         assignment,
		status:             status,
		layers:             layers,
		nodeNetworkBW:      NodeNetworkBW,
		assignmentLayerIDs: assignmentLayerIDs,
		idx:                idx,
		numVertex:          numVertex,
		maxFlow:            0,
	}

	return &g
}

func (g *flowGraph) getJobAssignment() (int64, destJobs) {
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

	log.Debug().Str("adjMatrix", fmt.Sprintln(g.adjMatrix)).Send()

	destJobs := make(destJobs)

	log.Debug().Str("assignment", fmt.Sprintln(g.assignment)).Send()

	for receiverID, layerIDs := range g.assignment {
		// for senderID, layerIDs := range g.status {
		for layerID := range layerIDs {
			for senderID, senderLayers := range g.status {
				meta, hasLayer := senderLayers[layerID]
				if !hasLayer {
					continue
				}

				client := flowNode{kind: kindClient, nodeID: senderID, sourceType: meta.SourceType}
				layer := flowNode{kind: kindLayer, layerID: layerID}
				flowSize := g.adjMatrix[g.idx[layer]][g.idx[client]]
				if flowSize > 0 {
					rate := flowSize / t

					newJob := job{senderID, layerID, rate, flowSize}
					destJobs[receiverID] = append(destJobs[receiverID], newJob)
					log.Debug().Str("job", newJob.String()).Send()
				}
			}
		}
		// }
	}

	log.Info().
		Int64("ETA(s)", t).
		Msg("job assignment updated")

	log.Debug().Str("destJobs", fmt.Sprintln(destJobs)).Send()
	return t, destJobs
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

	// 2. sender to client(source) and 3. client(source) to layer
	for nodeID, layerIDs := range g.status {
		sender := flowNode{kind: kindSender, nodeID: nodeID}
		for layerID, meta := range layerIDs {
			if _, needed := g.assignmentLayerIDs[layerID]; !needed {
				continue
			}

			client := flowNode{kind: kindClient, nodeID: nodeID, sourceType: meta.SourceType}
			layer := flowNode{kind: kindLayer, layerID: layerID}

			g.addEdge(g.idx[sender], g.idx[client], meta.LimitRate*time)
			// One layer can be distributed to multiple receivers; do not cap by single layer size
			g.addEdge(g.idx[client], g.idx[layer], math.MaxInt64)
		}
	}

	// 4. layer to receiver
	for nodeID, layerIDs := range g.assignment {
		receiver := flowNode{kind: kindReceiver, nodeID: nodeID}
		for layerID := range layerIDs {
			layer := flowNode{kind: kindLayer, layerID: layerID}
			g.addEdge(g.idx[layer], g.idx[receiver], g.layers[layerID].DataSize)
		}

		// 5. receiver to sink
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
