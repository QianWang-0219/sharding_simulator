package partition

import "simple_go/sharding_simulator/utils"

type CompressedVertex struct {
	VirtualID int             // 虚拟节点的编号
	Original  map[Vertex]bool // 记录该虚拟节点聚合的原始节点
	Load      int             // 该虚拟节点内节点之间的交易和
}

type CompressedGraph struct {
	VertexMapping map[Vertex]int            //建立原始节点和虚拟节点之间的关系
	VertexSet     map[int]*CompressedVertex // 压缩节点集合，其实是 set
	EdgeSet       map[int][]int             // 记录压缩节点与压缩节点之间的边，邻接表
}

func CompressGraph(originalGraph Graph) CompressedGraph {
	// 初始化压缩图
	compressedGraph := CompressedGraph{
		VertexMapping: make(map[Vertex]int),
		VertexSet:     make(map[int]*CompressedVertex),
		EdgeSet:       make(map[int][]int),
	}

	//vertexMapping := make(map[Vertex]int) //建立原始节点和虚拟节点之间的关系
	// 遍历原始图的节点
	for originalVertex := range originalGraph.VertexSet {
		virtualID := utils.CompressAddr(originalVertex.Addr)
		//fmt.Println(originalVertex.Addr, virtualID)

		// 判断压缩图中是否已经有该压缩节点
		if _, ok := compressedGraph.VertexSet[virtualID]; !ok {
			compressedGraph.VertexSet[virtualID] = &CompressedVertex{
				VirtualID: virtualID,
				Original:  make(map[Vertex]bool),
			}
		}

		// 把原始节点存入压缩图中对应的压缩节点
		compressedGraph.VertexSet[virtualID].Original[originalVertex] = true
		compressedGraph.VertexMapping[originalVertex] = virtualID
	}

	// 遍历原始图的边
	for startVertex, endVertices := range originalGraph.EdgeSet {
		startVirtualID := compressedGraph.VertexMapping[startVertex]

		for _, endVertex := range endVertices {
			endVirtualID := compressedGraph.VertexMapping[endVertex]

			if startVirtualID == endVirtualID {
				compressedGraph.VertexSet[startVirtualID].Load++
			} else {
				compressedGraph.EdgeSet[startVirtualID] = append(compressedGraph.EdgeSet[startVirtualID], endVirtualID)
				//compressedGraph.EdgeSet[endVirtualID] = append(compressedGraph.EdgeSet[endVirtualID], startVirtualID)
			}
		}
	}
	for id := range compressedGraph.VertexSet {
		compressedGraph.VertexSet[id].Load /= 2
	}

	return compressedGraph
}

// 添加节点到虚拟节点
func (cg *CompressedGraph) AddVVertex(v Vertex) {
	virtualID := utils.CompressAddr(v.Addr) //获得节点对应的虚拟节点的位置
	// 判断压缩图中是否已经有该压缩节点
	if _, ok := cg.VertexSet[virtualID]; !ok {
		cg.VertexSet[virtualID] = &CompressedVertex{
			VirtualID: virtualID,
			Original:  make(map[Vertex]bool),
			Load:      0,
		}
	}
	// 把原始节点存入压缩图中对应的压缩节点
	cg.VertexSet[virtualID].Original[v] = true
	cg.VertexMapping[v] = virtualID
}

// 添加边到压缩图中
func (cg *CompressedGraph) AddVEdge(u, v Vertex) {
	cg.AddVVertex(u)
	cg.AddVVertex(v)

	// 添加边
	startVirtualID := cg.VertexMapping[u]
	endVirtualID := cg.VertexMapping[v]

	if startVirtualID == endVirtualID { //同一个虚拟节点
		cg.VertexSet[startVirtualID].Load++
	} else { //不同虚拟节点，虚拟节点之间的双向边
		cg.EdgeSet[startVirtualID] = append(cg.EdgeSet[startVirtualID], endVirtualID)
		cg.EdgeSet[endVirtualID] = append(cg.EdgeSet[endVirtualID], startVirtualID)
	}
}
