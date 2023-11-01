// 标签传播算法的改进，用于压缩图
package partition

import (
	"fmt"
	"simple_go/sharding_simulator/params"
	"simple_go/sharding_simulator/utils"

	"time"

	"github.com/grd/statistics"
)

type PartitionStatic struct {
	NetGraph          CompressedGraph
	PartitionMap      map[int]int
	Edges2Shard       []int
	VertexsNumInShard []int
	WeightPenalty     float64
	MinEdges2Shard    int
	MaxEdges2Shard    int
	MaxIterations     int
	CrossShardEdgeNum int
	ShardNum          int
}

// 设置参数
func (ps *PartitionStatic) Init_PartitionStatic(wp float64, mIter, sn int) {
	ps.WeightPenalty = wp    //0.5
	ps.MaxIterations = mIter //100
	ps.ShardNum = sn         //8
	ps.VertexsNumInShard = make([]int, ps.ShardNum)
	ps.PartitionMap = make(map[int]int)

	// 初始化压缩图
	ps.NetGraph = CompressedGraph{
		VertexMapping: make(map[Vertex]int),
		VertexSet:     make(map[int]*CompressedVertex),
		EdgeSet:       make(map[int][]int),
	}
}

// 加入节点，需要将它默认归到一个分片中
func (ps *PartitionStatic) AddVVertex(v Vertex) {
	ps.NetGraph.AddVVertex(v) //增加图中的点

	//如果虚拟节点是新的，随机分配一个ShardID
	ps.PartitionMap[ps.NetGraph.VertexMapping[v]] = ps.fetchMap(ps.NetGraph.VertexMapping[v])
}

// 加入边，需要将它的端点（如果不存在）默认归到一个分片中
func (ps *PartitionStatic) AddVEdge(u, v Vertex) {
	// 如果没有压缩图中没有对应的点，则添加相应的点，并将它默认归到一个分片中
	if _, ok := ps.NetGraph.VertexMapping[u]; !ok {
		ps.AddVVertex(u)
	}
	if _, ok := ps.NetGraph.VertexMapping[v]; !ok {
		ps.AddVVertex(v)
	}

	// 在压缩图中添加新的边
	ps.NetGraph.AddVEdge(u, v)
}

// 静态划分算法
func (ps *PartitionStatic) PartitionStatic_Algorithm() map[string]uint64 {
	// 计算各个分片的负载，最大/最小分片负载，跨分片交易比例
	ps.ComputeEdges2Shard()
	fmt.Println("划分前跨分片交易数量:", ps.CrossShardEdgeNum)

	updateTreshold := make(map[int]int) // 节点更新次数阈值,同一个节点最多更新50次
	// 第一层循环，最大迭代次数100
	for iter := 0; iter < ps.MaxIterations; iter++ {
		// 算法部分，为每个节点寻找最优分片
		// 遍历每一个节点
		for v := range ps.NetGraph.VertexSet {
			if updateTreshold[v] >= 50 { // 一个节点最多更新50次
				continue
			}

			neighborShardScore := make(map[int]float64)
			max_score := -9999.0
			vNowShard, max_scoreShard := ps.PartitionMap[v], ps.PartitionMap[v]
			//fmt.Println(v, "vNowShard:", vNowShard)
			// 遍历节点v的每一个邻居，同一个分片中的邻居仅需计算分片分数一次
			for _, u := range ps.NetGraph.EdgeSet[v] {
				uShard := ps.PartitionMap[u]
				if _, computed := neighborShardScore[uShard]; !computed {
					neighborShardScore[uShard] = ps.getShard_score(v, uShard)
					if max_score < neighborShardScore[uShard] {
						max_score = neighborShardScore[uShard]
						max_scoreShard = uShard
						//fmt.Println("max_score", max_score, "max_scoreShard:", max_scoreShard)
					}
				}
			}
			// 处理上文找到的节点v的最佳分片max_scoreShard
			if vNowShard != max_scoreShard && ps.VertexsNumInShard[vNowShard] > 1 {
				// 更新划分状态
				//fmt.Println(ps.PartitionMap[v], "->", max_scoreShard)
				ps.PartitionMap[v] = max_scoreShard
				updateTreshold[v]++
				// 重新计算每个分片中节点的数量
				ps.VertexsNumInShard[vNowShard]--
				ps.VertexsNumInShard[max_scoreShard]++
				// 重新计算各个分片的负载
				ps.changeShardRecompute(v, vNowShard)
			}
		}
	}
	ps.ComputeEdges2Shard()
	fmt.Println("划分后跨分片交易数量:", ps.CrossShardEdgeNum)

	mmap := ps.NetGraph.VertexMapping ///mmap v.addr -> 虚拟id
	m := make(map[string]uint64)
	for v, shardid := range mmap {
		//m[v.Addr] = uint64(utils.CompressV2Shard(shardid))
		m[v.Addr] = uint64(ps.PartitionMap[shardid])
	}

	return m /// return v.addr -> shard id; ps.PartitionMap: 虚拟id -> shard id
}

// 根据当前的划分，计算每个分片的负载，最大/最小分片负载，跨分片交易比例
func (ps *PartitionStatic) ComputeEdges2Shard() {
	ps.Edges2Shard = make([]int, ps.ShardNum)
	interEdge := make([]int, ps.ShardNum) //临时参数
	// ps.MinEdges2Shard = math.MinInt64
	// ps.MaxEdges2Shard = math.MaxInt64
	ps.CrossShardEdgeNum = 0

	for idx := 0; idx < ps.ShardNum; idx++ {
		ps.Edges2Shard[idx] = 0 // 把每个分片的负载置零
		interEdge[idx] = 0      // 把每个分片的内部边数量置零
	}
	// 跨分片交易比例
	for v, lst := range ps.NetGraph.EdgeSet {
		// 获取虚拟节点v所属的分片id
		vShard := ps.PartitionMap[v]
		// 遍历节点v的每一个邻居节点u
		for _, u := range lst {
			uShard := ps.PartitionMap[u]
			if uShard != vShard {
				ps.CrossShardEdgeNum++
				ps.Edges2Shard[uShard] += 1 //仅计算入度
			} else {
				interEdge[uShard]++
			}
		}
		// 把虚拟节点的权重加入相关分片的负载中
		ps.Edges2Shard[vShard] += ps.NetGraph.VertexSet[v].Load
	}

	ps.CrossShardEdgeNum /= 2
	// 各个分片的负载
	for idx := 0; idx < params.ShardNum; idx++ {
		ps.Edges2Shard[idx] += interEdge[idx] / 2
	}
	// 求最大/最小分片负载
	ps.MinEdges2Shard, ps.MaxEdges2Shard = utils.FindMinMax(ps.Edges2Shard)
}

// 计算邻居分片的分数
func (ps *PartitionStatic) getShard_score(v int, uShard int) float64 {
	var score float64
	// 节点v的度
	v_degree := len(ps.NetGraph.EdgeSet[v])

	// 节点v与uShard相连的边数
	v_uShard := 0
	for _, item := range ps.NetGraph.EdgeSet[v] {
		if ps.PartitionMap[item] == uShard {
			v_uShard++
		}
	}

	score = float64(v_uShard) / float64(v_degree) * (1 - ps.WeightPenalty*float64(ps.Edges2Shard[uShard])/float64(ps.MinEdges2Shard))
	//score = float64(v_uShard)/float64(v_degree) + ps.WeightPenalty*float64(ps.MaxEdges2Shard-ps.Edges2Shard[uShard])/float64(ps.MaxEdges2Shard-ps.MinEdges2Shard)
	return score
}

// 更新一个节点的分片之后，对各分片负载进行重新计算
func (ps *PartitionStatic) changeShardRecompute(v, old int) {
	new := ps.PartitionMap[v]
	// 每一个节点v的邻居分片都会收到影响
	for _, u := range ps.NetGraph.EdgeSet[v] {
		neighborShard := ps.PartitionMap[u]
		if neighborShard != new && neighborShard != old {
			ps.Edges2Shard[new]++
			ps.Edges2Shard[old]--
		} else if neighborShard == new {
			ps.Edges2Shard[old]--
			ps.CrossShardEdgeNum--
		} else {
			ps.Edges2Shard[new]++
			ps.CrossShardEdgeNum++
		}
	}
	ps.MinEdges2Shard, ps.MaxEdges2Shard = utils.FindMinMax(ps.Edges2Shard)
}

// 查找虚拟节点id对应的分片
func (ps *PartitionStatic) fetchMap(i int) int {
	if _, ok := ps.PartitionMap[i]; !ok {
		ps.PartitionMap[i] = utils.CompressV2Shard(i)
		ps.VertexsNumInShard[ps.PartitionMap[i]]++
	}
	return ps.PartitionMap[i]
}

func Test_PartitionStatic_Algorithm() {
	k := new(PartitionStatic)
	k.Init_PartitionStatic(0.5, 100, params.ShardNum)

	// 构建压缩图
	cg := Test_Graph()
	k.NetGraph = cg
	startRandom := time.Now()
	// 初始化分片信息，为每个虚拟节点分配一个分片
	for i := range cg.VertexSet {
		k.fetchMap(i)
	}
	endRandom := time.Now()
	fmt.Printf("random划分执行时间为 %v\n", endRandom.Sub(startRandom))

	start := time.Now()
	// 静态图划分
	k.PartitionStatic_Algorithm()
	end := time.Now()
	// 计算执行时间并打印结果
	fmt.Printf("划分执行时间为 %v\n", end.Sub(start))

	// 打印静态图划分后的信息
	fmt.Println("划分后的跨分片交易比例：", float64(k.CrossShardEdgeNum)/float64(params.TotalDataSize))
	fmt.Println("划分后的各分片的负载分布情况：", k.Edges2Shard)

	data4var := statistics.Float64{}
	for _, val := range k.Edges2Shard {
		data4var = append(data4var, float64(val))
	}

	fmt.Println("各分片负载的方差，标准差", statistics.Variance(&data4var), statistics.Sd(&data4var))
}
