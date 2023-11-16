package partition

import (
	"container/heap"
	"simple_go/sharding_simulator/utils"
)

// OLAA 算法的状态
type OLAAState struct {
	NetGraph          Graph          //交易图
	PartitionMap      map[Vertex]int //账户划分结果：账户->分片ID
	Edges2Shard       []int          //各个分片处理过的交易数目
	CrossShardEdgeNum int            //跨分片交易总数
	ShardNum          int            //分片数目
	Queue2Shard       []int          //队列长度
	lambda            float64        //负载权重
	epsilon           int            //分母调节因子
	pq                NodePriorityQueueforOLAA
	WeightPenalty     float64 // 权重惩罚，对应论文中的 beta
	MinEdges2Shard    int     // 最少的 Shard 邻接边数，最小的 total weight of edges associated with label k
	MaxEdges2Shard    int
	CtxQueue          CtxPriorityQueue
}

// 设置参数
func (ol *OLAAState) Init_OLAAState(sn int, l float64, e int, wp float64) {
	ol.ShardNum = sn
	ol.PartitionMap = make(map[Vertex]int)
	ol.Edges2Shard = make([]int, ol.ShardNum)
	ol.Queue2Shard = make([]int, ol.ShardNum)
	ol.lambda = l
	ol.epsilon = e

	ol.pq = make(NodePriorityQueueforOLAA, 0)
	heap.Init(&ol.pq)

	ol.WeightPenalty = wp //0.5

	ol.CtxQueue = make(CtxPriorityQueue, 0)
	heap.Init(&ol.CtxQueue)
}

// 供外部(SUPERVISOR处理区块信息)调用，修改交易图
// 加入边/交易
func (ol *OLAAState) AddEdge(u, v Vertex) {
	// 如果是新账户
	if _, ok := ol.NetGraph.VertexSet[u]; !ok {
		ol.AddVertex(u)
	}
	if _, ok := ol.NetGraph.VertexSet[v]; !ok {
		ol.AddVertex(v)
	}
	// 在交易图中添加边的关系
	ol.NetGraph.AddEdge(u, v) //增加交易图中的边

	// // 把跨分片交易加入跨分片交易的优先队列
	// if ol.PartitionMap[u] != ol.PartitionMap[v] {
	// 	ol.CrossShardEdgeNum++
	// 	ol.CtxQueue.Push2priorityQueue(u.Addr, v.Addr)
	// 	ol.Edges2Shard[ol.PartitionMap[u]] += 1
	// 	ol.Edges2Shard[ol.PartitionMap[v]] += 1
	// } else {
	// 	ol.Edges2Shard[ol.PartitionMap[u]] += 1
	// }

}

// 加入账户/节点（供AddEdge调用）
func (ol *OLAAState) AddVertex(v Vertex) {
	ol.NetGraph.AddVertex(v) //增加交易图中的节点
}

// 根据当前交易图状态，计算跨分片交易数目CrossShardEdgeNum,每个分片处理过的验证过的交易数目Edges2Shard
func (ol *OLAAState) CalcCrossShardEdgeNum() {
	ol.Edges2Shard = make([]int, ol.ShardNum)
	interEdge := make([]int, ol.ShardNum)

	for idx := 0; idx < ol.ShardNum; idx++ {
		ol.Edges2Shard[idx] = 0 // 把每个分片的负载置零
		interEdge[idx] = 0      // 把每个分片的内部边数量置零
	}

	for v, lst := range ol.NetGraph.EdgeSet {
		// 获取节点 v 所属的shard
		vShard := ol.PartitionMap[v]

		for _, u := range lst { //遍历节点v的邻居节点u
			// 同上，获取邻居节点 u 所属的shard
			uShard := ol.PartitionMap[u]
			//如果节点v和邻居节点u不属于一个分片，那么给u的分片负载加一，如果两者属于一个分片，那么给这个分片的负载加一
			if vShard != uShard {
				// 判断节点 v, u 不属于同一分片，则对应的 Edges2Shard 加一
				ol.Edges2Shard[uShard] += 1
			} else {
				interEdge[uShard]++
			}
		}
	}
	ol.CrossShardEdgeNum = 0
	for _, val := range ol.Edges2Shard {
		ol.CrossShardEdgeNum += val
	}
	ol.CrossShardEdgeNum /= 2

	for idx := 0; idx < ol.ShardNum; idx++ {
		ol.Edges2Shard[idx] += interEdge[idx] / 2 // 每个分片的分片负载=内部负载+与自己相关的跨分负载（仅计算入度）
	}

	ol.MinEdges2Shard, ol.MaxEdges2Shard = utils.FindMinMax(ol.Edges2Shard)
}

// 计算输入账户v相对于邻居分片的分数
func (ol *OLAAState) getShard_score(v Vertex, uShardID int) float64 {
	var score float64
	// 计算与节点v相关的交易数量（分母）
	vDegree := len(ol.NetGraph.EdgeSet[v])
	//fmt.Println("节点v相关的交易数量:", vDegree)
	// 计算与uShardID分片相关的交易数量（分子）
	v2uShardDegree := 0
	for _, item := range ol.NetGraph.EdgeSet[v] {
		if ol.PartitionMap[item] == uShardID {
			v2uShardDegree += 1
		}
	}
	//fmt.Println("节点v与uShardID分片相关的交易数量:", v2uShardDegree)

	score = float64(v2uShardDegree) / float64(vDegree)

	return score
}

// 计算当前交易图的跨分片交易比例和负载均衡情况
func (ol *OLAAState) calculateCTR() (CTR, STD, COV float64) {
	totalEdge := 0.0
	for _, lst := range ol.NetGraph.EdgeSet {
		totalEdge += float64(len(lst)) / 2.0
	}
	CTR = float64(ol.CrossShardEdgeNum) / totalEdge

	loadbalance := utils.ConvertIntToFloat(ol.Edges2Shard)
	// fmt.Println(loadbalance)
	STD = utils.Std(loadbalance)

	COV = utils.CoefficientOfVariation(loadbalance)

	return
}

// 账户变动之后，重新计算各参数(单步)
func (ol *OLAAState) changeShardRecompute(v Vertex, old, new int) {
	for _, u := range ol.NetGraph.EdgeSet[v] {
		neighborShard := ol.PartitionMap[u]
		if neighborShard != new && neighborShard != old {
			ol.Edges2Shard[new]++
			ol.Edges2Shard[old]--
		} else if neighborShard == new {
			ol.Edges2Shard[old]--
			ol.CrossShardEdgeNum--
			//从ctx的优先队列中删除该跨分片交易的记录（在第一次调用的时候就全部删光了）
			//ol.CtxQueue.RemoveFromPriorityQueue(v.Addr, u.Addr)

		} else {
			ol.Edges2Shard[new]++
			ol.CrossShardEdgeNum++
			//在ctx中增加该分片的交易记录
			//ol.CtxQueue.Push2priorityQueue(v.Addr, u.Addr)
		}
	}
	ol.MinEdges2Shard, ol.MaxEdges2Shard = utils.FindMinMax(ol.Edges2Shard)
}

func (ol *OLAAState) OLAA_Partition(v Vertex) int {
	//ol.CalcCrossShardEdgeNum()
	// 为输入账户寻找最合适的分片 placement
	// 判断账户v是否为新账户,已经有分配结果的直接返回结果
	if _, ok := ol.PartitionMap[v]; !ok {
		size := ol.ShardNum // 切片大小
		// 为新账户分配一个合适的分片id
		totalScore := make([]float64, size)

		// 计算适应度得分
		scoreCross := make(map[int]float64)
		for _, u := range ol.NetGraph.EdgeSet[v] { //遍历当前节点v的相邻节点
			if uShard, ok := ol.PartitionMap[u]; ok {
				// 邻居节点已经分配账户了
				if _, computed := scoreCross[uShard]; !computed { //对每个相关分片最多计算一次
					scoreCross[uShard] = ol.getShard_score(v, uShard)
					//writer.Write([]string{"适应度得分", fmt.Sprintf("%d", uShard), fmt.Sprintf("%f", scoreCross[uShard])})
				}
			}
		}

		// 计算负载均衡得分
		scoreBal := make([]float64, size)
		ol.MinEdges2Shard, ol.MaxEdges2Shard = utils.FindMinMax(ol.Edges2Shard)
		for i := 0; i < ol.ShardNum; i++ {
			//scoreBal[i] = ol.lambda * float64(maxQ-ol.Queue2Shard[i]) / float64(ol.epsilon+maxQ-minQ)
			scoreBal[i] = ol.lambda * float64(ol.MaxEdges2Shard-ol.Edges2Shard[i]) / float64(ol.epsilon+ol.MaxEdges2Shard-ol.MinEdges2Shard)
			//writer.Write([]string{"负载均衡得分", fmt.Sprintf("%d", i), fmt.Sprintf("%f", scoreBal[i])})
			if _, ok := scoreCross[i]; ok {
				totalScore[i] = scoreBal[i] + scoreCross[i]
			} else {
				totalScore[i] = scoreBal[i]
			}
		}
		maxIndex, _ := utils.FindMaxIndexINf(totalScore)

		ol.PartitionMap[v] = maxIndex

		return maxIndex
	}
	vNowShard := ol.PartitionMap[v] // 获取账户v当前分片id
	return vNowShard
}

func (ol *OLAAState) ReOLAA_Partition(v Vertex) int {
	ol.lambda = 1.0

	ol.CalcCrossShardEdgeNum()
	// 为输入账户寻找最合适的分片 placement
	size := ol.ShardNum // 切片大小
	// 为新账户分配一个合适的分片id
	totalScore := make([]float64, size)

	// 计算适应度得分
	scoreCross := make(map[int]float64)
	//fmt.Println(ol.NetGraph.EdgeSet[v])
	for _, u := range ol.NetGraph.EdgeSet[v] { //遍历当前节点v的相邻节点
		if uShard, ok := ol.PartitionMap[u]; ok {
			// 邻居节点已经分配账户了
			if _, computed := scoreCross[uShard]; !computed { //对每个相关分片最多计算一次
				scoreCross[uShard] = ol.getShard_score(v, uShard)
			}
		}
	}

	// 计算负载均衡得分
	//scoreBal := make([]float64, size)
	minQ, _ := utils.FindMinMax(ol.Queue2Shard)
	//fmt.Println("当前分片队列:", ol.Queue2Shard)
	for i := 0; i < ol.ShardNum; i++ {
		// scoreBal[i] = ol.lambda * float64(maxQ-ol.Queue2Shard[i]) / float64(ol.epsilon+maxQ-minQ)
		if _, ok := scoreCross[i]; ok {
			//totalScore[i] = scoreBal[i] + scoreCross[i]
			totalScore[i] = scoreCross[i] * (1 - ol.WeightPenalty*float64(ol.Queue2Shard[i])/float64(minQ))
			//totalScore[i] = scoreCross[i] * (1- ol.WeightPenalty*float64(ol.Edges2Shard[i])/float64(ol.MinEdges2Shard))
		} else {
			//totalScore[i] = scoreBal[i]
			totalScore[i] = 0
		}
	}

	// // 计算负载均衡得分(根据队列中的负载)
	// scoreBal := make([]float64, size)
	// //ol.MinEdges2Shard, ol.MaxEdges2Shard = utils.FindMinMax(ol.Edges2Shard)
	// minQ, maxQ := utils.FindMinMax(ol.Queue2Shard)
	// for i := 0; i < ol.ShardNum; i++ {
	// 	scoreBal[i] = ol.lambda * float64(maxQ-ol.Queue2Shard[i]) / float64(ol.epsilon+maxQ-minQ)
	// 	if _, ok := scoreCross[i]; ok {
	// 		totalScore[i] = scoreBal[i] + scoreCross[i]
	// 	} else {
	// 		totalScore[i] = scoreBal[i]
	// 	}
	// }

	maxIndex, _ := utils.FindMaxIndexINf(totalScore)
	return maxIndex
}

// 计算节点迁移之后队列的标准差，把bestAddr迁移到bestShard
func (ol *OLAAState) changeShardCOVRecompute(bestAddr string, bestShard int, count int) (float64, float64, []float64) {
	loadBalance := utils.ConvertIntToFloat(ol.Edges2Shard)

	lb := make([]float64, len(loadBalance))
	copy(lb, loadBalance)

	oldShardID := ol.PartitionMap[Vertex{Addr: bestAddr}]
	newShardID := bestShard

	lb[oldShardID] -= float64(count)
	lb[newShardID] += float64(count)

	return utils.Std(lb), utils.CoefficientOfVariation(lb), lb
}
