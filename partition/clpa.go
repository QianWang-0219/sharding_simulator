package partition

import (
	"container/heap"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"simple_go/sharding_simulator/params"
	"simple_go/sharding_simulator/utils"
	"strconv"
	"time"

	"github.com/grd/statistics"
)

// CLPA算法状态，state of constraint label propagation algorithm
type CLPAState struct {
	NetGraph          Graph          // 需运行CLPA算法的图
	PartitionMap      map[Vertex]int // 记录分片信息的 map，某个节点属于哪个分片
	Edges2Shard       []int          // Shard 相邻接的边数，对应论文中的 total weight of edges associated with label k
	VertexsNumInShard []int          // Shard 内节点的数目
	WeightPenalty     float64        // 权重惩罚，对应论文中的 beta
	MinEdges2Shard    int            // 最少的 Shard 邻接边数，最小的 total weight of edges associated with label k
	MaxEdges2Shard    int
	MaxIterations     int // 最大迭代次数，constraint，对应论文中的\tau
	CrossShardEdgeNum int // 跨分片边的总数
	ShardNum          int // 分片数目
	CtxQueue          CtxPriorityQueue
}

// 加入节点，需要将它默认归到一个分片中
func (cs *CLPAState) AddVertex(v Vertex) {
	cs.NetGraph.AddVertex(v) //增加图中的点
	if val, ok := cs.PartitionMap[v]; !ok {
		cs.PartitionMap[v] = utils.Addr2Shard(v.Addr) //随机分配一个ShardID
	} else {
		cs.PartitionMap[v] = val
	}
	cs.VertexsNumInShard[cs.PartitionMap[v]] += 1 // 此处可以批处理完之后再修改 VertexsNumInShard 参数
	// 当然也可以不处理，因为 CLPA 算法运行前会更新最新的参数
}

// 加入边，需要将它的端点（如果不存在）默认归到一个分片中
func (cs *CLPAState) AddEdge(u, v Vertex) {
	// 如果没有点，则增加边，权恒定为 1，并随机给新来的节点分配一个分片
	if _, ok := cs.NetGraph.VertexSet[u]; !ok {
		cs.AddVertex(u)
	}
	if _, ok := cs.NetGraph.VertexSet[v]; !ok {
		cs.AddVertex(v)
	}
	cs.NetGraph.AddEdge(u, v)
	// 可以批处理完之后再修改 Edges2Shard 等参数
	// 当然也可以不处理，因为 CLPA 算法运行前会更新最新的参数

	// 把跨分片交易加入跨分片交易的优先队列
	if cs.PartitionMap[u] != cs.PartitionMap[v] {
		cs.CrossShardEdgeNum++
		cs.CtxQueue.push2priorityQueue(u.Addr, v.Addr)
		cs.Edges2Shard[cs.PartitionMap[u]] += 1
		cs.Edges2Shard[cs.PartitionMap[v]] += 1
	} else {
		cs.Edges2Shard[cs.PartitionMap[u]] += 1
	}

}

// 输出CLPA
func (cs *CLPAState) PrintCLPA() {
	//cs.NetGraph.PrintGraph()
	println(cs.MinEdges2Shard)
	for v, item := range cs.PartitionMap {
		print(v.Addr, " ", item, "\t")
	}
	for _, item := range cs.Edges2Shard {
		print(item, " ")
	}
	println()
}

// 根据当前划分，计算 Wk/负载，即 Edges2Shard, 以及MinEdges2Shard, CrossShardEdgeNum
func (cs *CLPAState) ComputeEdges2Shard() {
	cs.Edges2Shard = make([]int, cs.ShardNum)
	interEdge := make([]int, cs.ShardNum)

	for idx := 0; idx < cs.ShardNum; idx++ {
		cs.Edges2Shard[idx] = 0 // 把每个分片的负载置零
		interEdge[idx] = 0      // 把每个分片的内部边数量置零
	}

	for v, lst := range cs.NetGraph.EdgeSet {
		// 获取节点 v 所属的shard
		vShard := cs.PartitionMap[v]
		//在前期工作的时候，每读一笔交易，就会调用clpastate的AddEdge(s, r)，
		//而该方法会先加点（如果交易图NetGraph中还不存在这个点），同时会随机分配一个shard id给它）
		//然后会采用map的方式加边

		for _, u := range lst { //遍历节点v的邻居节点u
			// 同上，获取邻居节点 u 所属的shard
			uShard := cs.PartitionMap[u]
			//如果节点v和邻居节点u不属于一个分片，那么给u的分片负载加一，如果两者属于一个分片，那么给这个分片的负载加一
			if vShard != uShard {
				// 判断节点 v, u 不属于同一分片，则对应的 Edges2Shard 加一
				// 仅计算入度，这样不会重复计算（对每个分片来讲，交易没有重复计算，如果加起来算总数那么会增一倍）
				cs.Edges2Shard[uShard] += 1
			} else {
				interEdge[uShard]++
			}
		}
	}

	cs.CrossShardEdgeNum = 0
	for _, val := range cs.Edges2Shard {
		cs.CrossShardEdgeNum += val //所有跨分片交易的入度之和
	}
	cs.CrossShardEdgeNum /= 2 // 因为之前存数据的时候，采用的双向边，也就是一笔交易在两个点的edgeset中存在。在上一轮for循环中，Edges2Shard[]字符串数组仅计算了每个分片的跨分片交易数量

	for idx := 0; idx < cs.ShardNum; idx++ {
		cs.Edges2Shard[idx] += interEdge[idx] / 2 // 每个分片的分片负载=内部负载+与自己相关的跨分负载（仅计算入度）
	}

	// 求最大/最小分片负载
	cs.MinEdges2Shard, cs.MaxEdges2Shard = utils.FindMinMax(cs.Edges2Shard)

}

func (cs *CLPAState) computeCTR() float64 {
	// 计算跨分片交易比例
	totalEdge := 0.0
	// 当前图的总交易数目为
	for _, lst := range cs.NetGraph.EdgeSet {
		totalEdge += float64(len(lst)) / 2.0
	}
	// 返回当前图的跨分片交易数目
	return float64(cs.CrossShardEdgeNum) / totalEdge
}

// 在账户所属分片变动时（单步），重新计算各个参数，faster
func (cs *CLPAState) changeShardRecompute(v Vertex, old int) {
	new := cs.PartitionMap[v]
	for _, u := range cs.NetGraph.EdgeSet[v] {
		neighborShard := cs.PartitionMap[u]
		if neighborShard != new && neighborShard != old { // 这条边原本是跨分交易，现在依旧是跨片交易
			cs.Edges2Shard[new]++
			cs.Edges2Shard[old]--
		} else if neighborShard == new { //原本是跨片交易，现在是片内交易
			cs.Edges2Shard[old]--
			cs.CrossShardEdgeNum--

			//从ctx的优先队列中删除该跨分片交易的记录（在第一次调用的时候就全部删光了）
			cs.CtxQueue.RemoveFromPriorityQueue(v.Addr, u.Addr)

		} else {
			cs.Edges2Shard[new]++
			cs.CrossShardEdgeNum++

			//在ctx中增加该分片的交易记录
			cs.CtxQueue.push2priorityQueue(v.Addr, u.Addr)
		}
	}
	// 求最大/最小分片负载
	cs.MinEdges2Shard, cs.MaxEdges2Shard = utils.FindMinMax(cs.Edges2Shard)
}

// 设置参数
func (cs *CLPAState) Init_CLPAState(wp float64, mIter, sn int) {
	cs.WeightPenalty = wp    //0.5
	cs.MaxIterations = mIter //100
	cs.ShardNum = sn         //8
	cs.VertexsNumInShard = make([]int, cs.ShardNum)
	cs.PartitionMap = make(map[Vertex]int)
	cs.Edges2Shard = make([]int, cs.ShardNum)

	cs.CtxQueue = make(CtxPriorityQueue, 0)
	heap.Init(&cs.CtxQueue)
}

// 初始化划分，使用节点地址的尾数划分，应该保证初始化的时候不会出现空分片
func (cs *CLPAState) Init_Partition() {
	// 设置划分默认参数
	cs.VertexsNumInShard = make([]int, cs.ShardNum)
	cs.PartitionMap = make(map[Vertex]int)
	for v := range cs.NetGraph.VertexSet {
		var va = v.Addr[len(v.Addr)-8:]
		num, err := strconv.ParseInt(va, 16, 64)
		if err != nil {
			log.Panic()
		}
		cs.PartitionMap[v] = int(num) % cs.ShardNum
		cs.VertexsNumInShard[cs.PartitionMap[v]] += 1
	}
	cs.ComputeEdges2Shard() // 删掉会更快一点，但是这样方便输出（毕竟只执行一次Init，也快不了多少）
}

// 不会出现空分片的初始化划分
func (cs *CLPAState) Stable_Init_Partition() error {
	// 设置划分默认参数
	if cs.ShardNum > len(cs.NetGraph.VertexSet) {
		return errors.New("too many shards, number of shards should be less than nodes. ")
	}
	cs.VertexsNumInShard = make([]int, cs.ShardNum)
	cs.PartitionMap = make(map[Vertex]int)
	cnt := 0
	for v := range cs.NetGraph.VertexSet {
		cs.PartitionMap[v] = int(cnt) % cs.ShardNum
		cs.VertexsNumInShard[cs.PartitionMap[v]] += 1
		cnt++
	}
	cs.ComputeEdges2Shard() // 删掉会更快一点，但是这样方便输出（毕竟只执行一次Init，也快不了多少）
	return nil
}

// 计算 将节点 v 放入 uShard 所产生的 score
func (cs *CLPAState) getShard_score(v Vertex, uShard int) float64 {
	var score float64
	// 节点 v 的出度
	v_outdegree := len(cs.NetGraph.EdgeSet[v]) //节点v一共有多少笔交易
	// uShard 与节点 v 相连的边数
	Edgesto_uShard := 0
	for _, item := range cs.NetGraph.EdgeSet[v] { //与节点v有交易的节点中如果有属于ushard的那么++
		if cs.PartitionMap[item] == uShard {
			Edgesto_uShard += 1 //统计节点v和ushard中的节点有多少笔交易
		}
	}
	score = float64(Edgesto_uShard) / float64(v_outdegree) * (1 - cs.WeightPenalty*float64(cs.Edges2Shard[uShard])/float64(cs.MinEdges2Shard))
	//score = float64(Edgesto_uShard)/float64(v_outdegree) + cs.WeightPenalty*float64(cs.MaxEdges2Shard-cs.Edges2Shard[uShard])/float64(cs.MaxEdges2Shard-cs.MinEdges2Shard)
	return score
}

// CLPA 划分算法
func (cs *CLPAState) CLPA_Partition() (map[string]uint64, int) {
	cs.ComputeEdges2Shard()
	fmt.Println("初始化分片之后的跨分片交易数目:", cs.CrossShardEdgeNum)
	fmt.Println("初始化分片之后负载分布情况:", cs.Edges2Shard)
	data4var := statistics.Float64{}
	for _, val := range cs.Edges2Shard {
		data4var = append(data4var, float64(val))
	}
	fmt.Println("各分片负载的方差，标准差", statistics.Variance(&data4var), statistics.Sd(&data4var))

	res := make(map[string]uint64)
	updateTreshold := make(map[string]int)              // 节点更新次数阈值,一个节点不能一直换组
	for iter := 0; iter < cs.MaxIterations; iter += 1 { // 第一层循环控制算法次数，constraint
		for v := range cs.NetGraph.VertexSet { // 遍历图中的每个节点
			if updateTreshold[v.Addr] >= 50 { // 一个节点最多更新50次
				continue
			}
			neighborShardScore := make(map[int]float64) //相邻片ushard的分数
			max_score := -9999.0
			vNowShard, max_scoreShard := cs.PartitionMap[v], cs.PartitionMap[v] // max_scoreShard用于保存分数最高的那个分片的id,最后该节点v就会加入ID=max_scoreShard
			for _, u := range cs.NetGraph.EdgeSet[v] {                          // 遍历该节点v的相邻节点（也就是互相之间有交易的节点）
				uShard := cs.PartitionMap[u] // uShard 邻居节点u所在的分片ID
				// 对于属于 uShard 的邻居，仅需计算一次
				if _, computed := neighborShardScore[uShard]; !computed { // 也就是说同一个分片的节点，对于这个节点v而言，分数应该是一样的，所以仅计算一次
					neighborShardScore[uShard] = cs.getShard_score(v, uShard) //计算相邻片ushard的分数
					if max_score < neighborShardScore[uShard] {               // max_score（对于该节点v而言）迭代取最大值，也就是为它找一个最合适的的分片加入
						max_score = neighborShardScore[uShard]
						max_scoreShard = uShard
					}
				}
			}
			if vNowShard != max_scoreShard && cs.VertexsNumInShard[vNowShard] > 1 {
				// 更新CLPA的状态
				cs.PartitionMap[v] = max_scoreShard
				res[v.Addr] = uint64(max_scoreShard)
				updateTreshold[v.Addr]++ // 每更新一次节点的分片位置，该节点更新次数+1（最多50次）
				// 重新计算 VertexsNumInShard
				cs.VertexsNumInShard[vNowShard] -= 1
				cs.VertexsNumInShard[max_scoreShard] += 1
				// 重新计算Wk
				cs.changeShardRecompute(v, vNowShard)
			}
		}
	} //最大迭代次数tao是事先定义的，也就意味着遍历tao次图中的每个节点
	// for sid, n := range cs.VertexsNumInShard { // 打印tao=100次迭代划分后，每个shard的节点数量
	// 	fmt.Printf("shard %d has vertexs: %d, and it has %d edges\n", sid, n, cs.Edges2Shard[sid])
	// }

	//cs.ComputeEdges2Shard()                                                       // 计算分片负载，for遍历图的节点的时候，每次更新都会重新计算，为啥又重新计算？
	fmt.Println("cross shard edge num after partitioning:", cs.CrossShardEdgeNum) // 打印稳定后跨分片交易数量

	return res, cs.CrossShardEdgeNum // 返回map[string]uint64 res=节点-更新后的分片ID， CrossShardEdgeNum=系统的跨分片交易数量
}

func Test_CLPA() {
	k := new(CLPAState)
	k.Init_CLPAState(0.5, 100, params.ShardNum)

	txfile, err := os.Open(params.FileInput)
	if err != nil {
		log.Panic("打开文件失败", err)
	}

	defer func() {
		if err := txfile.Close(); err != nil {
			log.Panic("关闭文件失败", err)
		} else {
			fmt.Println("关闭文件成功")
		}
	}()

	reader := csv.NewReader(txfile) // 创建一个CSV文件对象
	datanum := 0
	reader.Read() // 读取字段名

	for {
		data, err := reader.Read() // 按行读取文件
		if err == io.EOF || datanum == params.TotalDataSize {
			break
		}
		if err != nil {
			log.Panic(err)
		}
		s := Vertex{
			Addr: data[0], // from地址去掉前两位
		}
		r := Vertex{
			Addr: data[1], // to地址去掉前两位
		}
		k.AddEdge(s, r) // 加上s->r的边
		datanum++
	}

	// 记录开始时间
	start := time.Now()
	res, ctxNum := k.CLPA_Partition() //  返回res=节点-更新后的分片ID， CrossShardEdgeNum=系统的跨分片交易数量
	// 记录结束时间
	end := time.Now()
	// 计算执行时间并打印结果
	fmt.Printf("clpa划分执行时间为 %v\n", end.Sub(start))

	fmt.Println("划分后更改位置的账户数量:", len(res))
	fmt.Println("划分后历史事务的跨分片事务比例:", float64(ctxNum)/float64(params.TotalDataSize))

	fmt.Println("划分后各分片中账户的数目:", k.VertexsNumInShard)
	var sumAcc int = 0
	for _, accountNum := range k.VertexsNumInShard {
		sumAcc += accountNum
	}
	fmt.Println("各个分片中账户的总和", sumAcc)

	fmt.Println("划分后各个分片的负载:", k.Edges2Shard)

	data4var := statistics.Float64{}
	for _, val := range k.Edges2Shard {
		data4var = append(data4var, float64(val))
	}
	fmt.Println("各分片负载的方差，标准差", statistics.Variance(&data4var), statistics.Sd(&data4var))

	for key, val := range res {
		k.PartitionMap[Vertex{Addr: key}] = int(val)
	}
	utils.WritePartition2csv("clpa_ideal_partition.csv", res)

	// 用上一个epoch划分结果指导下一个epoch的分配
	fmt.Println("继续下一个epoch...")
	ctx := 0 //计算下一个epoch中的跨分片交易比例
	datanum = 0
	for {
		data, err := reader.Read() // 按行读取文件
		if err == io.EOF || datanum == params.TotalDataSize {
			break
		}
		if err != nil {
			log.Panic(err)
		}
		s := Vertex{
			Addr: data[0], // from地址去掉前两位
		}
		r := Vertex{
			Addr: data[1], // to地址去掉前两位
		}

		if _, ok := k.PartitionMap[s]; !ok {
			k.PartitionMap[s] = utils.Addr2Shard(s.Addr)
		}
		if _, ok := k.PartitionMap[r]; !ok {
			k.PartitionMap[r] = utils.Addr2Shard(r.Addr)
		}
		if k.PartitionMap[s] != k.PartitionMap[r] {
			ctx++
		}
		datanum++
	}
	fmt.Println("下一个纪元中的跨分片事务数量:", ctx)
	fmt.Println("下一个纪元中的款分片事务比例:", float64(ctx)/float64(params.TotalDataSize))

}
