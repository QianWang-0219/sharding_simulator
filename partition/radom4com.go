package partition

import (
	"fmt"
	"simple_go/sharding_simulator/params"
	"simple_go/sharding_simulator/utils"
)

type RandomState struct {
	PartitionMap map[int]int
	ctxNum       int
	Edges2Shard  []int //各分片的负载（包括片内和跨片）
}

func (rs *RandomState) Init_RandomState() {
	rs.PartitionMap = make(map[int]int)
	rs.ctxNum = 0
	rs.Edges2Shard = make([]int, params.ShardNum)
}

func (rs *RandomState) fetchMap(v *CompressedVertex) int {
	if _, ok := rs.PartitionMap[v.VirtualID]; !ok {
		rs.PartitionMap[v.VirtualID] = utils.CompressV2Shard(v.VirtualID)
	}
	return rs.PartitionMap[v.VirtualID]
}

func Test_Random() {
	cg := Test_Graph() //把数据读进压缩图
	k := new(RandomState)
	k.Init_RandomState()
	// 为压缩图中的每一个节点分配一个分片
	for i := range cg.VertexSet {
		k.fetchMap(cg.VertexSet[i])
	}

	// 统计一些随机分配后的信息
	interEdge := make([]int, params.ShardNum) //临时参数
	// 跨分片交易比例
	for v, lst := range cg.EdgeSet {
		// 获取虚拟节点v所属的分片id
		vShard := k.PartitionMap[v]

		// 遍历节点v的每一个邻居节点u
		for _, u := range lst {
			uShard := k.PartitionMap[u]
			if uShard != vShard {
				k.ctxNum++
				k.Edges2Shard[uShard] += 1 //仅计算入度
			} else {
				interEdge[uShard]++
			}
		}
		// 把虚拟节点的权重加入相关分片的负载中
		k.Edges2Shard[vShard] += cg.VertexSet[v].Load
	}
	k.ctxNum /= 2
	// 各个分片的负载
	for idx := 0; idx < params.ShardNum; idx++ {
		k.Edges2Shard[idx] += interEdge[idx] / 2
	}

	fmt.Println("跨分片交易比例:", float64(k.ctxNum)/float64(params.TotalDataSize))
	fmt.Println("Edges2Shard:", k.Edges2Shard)
}
