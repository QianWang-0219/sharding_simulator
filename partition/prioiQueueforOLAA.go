package partition

import (
	"container/heap"
	"simple_go/sharding_simulator/utils"
)

// 定义candidate的优先队列
type NodePriorityItemforOLAA struct {
	Node                  string
	CrossShardTxRatio     float64 // 跨分片交易占所有度之中的比例
	CrossShardTxNeighbors int
	Count                 int //队列中交易的数量

}
type NodePriorityQueueforOLAA []*NodePriorityItemforOLAA

// 实现 heap.Interface 接口的方法

func (pq NodePriorityQueueforOLAA) Len() int {
	return len(pq)
}

func (pq NodePriorityQueueforOLAA) Less(i, j int) bool {

	// 跨分片交易比例越大，优先级越高
	if pq[i].CrossShardTxRatio == pq[j].CrossShardTxRatio {
		// 当跨分片交易比例相等时，crossShardTxNeighbors越大，优先级越高
		return pq[i].CrossShardTxNeighbors > pq[j].CrossShardTxNeighbors
	}
	return pq[i].CrossShardTxRatio > pq[j].CrossShardTxRatio

}

func (pq NodePriorityQueueforOLAA) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *NodePriorityQueueforOLAA) Push(x interface{}) {
	item := x.(*NodePriorityItemforOLAA)
	*pq = append(*pq, item)
}

func (pq *NodePriorityQueueforOLAA) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[:n-1]
	return item
}

// 计算节点的跨分片交易比例
func (ol *OLAAState) CalculateCrossShardTxRatio(vnode string) (CTR float64, ctx int) {
	totalNeighbors := 0
	crossShardTxNeighbors := 0
	CTR = 0.0

	for _, neighbor := range ol.NetGraph.EdgeSet[Vertex{Addr: vnode}] {
		if ol.PartitionMap[neighbor] != ol.PartitionMap[Vertex{Addr: vnode}] {
			crossShardTxNeighbors++
		}
		totalNeighbors++
	}

	if totalNeighbors > 0 {
		CTR = float64(crossShardTxNeighbors) / float64(totalNeighbors)
	}
	return CTR, crossShardTxNeighbors
}

func (ol *OLAAState) push2priorityQueue(vnode string, count int) {
	crossShardTxRatio, crossShardTxNeighbors := ol.CalculateCrossShardTxRatio(vnode)
	item := &NodePriorityItemforOLAA{
		Node:                  vnode,
		Count:                 count,
		CrossShardTxRatio:     crossShardTxRatio,
		CrossShardTxNeighbors: crossShardTxNeighbors,
	}
	heap.Push(&ol.pq, item)
}

// 计算虚拟节点迁移之后队列的标准差
func (ol *OLAAState) changeShardCOVRecompute(item *NodePriorityItemforOLAA, loadBalance []float64, bestShard int) (float64, []float64) {
	lb := make([]float64, len(loadBalance))
	copy(lb, loadBalance)

	oldShardID := ol.PartitionMap[Vertex{Addr: item.Node}]
	newShardID := bestShard

	lb[oldShardID] -= float64(item.Count)
	lb[newShardID] += float64(item.Count)

	return utils.Std(lb), lb
}
