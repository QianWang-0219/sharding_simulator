package partition

import (
	"container/heap"
	"simple_go/sharding_simulator/utils"
)

// 定义candidate的优先队列
type NodePriorityItem struct {
	Node                  int     // 压缩节点
	CrossShardTxRatio     float64 // 跨分片交易比例（只考虑度）
	CrossShardTxNeighbors int
	Count                 int //队列中交易的数量
}
type NodePriorityQueue []*NodePriorityItem

// 实现 heap.Interface 接口的方法

func (pq NodePriorityQueue) Len() int {
	return len(pq)
}

func (pq NodePriorityQueue) Less(i, j int) bool {

	// 跨分片交易比例越大，优先级越高
	if pq[i].CrossShardTxRatio == pq[j].CrossShardTxRatio {
		// 当跨分片交易比例相等时，crossShardTxNeighbors越大，优先级越高
		return pq[i].CrossShardTxNeighbors > pq[j].CrossShardTxNeighbors
	}
	return pq[i].CrossShardTxRatio > pq[j].CrossShardTxRatio

}

func (pq NodePriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *NodePriorityQueue) Push(x interface{}) {
	item := x.(*NodePriorityItem)
	*pq = append(*pq, item)
}

func (pq *NodePriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[:n-1]
	return item
}

// 计算虚拟节点int跨分片交易比例
func (ps *PartitionStatic) CalculateCrossShardTxRatio(vnode int) (CTR float64, ctx int) {
	totalNeighbors := 0
	crossShardTxNeighbors := 0
	CTR = 0.0

	// if _, ok := ps.NetGraph.VertexSet[vnode]; !ok {
	// 	fmt.Printf("虚拟节点%d还未加入压缩图\n", vnode)
	// 	fmt.Println(ps.NetGraph.EdgeSet[vnode])
	// }

	for _, neighbor := range ps.NetGraph.EdgeSet[vnode] {
		if ps.PartitionMap[neighbor] != ps.PartitionMap[vnode] {
			crossShardTxNeighbors++
		}
		totalNeighbors++
	}

	if totalNeighbors > 0 {
		CTR = float64(crossShardTxNeighbors) / float64(totalNeighbors)
	}
	return CTR, crossShardTxNeighbors
}

func (ps *PartitionStatic) push2priorityQueue(vnode int, count int) {
	crossShardTxRatio, crossShardTxNeighbors := ps.CalculateCrossShardTxRatio(vnode)
	item := &NodePriorityItem{
		Node:                  vnode,
		Count:                 count,
		CrossShardTxRatio:     crossShardTxRatio,
		CrossShardTxNeighbors: crossShardTxNeighbors,
	}
	heap.Push(&ps.pq, item)
}

// 计算虚拟节点迁移之后队列的离散系数
func (ps *PartitionStatic) changeShardCOVRecompute(item *NodePriorityItem, loadBalance []float64) (float64, []float64, int) {
	_, minLoadindex := utils.FindMaxAndMinIndex(loadBalance)

	oldShardID := ps.PartitionMap[item.Node]
	newShardID := minLoadindex

	loadBalance[oldShardID] -= float64(item.Count)
	loadBalance[newShardID] += float64(item.Count)

	return utils.Std(loadBalance), loadBalance, newShardID
}
