package partition

import "container/heap"

// 定义跨分片交易
type CtxItem struct {
	Addr1 string
	Addr2 string
	Count int
}

// 定义跨分片交易的优先队列
type CtxPriorityQueue []*CtxItem

// 实现heap.Interface接口的方法
func (q CtxPriorityQueue) Len() int {
	return len(q)
}

func (q CtxPriorityQueue) Less(i, j int) bool {
	return q[i].Count > q[j].Count // 按照Count降序排序
}

func (q CtxPriorityQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

func (q *CtxPriorityQueue) Push(x interface{}) {
	item := x.(*CtxItem)
	*q = append(*q, item)
}

func (q *CtxPriorityQueue) Pop() interface{} {
	old := *q
	n := len(old)
	item := old[n-1]
	*q = old[0 : n-1]
	return item
}

func (q *CtxPriorityQueue) RemoveFromPriorityQueue(senderAddr, receiptAddr string) {
	// 遍历优先队列找到目标项
	for i, ctx := range *q {
		if (ctx.Addr1 == senderAddr && ctx.Addr2 == receiptAddr) || (ctx.Addr1 == receiptAddr && ctx.Addr2 == senderAddr) {
			// 使用heap.Remove将目标项删除
			heap.Remove(q, i)
			break
		}
	}
}

// 在ctx的优先队列里增加一个item，或者在现有item中count++
func (q *CtxPriorityQueue) push2priorityQueue(senderAddr, receiptAddr string) {
	// 判断是否存在相同的跨分片交易
	var existed bool
	// 如果ctx的优先队列中已经有该交易了，数量++
	for _, ctx := range *q {
		if (ctx.Addr1 == senderAddr && ctx.Addr2 == receiptAddr) || (ctx.Addr1 == receiptAddr && ctx.Addr2 == senderAddr) {
			ctx.Count++
			existed = true
			break
		}
	}

	if !existed {
		// 如果不存在该跨分片交易，创建交易
		ctx := &CtxItem{
			Addr1: senderAddr,
			Addr2: receiptAddr,
			Count: 1,
		}
		heap.Push(q, ctx)
	}
}
