package partition

import (
	"container/heap"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"simple_go/sharding_simulator/params"
	"simple_go/sharding_simulator/utils"
	"sort"
	"strconv"
	"sync"
	"time"
)

type OLAA_Simulator struct {
	txQueue map[int][]*transaction
	ctx     float64
	totaltx float64
	mutex   sync.Mutex
	txGraph *OLAAState
}

func NewOLAA_Simulator() *OLAA_Simulator {
	og := new(OLAAState)
	og.Init_OLAAState(params.ShardNum, 1.5, 1, 0.5)
	return &OLAA_Simulator{
		txQueue: make(map[int][]*transaction),
		ctx:     0.0,
		totaltx: 0.0,
		txGraph: og,
	}
}

func (osi *OLAA_Simulator) Test_OLAA_Simulator() {
	// 读交易
	// 打开图数据文件
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
	sendtoShard := make(map[int][]*transaction)
	consensusCounter := 0

	// 定期处理交易
	go func() {
		ticker := time.Tick(10 * time.Second)
		for range ticker {
			//区块容量，每10秒处理2000笔交易
			consensusCounter++
			osi.mutex.Lock()
			for i := 0; i < params.ShardNum; i++ {
				// 打包交易

				txNum := params.Maxtx
				if len(osi.txQueue[i]) < txNum {
					txNum = len(osi.txQueue[i])
				}
				txs_Packed := osi.txQueue[i][:txNum]    // 待处理交易
				osi.txQueue[i] = osi.txQueue[i][txNum:] //剩余交易

				for _, tx := range txs_Packed {
					tx.reqTime = time.Now() //开始共识时间
				}

				// 处理打包交易
				for _, tx := range txs_Packed {
					var ssid, rsid int
					ssid = osi.txGraph.PartitionMap[Vertex{Addr: tx.senderAddr}]
					rsid = osi.txGraph.PartitionMap[Vertex{Addr: tx.receiptAddr}]
					if ssid != rsid && !tx.relayed { //跨分片交易且还未处理
						osi.ctx += 0.5
						osi.totaltx += 0.5
						tx.relayed = true
						osi.txQueue[rsid] = append(osi.txQueue[rsid], tx)
					} else {
						tx.commitTime = time.Now()
						latency := tx.commitTime.Sub(tx.time)
						writeRes2CSV("OLAA+incre tx detail information", []string{tx.senderAddr, tx.receiptAddr, strconv.FormatBool(tx.relayed),
							tx.time.Format("2006-01-02 15:04:05.000"), tx.reqTime.Format("2006-01-02 15:04:05.000"), tx.commitTime.Format("2006-01-02 15:04:05.000"),
							strconv.FormatFloat(latency.Seconds(), 'f', 3, 64)})

						if tx.relayed {
							osi.ctx += 0.5
							osi.totaltx += 0.5
						} else {
							osi.totaltx++
						}

					}
				}

			}
			osi.mutex.Unlock()
			fmt.Printf("第%d次共识：此时跨分片交易数量%f,处理完的交易总量%f\n", consensusCounter, osi.ctx, osi.totaltx)
		}
	}()

	// 监控队列长度
	go func() {
		ticker := time.Tick(50 * time.Second)
		for range ticker {
			loadBalance := make([]float64, params.ShardNum)
			stringSlice := make([]string, len(loadBalance))
			osi.mutex.Lock()
			for i, lst := range osi.txQueue {
				osi.txGraph.Queue2Shard[i] = len(lst)
				loadBalance[i] = float64(len(lst))
				stringSlice[i] = strconv.FormatFloat(loadBalance[i], 'f', 3, 64)
			}
			osi.mutex.Unlock()
			covQ := utils.CoefficientOfVariation(loadBalance)
			stdQ := utils.Std(loadBalance)

			fmt.Println("此时各分片队列分布", loadBalance)
			fmt.Println("离散系数为:", covQ, "标准差为:", stdQ)

			writeTXpool2CSV("OLAA+incre txPool", stringSlice)

			//离散程度较高，需要重划分
			if stdQ > 3e4 || covQ > 1 {
				fmt.Println("start re-partitioning...")
				moveAcc := make(map[string]uint64) // 节点id-新的分片id
				osi.mutex.Lock()
				// 重新获取当前loadbalance
				for i, lst := range osi.txQueue {
					loadBalance[i] = float64(len(lst))
				}
			
				// step 1. 遍历当前的交易队列，统计参与排队交易的节点，以及每个节点参与的交易数目
				queueAccCount := osi.countRelatedAccounts()

				// step 2. 根据历史交易模式，生成优先队列，跨片交易占总度更大的优先级更高
				for _, queueAcc := range queueAccCount {
					osi.txGraph.push2priorityQueue(queueAcc.Account, queueAcc.Count)
				}

				// step 3. 按照虚拟节点跨片交易的比例，依次弹出优先队列中的节点
				for osi.txGraph.pq.Len() > 0 {
					item := heap.Pop(&osi.txGraph.pq).(*NodePriorityItemforOLAA)
					for i := 0; i < params.ShardNum; i++ {
						osi.txGraph.Queue2Shard[i] = int(loadBalance[i])
					}
					bestShard := osi.txGraph.ReOLAA_Partition(Vertex{Addr: item.Node}) //当前最佳位置
					if bestShard == osi.txGraph.PartitionMap[Vertex{Addr: item.Node}] {
						continue
					}
					// 计算迁移节点到最佳位置对负载均衡的影响
					newstdQ, newloadBalance := osi.txGraph.changeShardCOVRecompute(item, loadBalance, bestShard)

					fmt.Printf("IF %s: migrate from %d to %d, txs count: %d", item.Node, osi.txGraph.PartitionMap[Vertex{Addr: item.Node}], bestShard, item.Count)
					fmt.Println("loadbalance from", loadBalance, stdQ, "to", newloadBalance, newstdQ)

					if newstdQ < stdQ { //负载均衡有增益
						stdQ = newstdQ
						loadBalance = newloadBalance
						fmt.Println("...此时负载情况", loadBalance)
						// 更改分配
						osi.txGraph.PartitionMap[Vertex{Addr: item.Node}] = bestShard
						moveAcc[item.Node] = uint64(bestShard) //保留重新划分的虚拟节点

						// // 计算当前划分下的跨分片交易数目
						// osi.txGraph.CalcCrossShardEdgeNum()
					}
					if stdQ <= 3e4 && stdQ < 1 {
						break
					}
				}
				// 清空优先队列
				osi.txGraph.pq = make(NodePriorityQueueforOLAA, 0)
				// 队列中交易迁移
				if len(moveAcc) != 0 {
					osi.migrationAcc(moveAcc)
				}

				fmt.Println("重划分完成...", loadBalance)

				osi.mutex.Unlock()
			}
		}
	}()

	// 发布交易
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
		osi.mutex.Lock()
		osi.txGraph.AddEdge(s, r) //同时也会把跨分片的边加入ctx的优先队列

		// 即时账户分配算法，贪心算法：为新账户寻找最合适的分片
		ssid := osi.txGraph.OLAA_Partition(s)
		osi.txGraph.OLAA_Partition(r)
		osi.mutex.Unlock()

		// if rsid != ssid {
		// 	osi.txGraph.Queue2Shard[rsid]++
		// 	osi.txGraph.Queue2Shard[ssid]++
		// } else {
		// 	osi.txGraph.Queue2Shard[rsid]++
		// }

		// 构建交易
		tx := Newtransaction(s.Addr, r.Addr)
		sendtoShard[ssid] = append(sendtoShard[ssid], tx)

		// 模拟发送到txpool,每秒发送2500笔交易
		if datanum != 0 && datanum%params.InjectSpeed == 0 {
			for i, txs := range sendtoShard {
				for _, tx := range txs {
					tx.time = time.Now()
				}
				osi.txQueue[i] = append(osi.txQueue[i], txs...)
			}
			sendtoShard = make(map[int][]*transaction)
			time.Sleep(time.Second)
		}
		datanum++
	}
	select {}
}

// 统计队列中有哪些账户，账户现在的分片id，队列中与该节点相关的交易数量
func (osi *OLAA_Simulator) countRelatedAccounts() (result []AccountCountforG) {
	accounts := make(map[string]int)

	for shardID, transactions := range osi.txQueue {
		for _, tx := range transactions {
			if tx.relayed {
				accounts[tx.receiptAddr]++
			} else {
				accounts[tx.senderAddr]++
			}
		}

		for acc, count := range accounts {
			result = append(result, AccountCountforG{
				Account: acc,
				ShardID: shardID,
				Count:   count,
			})
		}
		accounts = make(map[string]int)
	}

	// 只保留交易量top100的压缩节点
	// 按交易数量进行排序
	sort.Slice(result, func(i, j int) bool {
		return result[i].Count > result[j].Count
	})
	// 只保留交易数量前100的账户
	if len(result) > 100 {
		result = result[:100]
	}

	return result
}

// 根据重划分结果，对队列中的交易进行迁移
func (osi *OLAA_Simulator) migrationAcc(mmap map[string]uint64) {
	fmt.Println("需要迁移的虚拟节点", len(mmap)) //clpa可能会非常多

	txSend := make(map[uint64][]*transaction) //需要迁移的交易
	for shardID := 0; shardID < len(osi.txQueue); shardID++ {
		firstPtr := 0 // 保存无需迁移的交易
		for secondStr := 0; secondStr < len(osi.txQueue[shardID]); secondStr++ {
			ptx := osi.txQueue[shardID][secondStr] //指向需要迁移的交易

			value1, ok1 := mmap[ptx.senderAddr]
			condition1 := ok1 && !ptx.relayed // 非relayed交易，且sender账户需要迁移

			value2, ok2 := mmap[ptx.receiptAddr]
			condition2 := ok2 && ptx.relayed // relayed交易，且receipt账户需要迁移

			if condition1 {
				txSend[value1] = append(txSend[value1], ptx)
			} else if condition2 {
				txSend[value2] = append(txSend[value2], ptx)
			} else { // 无需迁移
				osi.txQueue[shardID][firstPtr] = ptx
				firstPtr++
			}
		}
		osi.txQueue[shardID] = osi.txQueue[shardID][:firstPtr]
		fmt.Printf("after cutting len of queue %d: %d\n", shardID, len(osi.txQueue[shardID]))

	}
	for targetshardID := 0; targetshardID < len(osi.txQueue); targetshardID++ {
		osi.txQueue[targetshardID] = append(osi.txQueue[targetshardID], txSend[uint64(targetshardID)]...)
	}

	//////
	loadBalance := make([]float64, params.ShardNum)
	for i, lst := range osi.txQueue {
		loadBalance[i] = float64(len(lst))
	}
	fmt.Println("迁移后的负载", loadBalance)
}
