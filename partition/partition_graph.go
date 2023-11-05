// 非压缩图，配合clpa，用来做对比实验
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

type SimulatorGraph struct {
	txQueue             map[int][]*transaction
	ctx                 float64
	totaltx             float64
	mutex               sync.Mutex
	txGraph             *CLPAState
	clpaLastRunningTime time.Time
	CTR                 float64 //当前图划分下的割边比例
	staticPartition     bool
}

type AccountCountforG struct {
	Account string
	ShardID int // ß节点此时的分片id
	Count   int // 队列中与该节点相关的交易数量
}

func NewSimulatorGraph() *SimulatorGraph {
	cg := new(CLPAState)
	cg.Init_CLPAState(0.5, 100, params.ShardNum)

	return &SimulatorGraph{
		txQueue:         make(map[int][]*transaction),
		ctx:             0.0,
		totaltx:         0.0,
		txGraph:         cg,
		CTR:             0.0,
		staticPartition: false,
	}
}

func (sg *SimulatorGraph) Test_GraphPartition() {
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
		ticker := time.Tick(10 * time.Second) // 每隔 xx 秒触发一次输出，模拟出块间隔
		for range ticker {
			//区块容量，每10秒处理2000笔交易
			consensusCounter++
			sg.mutex.Lock()
			for i := 0; i < params.ShardNum; i++ {
				txNum := params.Maxtx
				if len(sg.txQueue[i]) < txNum {
					txNum = len(sg.txQueue[i])
				}
				txs_Packed := sg.txQueue[i][:txNum]   // 待处理交易
				sg.txQueue[i] = sg.txQueue[i][txNum:] //剩余交易
				for _, tx := range txs_Packed {
					tx.reqTime = time.Now() //开始共识时间
				}
				// Simulate some processing time
				//time.Sleep(time.Duration(params.ConsensusTime) * time.Millisecond)

				for _, tx := range txs_Packed {
					var ssid, vsid int
					if _, ok := sg.txGraph.PartitionMap[Vertex{Addr: tx.senderAddr}]; !ok {
						ssid = utils.Addr2Shard(tx.senderAddr)
					} else {
						ssid = sg.txGraph.PartitionMap[Vertex{Addr: tx.senderAddr}]
					}
					if _, ok := sg.txGraph.PartitionMap[Vertex{Addr: tx.receiptAddr}]; !ok {
						vsid = utils.Addr2Shard(tx.receiptAddr)
					} else {
						vsid = sg.txGraph.PartitionMap[Vertex{Addr: tx.receiptAddr}]
					}

					if ssid != vsid && !tx.relayed { //跨分片交易且还未处理
						// 维持一个跨分片交易的优先队列
						//sg.push2priorityQueue(tx)

						sg.ctx += 0.5
						sg.totaltx += 0.5
						tx.relayed = true
						sg.txQueue[vsid] = append(sg.txQueue[vsid], tx)
					} else {
						sg.txGraph.AddEdge(Vertex{Addr: tx.senderAddr}, Vertex{Addr: tx.receiptAddr})
						// 该交易的用户感知时延
						tx.commitTime = time.Now()
						latency := tx.commitTime.Sub(tx.time)
						writeRes2CSV("CLPA+ tx detail information", []string{tx.senderAddr, tx.receiptAddr, strconv.FormatBool(tx.relayed),
							tx.time.Format("2006-01-02 15:04:05.000"), tx.reqTime.Format("2006-01-02 15:04:05.000"), tx.commitTime.Format("2006-01-02 15:04:05.000"),
							strconv.FormatFloat(latency.Seconds(), 'f', 3, 64)})
						if tx.relayed {
							sg.ctx += 0.5
							sg.totaltx += 0.5
						} else {
							sg.totaltx++
						}
					}
				}
			}
			fmt.Printf("第%d次共识：此时跨分片交易数量%f,处理完的交易总量%f\n", consensusCounter, sg.ctx, sg.totaltx)
			sg.mutex.Unlock()
		}
	}()

	// 监控队列长度/分片负载
	go func() {
		ticker := time.Tick(50 * time.Second)
		for range ticker {
			loadBalance := make([]float64, params.ShardNum)
			stringSlice := make([]string, len(loadBalance))
			sg.mutex.Lock()
			for i, lst := range sg.txQueue {
				loadBalance[i] = float64(len(lst))
				stringSlice[i] = strconv.FormatFloat(loadBalance[i], 'f', 3, 64)
			}
			sg.mutex.Unlock()
			covQ := utils.CoefficientOfVariation(loadBalance)
			stdQ := utils.Std(loadBalance)

			fmt.Println("此时各分片队列分布", loadBalance)
			fmt.Println("离散系数为:", covQ, "标准差为:", stdQ)

			writeTXpool2CSV("CLPA+ txPool", stringSlice)

		}
	}()

	// 监控图划分的质量
	go func() {
		ticker := time.Tick(50 * time.Second)
		for range ticker {
			mmap := make(map[string]uint64)
			sg.mutex.Lock()
			for sg.staticPartition && (sg.txGraph.computeCTR()-sg.CTR) > 0.2 && len(mmap) < 100 {
				// 增量式重划分
				ctx := heap.Pop(&sg.txGraph.CtxQueue).(*CtxItem)
				// 计算跨分片两边账户迁移的最小增益
				SG_1 := 2*ctx.Count - len(sg.txGraph.NetGraph.EdgeSet[Vertex{Addr: ctx.Addr1}])
				SG_2 := 2*ctx.Count - len(sg.txGraph.NetGraph.EdgeSet[Vertex{Addr: ctx.Addr2}])
				if SG_1 > SG_2 && SG_1 > 0 { //将addr1移动到addr2所在的分片，对跨分片交易比例更友好
					mmap[ctx.Addr1] = uint64(sg.txGraph.PartitionMap[Vertex{Addr: ctx.Addr2}])
					tempOld := sg.txGraph.PartitionMap[Vertex{Addr: ctx.Addr1}]
					sg.txGraph.PartitionMap[Vertex{Addr: ctx.Addr1}] = sg.txGraph.PartitionMap[Vertex{Addr: ctx.Addr2}]
					sg.txGraph.changeShardRecompute(Vertex{Addr: ctx.Addr1}, tempOld)
				} else if SG_1 < SG_2 && SG_2 > 0 { //将addr2移动到addr1所在的分片
					mmap[ctx.Addr2] = uint64(sg.txGraph.PartitionMap[Vertex{Addr: ctx.Addr1}])
					tempOld := sg.txGraph.PartitionMap[Vertex{Addr: ctx.Addr2}]
					sg.txGraph.PartitionMap[Vertex{Addr: ctx.Addr2}] = sg.txGraph.PartitionMap[Vertex{Addr: ctx.Addr1}]
					sg.txGraph.changeShardRecompute(Vertex{Addr: ctx.Addr2}, tempOld)
				}
			}
			//交易池中交易迁移
			if len(mmap) > 0 {
				sg.migrationAcc(mmap)
			}
			sg.mutex.Unlock()
		}
	}()

	// 添加一些新的边给图
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

		var ssid int //sender压缩节点的分片id
		sg.mutex.Lock()
		if _, ok := sg.txGraph.PartitionMap[s]; !ok {
			ssid = utils.Addr2Shard(s.Addr)
		} else {
			ssid = sg.txGraph.PartitionMap[s]
		}
		sg.mutex.Unlock()

		// 构建交易
		tx := Newtransaction(s.Addr, r.Addr)
		sendtoShard[ssid] = append(sendtoShard[ssid], tx)

		// 模拟发送到txpool,每秒发送2500笔交易
		if datanum != 0 && datanum%params.InjectSpeed == 0 {
			// set the algorithm timer begins
			if sg.clpaLastRunningTime.IsZero() {
				sg.clpaLastRunningTime = time.Now()
			}
			for i, txs := range sendtoShard {
				for _, tx := range txs {
					tx.time = time.Now()
				}
				sg.txQueue[i] = append(sg.txQueue[i], txs...)
			}
			sendtoShard = make(map[int][]*transaction)
			time.Sleep(time.Second)
		}
		datanum++

		// clpa划分
		if !sg.staticPartition && !sg.clpaLastRunningTime.IsZero() && time.Since(sg.clpaLastRunningTime) >= time.Duration(params.ClpaFreq)*time.Second {
			sg.mutex.Lock()
			mmap, _ := sg.txGraph.CLPA_Partition() //需要移动的账户
			sg.CTR = sg.txGraph.computeCTR()
			// 迁移队列中的交易
			sg.migrationAcc(mmap)

			sg.mutex.Unlock()
			time.Sleep(10 * time.Second)
			//sg.clpaLastRunningTime = time.Now()
			sg.staticPartition = true
		}

	}

	select {}
	// // 交易发送完之后依旧要定期重划分
	// for {
	// 	time.Sleep(time.Second)
	// 	if time.Since(sg.clpaLastRunningTime) >= time.Duration(params.ClpaFreq)*time.Second {
	// 		sg.mutex.Lock()
	// 		mmap, _ := sg.txGraph.CLPA_Partition() //需要移动的账户
	// 		// 迁移账户
	// 		sg.migrationAcc(mmap)

	// 		sg.mutex.Unlock()
	// 		time.Sleep(10 * time.Second)
	// 		sg.clpaLastRunningTime = time.Now()
	// 	}
	// }
}

// 根据重划分结果，对队列中的交易进行迁移
func (sg *SimulatorGraph) migrationAcc(mmap map[string]uint64) {
	fmt.Println("需要迁移的虚拟节点", mmap) //clpa可能会非常多

	txSend := make(map[uint64][]*transaction) //需要迁移的交易
	for shardID := 0; shardID < len(sg.txQueue); shardID++ {
		firstPtr := 0 // 保存无需迁移的交易
		for secondStr := 0; secondStr < len(sg.txQueue[shardID]); secondStr++ {
			ptx := sg.txQueue[shardID][secondStr] //指向需要迁移的交易

			value1, ok1 := mmap[ptx.senderAddr]
			condition1 := ok1 && !ptx.relayed // 非relayed交易，且sender账户需要迁移

			value2, ok2 := mmap[ptx.receiptAddr]
			condition2 := ok2 && ptx.relayed // relayed交易，且receipt账户需要迁移

			if condition1 {
				txSend[value1] = append(txSend[value1], ptx)
			} else if condition2 {
				txSend[value2] = append(txSend[value2], ptx)
			} else { // 无需迁移
				sg.txQueue[shardID][firstPtr] = ptx
				firstPtr++
			}
		}
		sg.txQueue[shardID] = sg.txQueue[shardID][:firstPtr]
	}
	for targetshardID := 0; targetshardID < len(sg.txQueue); targetshardID++ {
		sg.txQueue[targetshardID] = append(sg.txQueue[targetshardID], txSend[uint64(targetshardID)]...)
	}
}

// 统计队列中有哪些账户，账户现在的分片id，队列中与该节点相关的交易数量
func (sg *SimulatorGraph) countRelatedAccounts() (result []AccountCountforG) {
	accounts := make(map[string]int)

	for shardID, transactions := range sg.txQueue {
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
