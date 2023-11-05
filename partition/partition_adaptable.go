package partition

import (
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

type Simulator struct {
	epoch               int
	totTxNum            []int
	totCrossTxNum       []int
	txQueue             map[int][]*transaction
	ctx                 float64
	totaltx             float64
	mutex               sync.Mutex
	txGraph             *PartitionStatic
	clpaLastRunningTime time.Time
}

type transaction struct {
	relayed     bool
	senderAddr  string
	receiptAddr string
	time        time.Time //放进交易池的时间
	reqTime     time.Time //开始共识的时间
	commitTime  time.Time //结束共识的时间
}

type AccountCount struct {
	Account int // 虚拟节点id
	ShardID int // 虚拟节点此时的分片id
	Count   int // 队列中与该节点相关的交易数量
}

func Newtransaction(senderAddr, receiptAddr string) *transaction {
	return &transaction{
		relayed:     false,
		senderAddr:  senderAddr,
		receiptAddr: receiptAddr,
	}
}

func NewSimulator() *Simulator {
	k := new(PartitionStatic)
	k.Init_PartitionStatic(0.5, 100, params.ShardNum)

	return &Simulator{
		epoch:         0,
		totTxNum:      make([]int, 1),
		totCrossTxNum: make([]int, 1),
		txQueue:       make(map[int][]*transaction),
		ctx:           0.0,
		totaltx:       0.0,
		txGraph:       k,
	}
}

func (sr *Simulator) Test_Partition_Adaptable() {
	// 初始化
	// k := new(PartitionStatic)
	// k.Init_PartitionStatic(0.5, 100, params.ShardNum)

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
			sr.mutex.Lock()
			for i := 0; i < params.ShardNum; i++ {
				txNum := params.Maxtx
				if len(sr.txQueue[i]) < txNum {
					txNum = len(sr.txQueue[i])
				}
				txs_Packed := sr.txQueue[i][:txNum]   // 待处理交易
				sr.txQueue[i] = sr.txQueue[i][txNum:] //剩余交易
				for _, tx := range txs_Packed {
					tx.reqTime = time.Now() //开始共识时间
				}
				// Simulate some processing time
				//time.Sleep(time.Duration(params.ConsensusTime) * time.Millisecond)

				for _, tx := range txs_Packed {
					vs := utils.CompressAddr(tx.senderAddr)
					vr := utils.CompressAddr(tx.receiptAddr)
					var ssid, vsid int //sender压缩节点的分片id
					if _, ok := sr.txGraph.PartitionMap[vs]; !ok {
						ssid = utils.CompressV2Shard(vs)
					} else {
						ssid = sr.txGraph.PartitionMap[vs]
					}
					if _, ok := sr.txGraph.PartitionMap[vr]; !ok {
						vsid = utils.CompressV2Shard(vr)
					} else {
						vsid = sr.txGraph.PartitionMap[vr]
					}

					if ssid != vsid && !tx.relayed { //跨分片交易且还未处理
						sr.ctx += 0.5
						sr.totaltx += 0.5
						tx.relayed = true
						sr.txQueue[vsid] = append(sr.txQueue[vsid], tx)
					} else {
						sr.txGraph.AddVEdge(Vertex{Addr: tx.senderAddr}, Vertex{Addr: tx.receiptAddr})
						// 该交易的用户感知时延
						tx.commitTime = time.Now()
						latency := tx.commitTime.Sub(tx.time)
						writeRes2CSV("tx detail information", []string{tx.senderAddr, tx.receiptAddr, strconv.FormatBool(tx.relayed),
							tx.time.Format("2006-01-02 15:04:05.000"), tx.reqTime.Format("2006-01-02 15:04:05.000"), tx.commitTime.Format("2006-01-02 15:04:05.000"),
							latency.String()})
						if tx.relayed {
							sr.ctx += 0.5
							sr.totaltx += 0.5
						} else {
							sr.totaltx++
						}
					}
					// 处理完的交易，构建交易图
				}
			}
			fmt.Printf("第%d次共识：此时跨分片交易数量%f,处理完的交易总量%f\n", consensusCounter, sr.ctx, sr.totaltx)
			sr.mutex.Unlock()
		}
	}()

	// 监控队列长度/分片负载
	go func() {
		ticker := time.Tick(50 * time.Second)
		for range ticker {
			loadBalance := make([]float64, params.ShardNum)
			stringSlice := make([]string, len(loadBalance))
			sr.mutex.Lock()
			for i, lst := range sr.txQueue {
				loadBalance[i] = float64(len(lst))
				stringSlice[i] = strconv.FormatFloat(loadBalance[i], 'f', 3, 64)
			}
			sr.mutex.Unlock()
			covQ := utils.CoefficientOfVariation(loadBalance)
			stdQ := utils.Std(loadBalance)

			fmt.Println("此时各分片队列分布", loadBalance)
			fmt.Println("离散系数为:", covQ, "标准差为:", stdQ)

			writeTXpool2CSV("compress+incre txPool", stringSlice)

			// // 离散程度较高，需要进行重划分
			// if stdQ > 3e4 || covQ >1{
			// 	fmt.Println("start re-partitioning...")
			// 	moveAcc := make(map[int]int) // 虚拟节点id-新的分片id
			// 	sr.mutex.Lock()
			// // 重新获取当前的loadbalance
			// for i, lst := range sr.txQueue {
			// 	loadBalance[i] = float64(len(lst))
			// 	stringSlice[i] = strconv.FormatFloat(loadBalance[i], 'f', 3, 64)
			// }

			// step 1. 遍历当前的交易队列，统计参与排队交易的节点，以及每个节点参与的交易数目
				queueAccCount := sr.countRelatedAccounts()

			// step 2. 根据历史交易模式，生成优先队列，跨片交易占总度更大的优先级更高
				for _, queueAcc := range queueAccCount {
					sr.txGraph.push2priorityQueue(queueAcc.Account, queueAcc.Count)
				}

			// step 3. 按照虚拟节点跨片交易的比例，依次弹出优先队列中的节点
			// 	for sr.txGraph.pq.Len() > 0 {
			// 		item := heap.Pop(&sr.txGraph.pq).(*NodePriorityItem)
			// 		//fmt.Printf("Node: %d, CrossShardTxRatio: %.2f, CrossShardTxNeighbors: %d \n", item.Node, item.CrossShardTxRatio, item.CrossShardTxNeighbors)

			// 		// 先考虑负载均衡，然后再考虑跨分片交易比例
			// 		// 按照弹出的顺序，把节点迁移到负载最小的分片，计算移动后的离散系数，跨分片交易最小增益

			// 		// b.跨分片交易比例变化，希望跨分片增益为正 minimum swapping gain SG

			// 		// a.离散系数变化，节点的原分片-节点负载，目标分片+节点负载，把节点迁移到负载最小的分片肯定是对负载均衡产生增益的
			// 		newstdQ, newloadBalance, newShardID := sr.txGraph.changeShardCOVRecompute(item, loadBalance)
			// 		fmt.Println(newstdQ, newloadBalance, newShardID, stdQ)
			// 		if newstdQ < stdQ { //负载均衡有增益
			// 			stdQ = newstdQ
			// 			loadBalance = newloadBalance
			// 			// 更改分配
			// 			sr.txGraph.PartitionMap[item.Node] = newShardID
			// 			moveAcc[item.Node] = newShardID //保留重新划分的虚拟节点

			// 			// 计算当前划分下的跨分片交易数目
			// 			sr.txGraph.ComputeEdges2Shard()
			// 			fmt.Println("外部？此时历史交易图中的跨分片比例:", float64(sr.txGraph.CrossShardEdgeNum)/float64(sr.totaltx))

			// 			if stdQ <= 3e4 {
			// 				// 队列中交易迁移
			// 				sr.migrationAcc(moveAcc)

			// 				//////////
			// 				loadBalance := make([]float64, params.ShardNum)
			// 				for i, lst := range sr.txQueue {
			// 					loadBalance[i] = float64(len(lst))
			// 				}

			// 				fmt.Println("重划分完成...", loadBalance, covQ)
			// 				break
			// 			}
			// 		}
			// 	}
			// 	sr.mutex.Unlock()
			// }
		}
	}()

	// 添加一些新的边给压缩图
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

		vs := utils.CompressAddr(s.Addr) //sender的压缩节点id

		var ssid int //sender压缩节点的分片id
		sr.mutex.Lock()
		if _, ok := sr.txGraph.PartitionMap[vs]; !ok {
			ssid = utils.CompressV2Shard(vs)
		} else {
			ssid = sr.txGraph.PartitionMap[vs]
		}
		sr.mutex.Unlock()

		// 构建交易
		tx := Newtransaction(s.Addr, r.Addr)
		sendtoShard[ssid] = append(sendtoShard[ssid], tx)

		// 模拟发送到txpool,每秒发送2500笔交易
		if datanum != 0 && datanum%params.InjectSpeed == 0 {
			// set the algorithm timer begins
			if sr.clpaLastRunningTime.IsZero() {
				sr.clpaLastRunningTime = time.Now()
			}
			for i, txs := range sendtoShard {
				for _, tx := range txs {
					tx.time = time.Now()
				}
				sr.txQueue[i] = append(sr.txQueue[i], txs...)
			}
			sendtoShard = make(map[int][]*transaction)
			time.Sleep(time.Second)
		}

		datanum++

		// // 静态图基准
		// if datanum == params.StaticDataSize {
		// 	sr.mutex.Lock()
		// 	mmap := sr.txGraph.PartitionStatic_Algorithm()
		// 	fmt.Println("此时交易图的跨分片交易数量为：", sr.txGraph.CrossShardEdgeNum)
		// 	fmt.Println("跨分片交易比例为：", float64(sr.txGraph.CrossShardEdgeNum)/float64(params.StaticDataSize))
		// 	fmt.Println("各分片的负载分布情况为：", sr.txGraph.Edges2Shard)
		// 	// 迁移账户
		// 	sr.migrationAcc(mmap)
		// 	sr.mutex.Unlock()
		// }

		// 周期性执行全局划分算法
		// 定期clpa重划分
		if !sr.clpaLastRunningTime.IsZero() && time.Since(sr.clpaLastRunningTime) >= time.Duration(params.ClpaFreq)*time.Second {
			sr.mutex.Lock()
			mmap := sr.txGraph.PartitionStatic_Algorithm()
			// 迁移账户
			sr.migrationAcc(mmap)
			sr.mutex.Unlock()
			time.Sleep(10 * time.Second)
			sr.clpaLastRunningTime = time.Now()
		}

		// 按照条件触发增量式重划分程序
		// plan A: 按照跨分片交易的比例（需要一个基准，比如先对100万笔交易进行静态划分，划分后的跨分片交易比例作为基准）

		// plan B 对负载均衡情况进行反馈
	}
	// 交易发送完之后依旧要定期重划分
	for {
		time.Sleep(time.Second)
		if time.Since(sr.clpaLastRunningTime) >= time.Duration(params.ClpaFreq)*time.Second {
			sr.mutex.Lock()
			mmap := sr.txGraph.PartitionStatic_Algorithm()
			// 迁移账户
			sr.migrationAcc(mmap)
			sr.mutex.Unlock()
			time.Sleep(10 * time.Second)
			sr.clpaLastRunningTime = time.Now()
		}
	}
	//select {}
}

func (sr *Simulator) countRelatedAccounts() (result []AccountCount) {
	accounts := make(map[int]int)

	for shardID, transactions := range sr.txQueue {

		for _, tx := range transactions {
			if tx.relayed {
				accounts[utils.CompressAddr(tx.receiptAddr)]++
			} else {
				accounts[utils.CompressAddr(tx.senderAddr)]++
			}
		}

		for acc, count := range accounts {
			result = append(result, AccountCount{
				Account: acc,
				ShardID: shardID,
				Count:   count,
			})
		}
		accounts = make(map[int]int)
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
func (sr *Simulator) migrationAcc(moveAcc map[int]int) {
	fmt.Println("需要迁移的虚拟节点", moveAcc)

	//////
	loadBalance := make([]float64, params.ShardNum)
	for i, lst := range sr.txQueue {
		loadBalance[i] = float64(len(lst))
	}
	covQ := utils.CoefficientOfVariation(loadBalance)

	fmt.Println("队列迁移开始...", loadBalance, covQ)

	txSend := make(map[int][]*transaction) //需要迁移的交易
	for shardID := 0; shardID < len(sr.txQueue); shardID++ {
		firstPtr := 0 // 保存无需迁移的交易
		for secondStr := 0; secondStr < len(sr.txQueue[shardID]); secondStr++ {
			ptx := sr.txQueue[shardID][secondStr] //指向需要迁移的交易

			value1, ok1 := moveAcc[utils.CompressAddr(ptx.senderAddr)]
			condition1 := ok1 && !ptx.relayed // 非relayed交易，且sender账户需要迁移

			value2, ok2 := moveAcc[utils.CompressAddr(ptx.receiptAddr)]
			condition2 := ok2 && ptx.relayed // relayed交易，且receipt账户需要迁移

			if condition1 {
				txSend[value1] = append(txSend[value1], ptx)
			} else if condition2 {
				txSend[value2] = append(txSend[value2], ptx)
			} else { // 无需迁移
				sr.txQueue[shardID][firstPtr] = ptx
				firstPtr++
			}
		}
		sr.txQueue[shardID] = sr.txQueue[shardID][:firstPtr]
	}
	for targetshardID := 0; targetshardID < len(sr.txQueue); targetshardID++ {
		sr.txQueue[targetshardID] = append(sr.txQueue[targetshardID], txSend[targetshardID]...)
	}
}

// 把实验结果写进csv文件中
func writeRes2CSV(filename string, str []string) {
	dirpath := "./result"
	err := os.MkdirAll(dirpath, os.ModePerm)
	if err != nil {
		log.Panic(err)
	}

	targetPath := dirpath + "/" + filename + ".csv"
	f, err := os.Open(targetPath)

	if err != nil && os.IsNotExist(err) {
		file, er := os.Create(targetPath)
		if er != nil {
			panic(er)
		}
		defer file.Close()

		w := csv.NewWriter(file)
		title := []string{"from", "to", "relayed", "startTime", "reqTime", "commitTime", "latency"}
		w.Write(title)
		w.Flush()
		w.Write(str)
		w.Flush()
	} else {
		file, err := os.OpenFile(targetPath, os.O_APPEND|os.O_RDWR, 0666)

		if err != nil {
			log.Panic(err)
		}
		defer file.Close()
		writer := csv.NewWriter(file)
		err = writer.Write(str)
		if err != nil {
			log.Panic()
		}
		writer.Flush()
	}
	f.Close()
}

// 把负载分布写进csv文件中
func writeTXpool2CSV(filename string, str []string) {
	dirpath := "./result"
	err := os.MkdirAll(dirpath, os.ModePerm)
	if err != nil {
		log.Panic(err)
	}

	targetPath := dirpath + "/" + filename + ".csv"
	f, err := os.Open(targetPath)

	if err != nil && os.IsNotExist(err) {
		file, er := os.Create(targetPath)
		if er != nil {
			panic(er)
		}
		defer file.Close()

		w := csv.NewWriter(file)
		title := []string{"shard 0", "shard 1", "shard 2", "shard 3"}
		w.Write(title)
		w.Flush()
		w.Write(str)
		w.Flush()
	} else {
		file, err := os.OpenFile(targetPath, os.O_APPEND|os.O_RDWR, 0666)

		if err != nil {
			log.Panic(err)
		}
		defer file.Close()
		writer := csv.NewWriter(file)
		err = writer.Write(str)
		if err != nil {
			log.Panic()
		}
		writer.Flush()
	}
	f.Close()
}
