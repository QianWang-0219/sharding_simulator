package partition

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"simple_go/sharding_simulator/params"
	"simple_go/sharding_simulator/utils"
	"sync"
	"time"
)

type Simulator struct {
	//txpool        []float64
	epoch         int
	totTxNum      []int
	totCrossTxNum []int
	txQueue       map[int][]*transaction
	ctx           int
	totaltx       int
	// 定义互斥锁
	mutex sync.Mutex
}

type transaction struct {
	from        int
	to          int
	relayed     bool
	senderAddr  string
	receiptAddr string
}

func Newtransaction(ssid, rsid int, senderAddr, receiptAddr string) *transaction {
	return &transaction{
		from:        ssid,
		to:          rsid,
		relayed:     false,
		senderAddr:  senderAddr,
		receiptAddr: receiptAddr,
	}
}

func NewSimulator() *Simulator {
	return &Simulator{
		//txpool:        make([]float64, params.ShardNum),
		epoch:         0,
		totTxNum:      make([]int, 1),
		totCrossTxNum: make([]int, 1),
		txQueue:       make(map[int][]*transaction),
		ctx:           0,
		totaltx:       0,
	}
}

func (sr *Simulator) Test_Partition_Adaptable() {
	// 初始化
	k := new(PartitionStatic)
	k.Init_PartitionStatic(0.5, 100, params.ShardNum)

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

	// 定期处理交易
	go func() {
		ticker := time.Tick(10 * time.Second) // 每隔 xx 秒触发一次输出，模拟出块间隔
		for range ticker {
			//区块容量，每10秒处理2000笔交易
			sr.mutex.Lock()
			for i := 0; i < params.ShardNum; i++ {
				txNum := params.Maxtx
				if len(sr.txQueue[i]) < txNum {
					txNum = len(sr.txQueue[i])
				}
				txs_Packed := sr.txQueue[i][:txNum]   // 待处理交易
				sr.txQueue[i] = sr.txQueue[i][txNum:] //剩余交易

				for _, tx := range txs_Packed {
					if tx.from != tx.to && !tx.relayed { //跨分片交易且还未处理
						sr.ctx++
						tx.relayed = true
						sr.txQueue[tx.to] = append(sr.txQueue[tx.to], tx)
					} else {
						k.AddVEdge(Vertex{Addr: tx.senderAddr}, Vertex{Addr: tx.receiptAddr})
						sr.totaltx++
					}
					// 处理完的交易，构建交易图
				}
				fmt.Printf("此时跨分片交易数量%d,处理完的交易总量%d\n", sr.ctx, sr.totaltx)
			}
			sr.mutex.Unlock()
		}
	}()

	// 监控队列长度/分片负载
	go func() {
		ticker := time.Tick(50 * time.Second)
		for range ticker {
			loadBalance := make([]float64, params.ShardNum)
			for i, lst := range sr.txQueue {
				loadBalance[i] = float64(len(lst))
			}
			covQ := utils.CoefficientOfVariation(loadBalance)
			fmt.Println(loadBalance)
			fmt.Println(covQ)

			// 离散程度较高，需要进行重划分
			if covQ > 0.7 {

			}
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

		vs, vr := utils.CompressAddr(s.Addr), utils.CompressAddr(r.Addr)
		var ssid, rsid int
		if _, ok := k.PartitionMap[vs]; !ok {
			ssid = utils.CompressV2Shard(vs)
		} else {
			ssid = k.PartitionMap[vs]
		}

		if _, ok := k.PartitionMap[vr]; !ok {
			rsid = utils.CompressV2Shard(vr)
		} else {
			rsid = k.PartitionMap[vr]
		}

		// 构建交易
		tx := Newtransaction(ssid, rsid, s.Addr, r.Addr)
		sendtoShard[ssid] = append(sendtoShard[ssid], tx)

		// 模拟发送到txpool,每秒发送2500笔交易
		if datanum != 0 && datanum%params.InjectSpeed == 0 {
			for i, num := range sendtoShard {
				sr.txQueue[i] = append(sr.txQueue[i], num...)
			}
			sendtoShard = make(map[int][]*transaction)
			time.Sleep(time.Second)
		}

		datanum++

		// // 静态图基准
		// if datanum == params.StaticDataSize {
		// 	k.PartitionStatic_Algorithm()
		// 	fmt.Println("此时交易图的跨分片交易数量为：", k.CrossShardEdgeNum)
		// 	fmt.Println("跨分片交易比例为：", float64(k.CrossShardEdgeNum)/float64(params.StaticDataSize))
		// 	fmt.Println("各分片的负载分布情况为：", k.Edges2Shard)
		// }

		// 按照条件触发增量式重划分程序
		// plan A: 按照跨分片交易的比例（需要一个基准，比如先对100万笔交易进行静态划分，划分后的跨分片交易比例作为基准）

		// plan B 对负载均衡情况进行反馈
	}
	select {}
}

func (ps *PartitionStatic) IncrementalRepartiton() {

}
