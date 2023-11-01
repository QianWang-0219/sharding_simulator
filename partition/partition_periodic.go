// plan A 定期执行交易图划分算法，利用过去一个纪元内的交易信息构建历史交易图，使用标签传播算法对其进行重划分
// 数据量，过去所有的划分结果map，和过去一个纪元内的交易信息
package partition

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"simple_go/sharding_simulator/params"
	"simple_go/sharding_simulator/utils"

	"time"
)

func Test_Partition_Periodic() {
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
	clpaLastRunningTime := time.Time{}
	epoch := 0
	totTxNum := make([]int, 1)
	totCrossTxNum := make([]int, 1)
	algRunningTime := make([]time.Duration, 0)

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
		if clpaLastRunningTime.IsZero() {
			clpaLastRunningTime = time.Now()
		}
		k.AddVEdge(s, r)
		if datanum != 0 && datanum%2000 == 0 {
			time.Sleep(1 * time.Second)
		}

		// 按照一定时间频率对这段时间内的交易进行图划分
		if !clpaLastRunningTime.IsZero() && time.Since(clpaLastRunningTime) >= time.Duration(params.ClpaFreq)*time.Second {
			// 打印压缩图
			k.PrintTxCompressGraph()

			fmt.Println(totCrossTxNum, totTxNum)
			totTxNum = append(totTxNum, 0)
			totCrossTxNum = append(totCrossTxNum, 0)
			// 执行静态划分算法
			start := time.Now()
			k.PartitionStatic_Algorithm()
			end := time.Now()
			algRunningTime = append(algRunningTime, end.Sub(start))
			fmt.Println(algRunningTime[epoch])

			epoch++
			// 划分状态重置 ?
			//k.PartitionStateReset(mmap)
			clpaLastRunningTime = time.Now()
		}

		// 统计当前跨分片交易信息，各分片负载信息
		if k.PartitionMap[k.NetGraph.VertexMapping[s]] != k.PartitionMap[k.NetGraph.VertexMapping[r]] {
			// 跨分片交易
			totCrossTxNum[epoch]++
			totTxNum[epoch]++
		} else {
			totTxNum[epoch]++
		}
		datanum++
	}
	fmt.Println(totCrossTxNum, totTxNum, algRunningTime)
}

func (ps *PartitionStatic) PartitionStateReset(mmap map[string]uint64) {
	ps = new(PartitionStatic)
	ps.Init_PartitionStatic(0.5, 100, params.ShardNum)
	fmt.Println(ps)
	fmt.Println(ps.NetGraph)
	// 保留历史划分的划分结果map
	for key, val := range mmap {
		ps.PartitionMap[utils.CompressAddr(key)] = int(val)
	}
	fmt.Println("此时映射表的大小", len(ps.PartitionMap))
}

func (ps *PartitionStatic) PrintTxCompressGraph() {
	fmt.Println("虚拟节点个数为", len(ps.NetGraph.VertexSet))
	fmt.Println("节点个数为", len(ps.PartitionMap))

	totalTX := 0.0
	for i := range ps.NetGraph.VertexSet {
		// if ps.NetGraph.VertexSet[i].Load != 0 {
		// 	fmt.Printf("虚拟节点<%d>点内负载为：%d\n", i, ps.NetGraph.VertexSet[i].Load)
		// }
		// fmt.Printf("虚拟节点<%d>的度为：%d\n", i, len(ps.NetGraph.EdgeSet[i]))
		totalTX += float64(ps.NetGraph.VertexSet[i].Load) + float64(len(ps.NetGraph.EdgeSet[i]))/2
	}
	fmt.Printf("总交易数目为：%f\n", totalTX)
}
