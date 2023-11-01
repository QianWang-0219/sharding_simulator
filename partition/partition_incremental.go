package partition

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"simple_go/sharding_simulator/params"
	"time"

	"github.com/grd/statistics"
)

// type PartitionIncremental struct {
// 	NetGraph CompressedGraph
// }

func Test_partitionIncremental_Algorithm() {
	/// step 1. 初始化
	// 初始图/划分 k.NetGraph(压缩图）/cg   k.PartitionMap(划分map)
	k := new(PartitionStatic)
	k.Init_PartitionStatic(0.5, 100, params.ShardNum)
	// 构建压缩图
	cg := Test_Graph()
	k.NetGraph = cg

	// 静态图划分算法
	// 初始化分片信息，为每个虚拟节点分配一个分片
	for i := range cg.VertexSet {
		k.fetchMap(i)
	}
	// 标签传播算法
	k.PartitionStatic_Algorithm()

	// 打印静态图划分后的信息
	fmt.Println("划分后的跨分片交易比例：", float64(k.CrossShardEdgeNum)/float64(params.TotalDataSize))
	k.PrintLoad()

	time.Sleep(10 * time.Second)

	/// step 2. 新交易/新节点的加入

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

	//跳过前xx行
	for i := 0; i < params.TotalDataSize; i++ {
		_, err := reader.Read()
		if err == io.EOF {
			log.Panic("文件行数不足")
		}
		if err != nil {
			log.Panic(err)
		}
	}

	// 添加一些新的边给压缩图
	for {
		data, err := reader.Read() // 按行读取文件
		if err == io.EOF || datanum == 25*params.TotalDataSize {
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
		//k.NetGraph.AddVEdge(s, r)
		k.AddVEdge(s, r)
		datanum++
		k.ComputeEdges2Shard()
		if datanum%10000 == 0 {
			fmt.Println(params.TotalDataSize+datanum, "->")
			fmt.Println("虚拟节点个数：", len(k.NetGraph.VertexSet), len(k.NetGraph.EdgeSet))
			fmt.Println("此时跨分片交易比例：", float64(k.CrossShardEdgeNum)/float64(params.TotalDataSize+datanum))
			k.PrintLoad()
		}
	}
}

func (ps *PartitionStatic) PrintLoad() {
	fmt.Println("划分后的各分片的负载分布情况：", ps.Edges2Shard)
	data4var := statistics.Float64{}
	for _, val := range ps.Edges2Shard {
		data4var = append(data4var, float64(val))
	}
	fmt.Println("各分片负载的方差，标准差", statistics.Variance(&data4var), statistics.Sd(&data4var))

}
