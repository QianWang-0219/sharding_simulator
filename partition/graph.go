// 图的相关操作
package partition

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"simple_go/sharding_simulator/params"
)

// 图中的结点，即区块链网络中参与交易的账户
type Vertex struct {
	Addr string // 账户地址
	// 其他属性待补充
}

// 描述当前区块链交易集合的图
type Graph struct {
	VertexSet map[Vertex]bool     // 节点集合，其实是 set
	EdgeSet   map[Vertex][]Vertex // 记录节点与节点间是否存在交易，邻接表
}

// 增加图中的点
func (g *Graph) AddVertex(v Vertex) {
	if g.VertexSet == nil {
		g.VertexSet = make(map[Vertex]bool)
	}
	g.VertexSet[v] = true
}

// 增加图中的边
func (g *Graph) AddEdge(u, v Vertex) {
	// 如果没有点，则先加点
	if _, ok := g.VertexSet[u]; !ok {
		g.AddVertex(u)
	}
	if _, ok := g.VertexSet[v]; !ok {
		g.AddVertex(v)
	}
	if g.EdgeSet == nil {
		g.EdgeSet = make(map[Vertex][]Vertex)
	}
	// 添加边
	// 无向图，使用双向边（其实是有权的，因为允许重复添加有交易的点）
	g.EdgeSet[u] = append(g.EdgeSet[u], v)
	g.EdgeSet[v] = append(g.EdgeSet[v], u)
}

// 输出图
func (g Graph) PrintGraph() {
	for v := range g.VertexSet {
		print(v.Addr, " ")
		print("edge:")
		for _, u := range g.EdgeSet[v] {
			print(" ", u.Addr, "\t")
		}
		println()
	}
	println()
}

func (g Graph) CountGraph() {
	fmt.Println("vertex count:", len(g.VertexSet))
	txNum := 0
	for _, lst := range g.EdgeSet {
		txNum += len(lst)
	}
	fmt.Println("edge count:", txNum/2)
}

func Test_Graph() CompressedGraph {
	// 初始化图
	graph := new(Graph)

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
		graph.AddEdge(s, r) // 加上s->r的边
		datanum++
	}
	datanum = 0
	//graph.PrintGraph()
	graph.CountGraph()

	CG := CompressGraph(*graph)
	//fmt.Println(CG)

	// // 添加一些新的边给压缩图
	// for {
	// 	data, err := reader.Read() // 按行读取文件
	// 	if err == io.EOF || datanum == 10 {
	// 		break
	// 	}
	// 	if err != nil {
	// 		log.Panic(err)
	// 	}
	// 	s := Vertex{
	// 		Addr: data[0], // from地址去掉前两位
	// 	}
	// 	r := Vertex{
	// 		Addr: data[1], // to地址去掉前两位
	// 	}
	// 	CG.AddVEdge(s, r)
	// 	datanum++
	// }

	totalTX := 0.0
	for i := range CG.VertexSet {
		// if CG.VertexSet[i].Load != 0 {
		// 	fmt.Printf("虚拟节点<%d>点内负载为：%d\n", i, CG.VertexSet[i].Load)
		// }
		// fmt.Printf("虚拟节点<%d>的度为：%d\n", i, len(CG.EdgeSet[i]))
		totalTX += float64(CG.VertexSet[i].Load) + float64(len(CG.EdgeSet[i]))/2
	}
	fmt.Println("虚拟节点个数：", len(CG.VertexSet))
	fmt.Printf("总交易数目为：%f\n", totalTX)

	return CG
}
