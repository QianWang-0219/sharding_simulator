package main

import "simple_go/sharding_simulator/partition"

func main() {
	//======================= 测试图结构=============================//
	//partition.Test_Graph()

	//======================= 测试划分算法===========================//
	//partition.Test_CLPA()

	//======================= 测试压缩图的划分算法====================//
	//partition.Test_Random()
	//partition.Test_PartitionStatic_Algorithm()
	//partition.Test_partitionIncremental_Algorithm()

	//======================= 测试划分算法按照时间周期性更新===========//
	//partition.Test_Partition_Periodic()
	simulator := partition.NewSimulator()
	simulator.Test_Partition_Adaptable()

	//======================= 测试小工具===========================//
	// data := []float64{44052, 87434, 45432, 13428}
	// fmt.Println(utils.CoefficientOfVariation(data))
}
