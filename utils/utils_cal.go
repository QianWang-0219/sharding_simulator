package utils

import (
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"simple_go/sharding_simulator/params"
	"strconv"
)

type Address = string

// the default method 根据地址后缀随机取一个shard
func Addr2Shard(addr Address) int { // 传入节点地址
	last16_addr := addr[len(addr)-8:]                  // 截取地址（16进制）的后八位
	num, err := strconv.ParseUint(last16_addr, 16, 64) // string->uint64
	if err != nil {
		fmt.Println("here")
		log.Panic(err)
	}
	return int(num) % params.ShardNum // 按照地址返回一个shardID
}

func CompressV2Shard(shardID int) int {
	return shardID % params.ShardNum
}

func WritePartition2csv(filename string, partition map[string]uint64) {
	// Create a new CSV file
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("Error creating CSV file:", err)
	}
	defer file.Close()

	// Create a CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header row
	writer.Write([]string{"account", "shardID"})

	// Write data rows
	for key, value := range partition {
		writer.Write([]string{key, fmt.Sprintf("%d", value)})
	}

}

// 节点压缩
func CompressAddr(addr Address) int {
	last8_addr := addr[len(addr)-8:]                  // 截取地址（16进制）的后八位
	num, err := strconv.ParseUint(last8_addr, 16, 64) // string->uint64
	if err != nil {
		fmt.Println("here")
		log.Panic(err)
	}

	return int(num) % params.CompressFactor
}

// 寻找[]int中的最大/最小值
func FindMinMax(nums []int) (int, int) {
	min := math.MaxInt64
	max := math.MinInt64

	for _, num := range nums {
		if num < min {
			min = num
		}
		if num > max {
			max = num
		}
	}

	return min, max
}

// 计算离散系数
func CoefficientOfVariation(nums []float64) float64 {
	return std(nums) / mean(nums)
}

func mean(v []float64) float64 {
	var res float64 = 0
	var n int = len(v)
	for i := 0; i < n; i++ {
		res += v[i]
	}
	return res / float64(n)
}

func variance(v []float64) float64 {
	var res float64 = 0
	var m = mean(v)
	var n int = len(v)
	for i := 0; i < n; i++ {
		res += (v[i] - m) * (v[i] - m)
	}
	return res / float64(n-1)
}
func std(v []float64) float64 {
	return math.Sqrt(variance(v))
}
