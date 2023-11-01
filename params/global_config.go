package params

var (
	StaticDataSize = 100000
	TotalDataSize  = 1000000                         // the total number of txs 2w一组
	CompressFactor = 10000                           // 虚拟节点的大小
	FileInput      = `graphdata/graphDataTOTAL2.csv` //the raw BlockTransaction data path
	ShardNum       = 4
	ClpaFreq       = 10
	InjectSpeed    = 2500
	Maxtx          = 2000
)
