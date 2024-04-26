package conf

import (
	"kv_projects/index"
	"os"
)

//数据库启动时接收用户自定义的配置项

type Options struct {
	// 数据库的数据目录
	DirPath string

	// 活跃文件可以写入数据的大小
	DataFileSize int64

	// 设置文件写入阈值，达到该阈值后，进行文件持久化
	BytesPerSync uint64

	// 用于每次写入数据后判断用户是否需要可以进行数据持久化
	SyncWrite bool

	// 索引类型
	IndexType index.IndexType

	// 指定启动时是否使用MMap进行加载
	MMapAtStartUp bool
}

// 用户初始化迭代器时，传入的配置
type IteratorOptions struct {
	// 遍历前缀为指定值的 key，默认为空
	Prefix []byte
	// 是否反向遍历，默认 false 正向遍历
	Reverse bool
}

type WriteBatchOptions struct {
	// 事务操作中一个批次可以存放数据的最大值
	MaxBatchNum uint
	// 提交事务时，是否进行 sync 持久化
	SyncWrites bool
}

var DefaultOptions = Options{
	DirPath:       os.TempDir(),
	DataFileSize:  256 * 1024 * 1024, // 256MB
	BytesPerSync:  0,
	SyncWrite:     false,
	IndexType:     index.Btree,
	MMapAtStartUp: true,
}

// 用户迭代器默认配置
var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
