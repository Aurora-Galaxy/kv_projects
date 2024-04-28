package index

import (
	"bytes"
	"github.com/google/btree"
	"kv_projects/data"
)

// Indexer 抽象索引接口，后续添加其他数据结构，直接实现该接口
type Indexer interface {
	// Put 向索引中添加key对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos

	// Get 获取key对应的数据位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 删除key对应的数据位置信息
	Delete(key []byte) (*data.LogRecordPos, bool)

	// 获取 key 的数量
	Size() int

	// 返回迭代器
	Iterator(reverse bool) Iterator

	// 关闭索引,只是B+树需要使用
	Close() error
}

type IndexType = uint8

// 定义索引类型的枚举，可以提供多种索引类型
const (
	// btree 索引
	Btree IndexType = iota + 1

	// ART 自适应基数数索引
	ART

	// B+树索引
	BPTree
)

/**
 * NewIndexer
 * @Description: 根据不同的类型初始化索引
 * @param tp
 * @return Indexer
 */
func NewIndexer(tp IndexType, dirPath string, syncWrite bool) Indexer {
	switch tp {
	case Btree:
		return NewBtree()
	case ART:
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, syncWrite)
	default:
		panic("unsupported Index type")
	}
	return nil
}

type ItemSelf struct {
	key []byte
	pos *data.LogRecordPos
}

// Less 定义less方法实现btree库中的Item接口
func (ai *ItemSelf) Less(bi btree.Item) bool {
	//ai.key在bi.key之前返回 true
	return bytes.Compare(ai.key, bi.(*ItemSelf).key) == -1
}

// 索引器接口
type Iterator interface {
	/**
	 * Rewind
	 * @Description:重新回到迭代器的起点，即第一个数据
	 */
	Rewind()

	/**
	 * Seek
	 * @Description:根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
	 * @param key
	 */
	Seek(key []byte)

	/**
	 * Next
	 * @Description:跳转到下一个 key
	 */
	Next()

	/**
	 * Valid
	 * @Description:是否有效，即是否已经遍历完了所有的 key，用于退出遍历
	 * @return bool
	 */
	Valid() bool

	/**
	 * Key
	 * @Description:当前遍历位置的 Key 数据
	 * @return []byte
	 */
	Key() []byte

	/**
	 * Value
	 * @Description:当前遍历位置的 Value 数据
	 * @return *data.LogRecordPos
	 */
	Value() *data.LogRecordPos

	/**
	 * Close
	 * @Description:关闭迭代器，释放相应资源
	 */
	Close()
}
