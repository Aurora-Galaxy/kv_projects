package index

import (
	"bytes"
	"github.com/google/btree"
	"kv_projects/data"
)

// Indexer 抽象索引接口，后续添加其他数据结构，直接实现该接口
type Indexer interface {
	// Put 向索引中添加key对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 获取key对应的数据位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 删除key对应的数据位置信息
	Delete(key []byte) bool
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
