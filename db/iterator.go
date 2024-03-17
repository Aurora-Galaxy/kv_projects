package db

import (
	"bytes"
	"kv_projects/conf"
	"kv_projects/index"
)

// 提供给用户调用的 iterator 方法

type Iterator struct {
	IndexIter index.Iterator       // 索引迭代器
	Db        *DB                  // 需要根据 pos 取出数据，所以要包含 DB
	Options   conf.IteratorOptions //迭代器配置项
}

// 实现迭代器接口
/**
 * Rewind
 * @Description:重新回到迭代器的起点，即第一个数据
 */
func (it *Iterator) Rewind() {
	it.IndexIter.Rewind()
	it.skipToNext()
}

/**
 * Seek
 * @Description:根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
 * @param key
 */
func (it *Iterator) Seek(key []byte) {
	it.IndexIter.Seek(key)
	it.skipToNext()
}

/**
 * Next
 * @Description:跳转到下一个 key
 */
func (it *Iterator) Next() {
	it.IndexIter.Next()
	it.skipToNext()
}

/**
 * Valid
 * @Description:是否有效，即是否已经遍历完了所有的 key，用于退出遍历
 * @return bool
 */
func (it *Iterator) Valid() bool {
	return it.IndexIter.Valid()
}

/**
 * Key
 * @Description:当前遍历位置的 Key 数据
 * @return []byte
 */
func (it *Iterator) Key() []byte {
	return it.IndexIter.Key()
}

/**
 * Value
 * @Description:当前遍历位置的 Value 数据
 * @return []byte,当前 key 对应的 value
 */
func (it *Iterator) Value() ([]byte, error) {
	it.Db.Mutex.RLock()
	defer it.Db.Mutex.RUnlock()
	valueByPosition, err := it.Db.GetValueByPosition(it.IndexIter.Value())
	if err != nil {
		return nil, err
	}
	return valueByPosition, nil
}

/**
 * Close
 * @Description:关闭迭代器，释放相应资源
 */
func (it *Iterator) Close() {
	it.IndexIter.Close()
}

// 用户可能会配置从指定前缀的 key 开始遍历
func (it *Iterator) skipToNext() {
	prefixLen := len(it.Options.Prefix)
	if prefixLen == 0 {
		return
	}
	for ; it.IndexIter.Valid(); it.IndexIter.Next() {
		key := it.IndexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(it.Options.Prefix, key[:prefixLen]) == 0 {
			break // 到达指定位置，直接跳出循环，当前下标就是满足条件的下标
		}
	}
}
