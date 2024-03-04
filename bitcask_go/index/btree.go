package index

import (
	"bitcask_go/data"
	"github.com/google/btree"
	"sync"
)

// BTree 索引，封装google的btree库
type BTree struct {
	tree *btree.BTree // 并发写不安全，可自己加锁(put,delete需要加锁操作)。并发读安全
	lock *sync.RWMutex
}

/**
 * NewBtree
 * @Description: 初始化 BTree 索引结构
 * @return *BTree
 */
func NewBtree() *BTree {
	return &BTree{
		tree: btree.New(32), //指定btree的叶子节点个数
		lock: new(sync.RWMutex),
	}
}

/**
 * Put
 * @Description: B 树中插入数据
 * @receiver bt
 * @param key
 * @param pos
 * @return bool
 */
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &ItemSelf{
		key: key,
		pos: pos,
	}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

/**
 * Get
 * @Description: 根据key从B树中取出数据
 * @receiver bt
 * @param key
 * @return *data.LogRecord
 */
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &ItemSelf{
		key: key,
	}
	btreeRes := bt.tree.Get(it)
	if btreeRes == nil {
		return nil
	}
	return btreeRes.(*ItemSelf).pos
}

/**
 * Delete
 * @Description:删除
 * @receiver bt
 * @param key
 * @return bool
 */
func (bt *BTree) Delete(key []byte) bool {
	it := &ItemSelf{
		key: key,
	}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	// delete会返回删除键对应的内容，如果为空代表删除失败
	if oldItem == nil {
		return false
	} else {
		return true
	}
}
