package index

import (
	"bytes"
	"github.com/google/btree"
	"kv_projects/data"
	"sort"
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

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}
func (bt *BTree) Size() int {
	return bt.tree.Len()
}

type BTreeIterator struct {
	currIndex int         //当前遍历位置的下标
	reverse   bool        // 是否是反向遍历
	values    []*ItemSelf // key和其对应内容的位置信息
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *BTreeIterator {
	var Index int
	// 受限于 btree ，只能将节点取出存放到数组然后实现迭代器方法，会占用大量内存
	values := make([]*ItemSelf, tree.Len())

	// 将所有数据存放在数组中
	saveValues := func(it btree.Item) bool {
		values[Index] = it.(*ItemSelf)
		Index++
		return true
	}
	if reverse {
		// 数据从大到小排列
		tree.Descend(saveValues)
	} else {
		// 数据从小到大排列
		tree.Ascend(saveValues)
	}
	return &BTreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}

}

// BTree 实现索引器接口
/**
 * Rewind
 * @Description:重新回到迭代器的起点，即第一个数据
 */
func (bti *BTreeIterator) Rewind() {
	bti.currIndex = 0
}

/**
 * Seek
 * @Description:根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
 * @param key
 */
func (bti *BTreeIterator) Seek(key []byte) {
	// btree 本身有序，取出后存放在数组也是有序的
	if bti.reverse {
		// 从大到小排列，找到小于等于key
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		// 从小到大排列，找到大于等于key
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

/**
 * Next
 * @Description:跳转到下一个 key
 */
func (bti *BTreeIterator) Next() {
	bti.currIndex += 1
}

/**
 * Valid
 * @Description:是否有效，即是否已经遍历完了所有的 key，用于退出遍历
 * @return bool
 */
func (bti *BTreeIterator) Valid() bool {
	return bti.currIndex < len(bti.values)
}

/**
 * Key
 * @Description:当前遍历位置的 Key 数据
 * @return []byte
 */
func (bti *BTreeIterator) Key() []byte {
	return bti.values[bti.currIndex].key
}

/**
 * Value
 * @Description:当前遍历位置的 Value 数据
 * @return *data.LogRecordPos
 */
func (bti *BTreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currIndex].pos
}

/**
 * Close
 * @Description:关闭迭代器，释放相应资源
 */
func (bti *BTreeIterator) Close() {
	// 清楚临时数组
	bti.values = nil
}
