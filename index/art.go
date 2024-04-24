package index

import (
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"kv_projects/data"
	"sort"
	"sync"
)

// AdaptiveRadixTree
// @Description: 自适应基数树索引
type AdaptiveRadixTree struct {
	tree  goart.Tree
	mutex *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree:  goart.New(),
		mutex: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
	art.mutex.Lock()
	art.tree.Insert(key, pos)
	art.mutex.Unlock()
	return true
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.mutex.RLock()
	defer art.mutex.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) bool {
	art.mutex.Lock()
	defer art.mutex.Unlock()
	_, deleted := art.tree.Delete(key)
	return deleted
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.mutex.Lock()
	defer art.mutex.Unlock()
	return newARTIterator(art.tree, reverse)
}
func (art *AdaptiveRadixTree) Size() int {
	art.mutex.RLock()
	size := art.tree.Size()
	art.mutex.RUnlock()
	return size
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

type ARTIterator struct {
	currIndex int         //当前遍历位置的下标
	reverse   bool        // 是否是反向遍历
	values    []*ItemSelf // key和其对应内容的位置信息
}

func newARTIterator(art goart.Tree, reverse bool) *ARTIterator {
	Index := 0
	if reverse {
		Index = art.Size() - 1
	}
	// 将节点取出存放到数组然后实现迭代器方法，会占用大量内存
	values := make([]*ItemSelf, art.Size())

	saveValues := func(node goart.Node) bool {
		item := &ItemSelf{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[Index] = item
		if reverse {
			Index--
		} else {
			Index++
		}
		return true
	}

	art.ForEach(saveValues)

	return &ARTIterator{
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
func (ai *ARTIterator) Rewind() {
	ai.currIndex = 0
}

/**
 * Seek
 * @Description:根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
 * @param key
 */
func (ai *ARTIterator) Seek(key []byte) {
	// btree 本身有序，取出后存放在数组也是有序的
	if ai.reverse {
		// 从大到小排列，找到小于等于key
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) <= 0
		})
	} else {
		// 从小到大排列，找到大于等于key
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) >= 0
		})
	}
}

/**
 * Next
 * @Description:跳转到下一个 key
 */
func (ai *ARTIterator) Next() {
	ai.currIndex += 1
}

/**
 * Valid
 * @Description:是否有效，即是否已经遍历完了所有的 key，用于退出遍历
 * @return bool
 */
func (ai *ARTIterator) Valid() bool {
	return ai.currIndex < len(ai.values)
}

/**
 * Key
 * @Description:当前遍历位置的 Key 数据
 * @return []byte
 */
func (ai *ARTIterator) Key() []byte {
	return ai.values[ai.currIndex].key
}

/**
 * Value
 * @Description:当前遍历位置的 Value 数据
 * @return *data.LogRecordPos
 */
func (ai *ARTIterator) Value() *data.LogRecordPos {
	return ai.values[ai.currIndex].pos
}

/**
 * Close
 * @Description:关闭迭代器，释放相应资源
 */
func (ai *ARTIterator) Close() {
	// 清楚临时数组
	ai.values = nil
}
