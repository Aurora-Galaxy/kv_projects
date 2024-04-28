package index

import (
	"go.etcd.io/bbolt"
	"kv_projects/data"
	"path/filepath"
)

const BtreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

// b+ 树索引
type BPlusTree struct {
	tree *bbolt.DB
}

/**
 * NewBPlusTree
 * @Description: 初始化 B+ 索引
 * @param dirPath
 * @return *BPlusTree
 */
func NewBPlusTree(dirPath string, syncWrite bool) *BPlusTree {
	// 按照需求修改相应的配置
	options := bbolt.DefaultOptions
	//b+ 树是否持久化的操作保持和用户传入的配置一致
	options.NoSync = !syncWrite
	bpTree, err := bbolt.Open(filepath.Join(dirPath, BtreeIndexFileName), 0644, options)
	if err != nil {
		panic("failed to open bptree")
	}
	// bbolt 包的操作默认支持事务，每次操作都相当于开启了一个事务
	if err := bpTree.Update(func(tx *bbolt.Tx) error {
		// 执行操作时，需要创建一个 bucket
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}
	return &BPlusTree{tree: bpTree}
}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	var oldValue []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		oldValue = bucket.Get(key)

		// put value 为[]byte, 所以需要编码后存入
		return bucket.Put(key, data.EncoderLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value in bptree")
	}
	if len(oldValue) == 0 {
		return nil
	}
	return data.DecoderLogRecordPos(oldValue)
}

func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	// view 开启一个只读的事务
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecoderLogRecordPos(value)
		}
		return nil
	}); err != nil {
		return nil
		//panic("failed to get value in bptree")
	}
	return pos
}

func (bpt *BPlusTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	var ok bool
	var oldValue []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		oldValue = bucket.Get(key)
		if len(oldValue) != 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value in bptree")
	}
	if len(oldValue) == 0 {
		return nil, false
	}
	return data.DecoderLogRecordPos(oldValue), ok
}

func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		// 获取 B+ 树中 key 的数量
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get size in bptree")
	}
	return size
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBpTreeIterator(bpt.tree, reverse)
}

type bpTreeIterator struct {
	tx *bbolt.Tx
	// 游标，相当于迭代器
	cursor   *bbolt.Cursor
	reverse  bool
	curKey   []byte
	curValue []byte
}

func newBpTreeIterator(tree *bbolt.DB, reverse bool) *bpTreeIterator {
	// writable 是否开启一个可写的事务，可写的事务只能开启一个，可读的可以开启多个
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}

	bpi := &bpTreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	// 初始化可能会导致 key 和 value 为空，会导致 valid 方法返回 false
	// 所以手动调用rewind
	bpi.Rewind()
	return bpi
}

/**
 * Rewind
 * @Description:重新回到迭代器的起点，即第一个数据
 */
func (bpi *bpTreeIterator) Rewind() {
	if bpi.reverse {
		bpi.curKey, bpi.curValue = bpi.cursor.Last()
	} else {
		bpi.curKey, bpi.curValue = bpi.cursor.First()
	}
}

/**
 * Seek
 * @Description:根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
 * @param key
 */
func (bpi *bpTreeIterator) Seek(key []byte) {
	bpi.curKey, bpi.curValue = bpi.cursor.Seek(key)
}

/**
 * Next
 * @Description:跳转到下一个 key
 */
func (bpi *bpTreeIterator) Next() {
	if bpi.reverse {
		bpi.curKey, bpi.curValue = bpi.cursor.Prev()
	} else {
		bpi.curKey, bpi.curValue = bpi.cursor.Next()
	}
}

/**
 * Valid
 * @Description:是否有效，即是否已经遍历完了所有的 key，用于退出遍历
 * @return bool
 */
func (bpi *bpTreeIterator) Valid() bool {
	return len(bpi.curKey) != 0
}

/**
 * Key
 * @Description:当前遍历位置的 Key 数据
 * @return []byte
 */
func (bpi *bpTreeIterator) Key() []byte {
	return bpi.curKey
}

/**
 * Value
 * @Description:当前遍历位置的 Value 数据
 * @return *data.LogRecordPos
 */
func (bpi *bpTreeIterator) Value() *data.LogRecordPos {
	return data.DecoderLogRecordPos(bpi.curValue)
}

/**
 * Close
 * @Description:关闭迭代器，释放相应资源
 */
func (bpi *bpTreeIterator) Close() {
	// 根据官方，只读事务使用RollBack，而不是Commit
	_ = bpi.tx.Rollback()
}
