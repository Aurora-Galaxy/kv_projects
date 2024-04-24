package db

import (
	"encoding/binary"
	"kv_projects/conf"
	"kv_projects/data"
	"kv_projects/errs"
	"kv_projects/index"
	"sync"
	"sync/atomic"
)

// 标记未开启事务操作的数据
const nonTransactionSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin")

// WriteBatch
// @Description: 批量写数据，保证数据的原子化
type WriteBatch struct {
	options *conf.WriteBatchOptions
	db      *DB
	mu      *sync.Mutex
	// 暂存用户数据
	pendingWrites map[string]*data.LogRecord
}

func (db *DB) NewWriteBatch(opt *conf.WriteBatchOptions) *WriteBatch {
	// 如果索引类型为 B+ 树，并且没有保存最新事务序列号的文件，报警告
	// 正常关闭数据库时，会保存最新的事务序列号
	if db.Options.IndexType == index.BPTree && !db.SeqNoFileExists && !db.IsInitial {
		panic("cannot use write batch, seq no file not exists")
	}
	return &WriteBatch{
		options:       opt,
		db:            db,
		mu:            new(sync.Mutex),
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// 将数据存放在暂存区
func (wt *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return errs.ErrKeyIsEmpty
	}
	wt.mu.Lock()
	defer wt.mu.Unlock()

	// 暂存 LogRecord
	logRecord := &data.LogRecord{Key: key, Value: value}
	wt.pendingWrites[string(key)] = logRecord
	return nil
}

func (wt *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errs.ErrKeyIsEmpty
	}
	wt.mu.Lock()
	defer wt.mu.Unlock()

	// 传入需要删除的 key 本身在数据库中就不存在
	logRecordPos := wt.db.Index.Get(key)
	if logRecordPos == nil {
		if wt.pendingWrites[string(key)] != nil {
			delete(wt.pendingWrites, string(key))
		}
		return nil
	}

	// 暂存 LogRecord,标记 已删除
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	wt.pendingWrites[string(key)] = logRecord
	return nil
}

/**
 * Commit
 * @Description: 提交事务，将暂存的数据写入数据文件，并更新内存索引
 * @receiver wt
 * @return error
 */
func (wt *WriteBatch) Commit() error {
	wt.mu.Lock()
	defer wt.mu.Unlock()

	if len(wt.pendingWrites) == 0 {
		return nil
	}
	if uint(len(wt.pendingWrites)) > wt.options.MaxBatchNum {
		return errs.ErrExceedMaxBatchNum
	}

	// db 加锁 保证事务提交的 串行化
	wt.db.Mutex.Lock()
	defer wt.db.Mutex.Unlock()

	// 获取当前最新的事务序列号
	seqNo := atomic.AddUint64(&wt.db.SeqNo, 1)

	// 写数据到数据文件中
	// 暂存数据，便于后面更新内存索引
	positions := make(map[string]*data.LogRecordPos)
	for _, record := range wt.pendingWrites {
		// 前面已经加锁，此处不用加锁
		logRecordPos, err := wt.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = logRecordPos
	}

	// 添加标识事务完成的标志
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}
	_, err := wt.db.appendLogRecord(finishedRecord)
	if err != nil {
		return nil
	}

	// 根据配置项决定是否持久化
	if wt.options.SyncWrites && wt.db.ActiveFile != nil {
		if err := wt.db.ActiveFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, record := range wt.pendingWrites {
		pos := positions[string(record.Key)]
		if record.Type == data.LogRecordNormal {
			wt.db.Index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			wt.db.Index.Delete(record.Key)
		}
	}

	// 清空暂存的数据
	wt.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

// key 和 事务序列号进行联合编码
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)
	return encKey
}

// 对 key 和序列号解码
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
