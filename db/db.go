package db

import (
	"kv_projects/conf"
	"kv_projects/data"
	"kv_projects/errs"
	"kv_projects/index"
	"sync"
)

// 定义一个bitcask引擎实例
type DB struct {
	options    conf.Options
	mutex      *sync.RWMutex             // 添加读写锁
	activeFile *data.DataFile            // 存储活跃文件
	oldFile    map[uint32]*data.DataFile //存储旧文件,旧文件只可以用来读取数据
	index      index.Indexer             // 内存索引
}

/**
 * Put
 * @Description: 写入key-value数据，key不能为空
 * @receiver db
 * @param key
 * @param value
 * @return error
 */
func (db *DB) Put(key []byte, value []byte) error {
	// 判断key是否有效
	if len(key) == 0 {
		return errs.ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 将数据写入到当前活跃数据文件
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	//更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return errs.ErrIndexUpdateFailed
	}
	return nil

}

/**
 * appendLogRecord
 * @Description: 添加文件写入记录(写入到活跃文件中)，返回文件索引信息
 * @receiver db
 * @return *data.LogRecordPos
 * @return error
 */
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	//判断当前活跃文件，是否存在，数据库在没有写入时没有文件生成
	// 不存在需要初始化一个活跃文件
	if db.activeFile == nil {
		if err := db.setActivateDataFile(); err != nil {
			return nil, err
		}
	}

	// 将数据进行编码
	encRecord, size := data.EncoderLogRecord(logRecord)

	// 如果写入的数据到达活跃文件的阈值，则关闭活跃文件，打开新的活跃文件
	if db.activeFile.WriteOffset+size > db.options.DataFileSize {
		// 先持久化活跃文件数据，即落盘
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 将该文件转换为旧数据文件
		db.oldFile[db.activeFile.FileId] = db.activeFile

		//打开新的数据文件
		if err := db.setActivateDataFile(); err != nil {
			return nil, err
		}
	}
	// 数据写入操作
	writeOffset := db.activeFile.WriteOffset
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	//根据用户配置决定是否在写入数据后进行数据持久化
	if db.options.SyncWrite {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// 构造内存索引信息，即文件存放的文件id和在该文件内的偏移量
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOffset,
	}
	return pos, nil
}

/**
 * setActivateDataFile
 * @Description: 设置当前活跃文件，在使用该方法前必须使用互斥锁
 * @receiver db
 * @return error
 */
func (db *DB) setActivateDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		// 数据文件新建时 id 自增
		initialFileId = db.activeFile.FileId + 1
	}
	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}
