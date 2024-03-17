package db

import (
	"errors"
	"io"
	"kv_projects/conf"
	"kv_projects/data"
	"kv_projects/errs"
	"kv_projects/index"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// 定义一个bitcask引擎实例
type DB struct {
	Options    conf.Options              //接收用户的自定义配置
	Mutex      *sync.RWMutex             // 添加读写锁
	FileIds    []int                     // 文件id列表，只用于加载索引，不能在其他地方更新或使用
	ActiveFile *data.DataFile            // 存储活跃文件
	OlderFiles map[uint32]*data.DataFile //存储旧文件,旧文件只可以用来读取数据
	Index      index.Indexer             // 内存索引
}

func Open(options conf.Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	// 对用户传入的路径进行判断，如果不存在则创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		// os.ModePerm 创建文件夹权限 0777
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 定义一个 db 实例
	db := &DB{
		Options:    options,
		Mutex:      new(sync.RWMutex),
		OlderFiles: make(map[uint32]*data.DataFile),
		Index:      index.NewIndexer(options.IndexType),
	}

	// 加载用户配置文件
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	//构建内存索引
	if err := db.loadIndexFromDataFile(); err != nil {
		return nil, err
	}
	return db, nil
}

// 初始化用户迭代器
func (db *DB) NewUserIterator(options conf.IteratorOptions) *Iterator {
	IndexIter := db.Index.Iterator(options.Reverse)
	return &Iterator{
		IndexIter: IndexIter,
		Db:        db,
		Options:   options,
	}
}

// 获取所用的key
func (db *DB) ListKeys() [][]byte {
	iter := db.Index.Iterator(false)
	keys := make([][]byte, db.Index.Size())
	var idx int
	for iter.Rewind(); iter.Valid(); iter.Next() {
		keys[idx] = iter.Key()
		idx++
	}
	return keys
}

// 获取所有数据，然后执行用户指定操作
func (db *DB) Fold(fn func(key, value []byte) bool) error {
	db.Mutex.RLock()
	defer db.Mutex.RUnlock()
	iter := db.Index.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		// 读取数据
		value, err := db.GetValueByPosition(iter.Value())
		if err != nil {
			return err
		}
		if !fn(iter.Key(), value) {
			break
		}
	}
	return nil
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
	if ok := db.Index.Put(key, pos); !ok {
		return errs.ErrIndexUpdateFailed
	}
	return nil

}

/**
 * Get
 * @Description: 取出 key 对应的 value
 * @receiver db
 * @param key
 * @return []byte
 * @return error
 */
func (db *DB) Get(key []byte) ([]byte, error) {
	db.Mutex.Lock()
	defer db.Mutex.Unlock()
	//判断key是否为空
	if len(key) == 0 {
		return nil, errs.ErrKeyIsEmpty
	}
	// 从内存中，取出相应的key对应的索引信息
	logRecordPos := db.Index.Get(key)
	// 没有取到对应数据，说明key不存在
	if logRecordPos == nil {
		return nil, errs.ErrKeyNotFound
	}

	// 获取当前 logRecorfPos 对应的文件内容
	return db.GetValueByPosition(logRecordPos)
}

func (db *DB) Delete(key []byte) error {
	// 无效的 key
	if len(key) == 0 {
		return errs.ErrKeyIsEmpty
	}

	// 先判断 key 是否存在，如果不存在直接返回
	if pos := db.Index.Get(key); pos == nil {
		return errs.ErrKeyNotFound
	}

	// 构建一个 logrecord,标记该 key 的内容已被删除
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	// 添加该记录
	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}
	// 在内存索引中删除 key
	ok := db.Index.Delete(key)
	if !ok {
		return errs.ErrIndexUpdateFailed
	}
	return nil
}

/**
 * Close
 * @Description: 关闭数据库文件
 * @receiver db
 * @return error
 */
func (db *DB) Close() error {
	if db.ActiveFile == nil {
		return nil
	}
	db.Mutex.Lock()
	defer db.Mutex.Unlock()

	// 关闭当前活跃文件
	if err := db.ActiveFile.Close(); err != nil {
		return nil
	}

	// 关闭旧的活跃文件
	for _, oldFile := range db.OlderFiles {
		if err := oldFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

/**
 * Sync
 * @Description: 数据库文件落盘操作
 * @receiver db
 * @return error
 */
func (db *DB) Sync() error {
	if db.ActiveFile == nil {
		return nil
	}
	db.Mutex.Lock()
	defer db.Mutex.Unlock()
	return db.ActiveFile.Sync()
}

func (db *DB) GetValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
	// 根据文件id 找到对应数据的位置
	var dataFile *data.DataFile
	// 文件 id 为当前活跃文件
	if db.ActiveFile.FileId == logRecordPos.Fid {
		dataFile = db.ActiveFile
	} else {
		// 不是活跃文件，根据文件id 在旧文件中找
		dataFile = db.OlderFiles[logRecordPos.Fid]
	}
	if dataFile == nil {
		return nil, errs.ErrDataFileNotFound
	}
	//找到文件位置后，根据偏移量将内容读取出来
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	//对LogRecord(根据墓碑值)进行判断，是否被删除
	if logRecord.Type == data.LogRecordDeleted {
		return nil, errs.ErrDataAlreadyDeleted
	}
	return logRecord.Value, nil
}

/**
 * appendLogRecord
 * @Description: 添加文件写入记录(写入到活跃文件中)，返回文件索引信息
 * @receiver db
 * @return *data.LogRecordPos
 * @return error
 */
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.Mutex.Lock()
	defer db.Mutex.Unlock()

	//判断当前活跃文件，是否存在，数据库在没有写入时没有文件生成
	// 不存在需要初始化一个活跃文件
	if db.ActiveFile == nil {
		if err := db.setActivateDataFile(); err != nil {
			return nil, err
		}
	}

	// 将数据进行编码
	encRecord, size := data.EncoderLogRecord(logRecord)

	// 如果写入的数据到达活跃文件的阈值，则关闭活跃文件，打开新的活跃文件
	if db.ActiveFile.WriteOffset+size > db.Options.DataFileSize {
		// 先持久化活跃文件数据，即落盘
		if err := db.ActiveFile.Sync(); err != nil {
			return nil, err
		}

		// 将该文件转换为旧数据文件
		db.OlderFiles[db.ActiveFile.FileId] = db.ActiveFile

		//切记 落盘后将该文件的文件句柄关闭
		//if err := db.OlderFiles[db.ActiveFile.FileId].Close(); err != nil {
		//	return nil, err
		//}

		//打开新的数据文件
		if err := db.setActivateDataFile(); err != nil {
			return nil, err
		}
	}
	// 数据写入操作
	writeOffset := db.ActiveFile.WriteOffset
	if err := db.ActiveFile.Write(encRecord); err != nil {
		return nil, err
	}

	//根据用户配置决定是否在写入数据后进行数据持久化
	if db.Options.SyncWrite {
		if err := db.ActiveFile.Sync(); err != nil {
			return nil, err
		}
	}

	// 构造内存索引信息，即文件存放的文件id和在该文件内的偏移量
	pos := &data.LogRecordPos{
		Fid:    db.ActiveFile.FileId,
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
	if db.ActiveFile != nil {
		// 数据文件新建时 id 自增
		initialFileId = db.ActiveFile.FileId + 1
	}
	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.Options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.ActiveFile = dataFile
	return nil
}

func (db *DB) loadDataFile() error {
	// 获取目录下的所有文件
	dirEntries, err := os.ReadDir(db.Options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int

	// 遍历所有文件，找到以 .data 为结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			// 存储数据的文件名被命名为 00001.data，前面为文件id
			splitName := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitName[0])
			if err != nil {
				return errs.ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// 对 fileIds 进行排序，从小到大一次加载
	sort.Ints(fileIds)
	db.FileIds = fileIds
	// 遍历每个文件id,打开该文件
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.Options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		// 最后一个文件为最新文件，即活跃文件
		if i == len(fileIds)-1 {
			db.ActiveFile = dataFile
		} else {
			db.OlderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

/**
 * loadIndexFromDataFile
 * @Description: 从数据文件中加载索引，遍历所有记录并将其加载到内存索引
 * @receiver db
 */
func (db *DB) loadIndexFromDataFile() error {
	// 没有文件，数据库为空
	if len(db.FileIds) == 0 {
		return nil
	}
	// 遍历所有文件id，处理文件记录
	for i, fid := range db.FileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		// 先得到对应的datafile对象，然后读取内容构建索引
		// 判断是活跃文件还是旧文件
		if fileId == db.ActiveFile.FileId {
			dataFile = db.ActiveFile
		} else {
			dataFile = db.OlderFiles[fileId]
		}

		// 读取内容
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				// 读取到文件结尾
				if err == io.EOF {
					break
				}
				return err
			}
			// 构造内存索引并保存
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}
			// 判断记录是否被删除
			if logRecord.Type == data.LogRecordDeleted {
				db.Index.Delete(logRecord.Key)
			} else {
				// 没有删除将key添加至内存索引
				db.Index.Put(logRecord.Key, logRecordPos)
			}
			// 更新 offset
			offset += size
		}
		//defer dataFile.Close()
		//当前文件如果是活跃文件的话，需要更新WriteOffset，方便后面打开数据库时，定位文件写入位置
		if i == len(db.FileIds)-1 {
			db.ActiveFile.WriteOffset = offset
		}
	}
	return nil
}

// 对用户的配置项进行校验
func checkOptions(options conf.Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	return nil
}
