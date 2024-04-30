package db

import (
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"io"
	"kv_projects/conf"
	"kv_projects/data"
	"kv_projects/errs"
	"kv_projects/fio"
	"kv_projects/index"
	"kv_projects/utils"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	SeqNoKey     = "seq-no"
	FileLockName = "flock"
)

// 定义一个bitcask引擎实例
type DB struct {
	Options    conf.Options              //接收用户的自定义配置
	Mutex      *sync.RWMutex             // 添加读写锁
	FileIds    []int                     // 文件id列表，只用于加载索引，不能在其他地方更新或使用
	ActiveFile *data.DataFile            // 存储活跃文件
	OlderFiles map[uint32]*data.DataFile //存储旧文件,旧文件只可以用来读取数据
	Index      index.Indexer             // 内存索引
	SeqNo      uint64                    // 事务序列号，全局递增
	IsMerging  bool                      //标识merge操作是否正在进行

	// 主要用于索引是B+树的情况
	SeqNoFileExists bool //标识存放全局事务序列号的文件是否存在
	IsInitial       bool //标识是否是初始化数据库实例

	FileLock    *flock.Flock //文件锁保证多个进程之间的互斥
	BytesWrite  uint64       //标识当前所写的字节数
	ReclaimSize int64        // 记录当前数据库中无效的字节数
}

// Stat
// @Description: 存储引擎统计信息
type Stat struct {
	KeyNum      uint   // key的总数量
	DataFileNum uint   // 文件总数
	ReclaimSize int64  // 可以被merge回收的字节大小
	DiskSize    uint64 // 数据目录所占磁盘大小,以字节为单位
}

func Open(options conf.Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial bool

	// 对用户传入的路径进行判断，如果不存在则创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		// os.ModePerm 创建文件夹权限 0777
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//判断当前文件目录是否正在使用
	fileLock := flock.New(filepath.Join(options.DirPath, FileLockName))
	// 尝试获取文件锁，如果 db 实例在该文件夹已经启动，则 fold 为false，否则为true，保证一个文件夹只能启动一个db
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, errs.ErrDatabaseIsUsing
	}

	// 考虑目录存在，但是为空的情况，此时在该目录上初始化db，也需要将IsInitial设为 true
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}

	// 定义一个 db 实例
	db := &DB{
		Options:    options,
		Mutex:      new(sync.RWMutex),
		OlderFiles: make(map[uint32]*data.DataFile),
		Index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrite),
		IsInitial:  isInitial,
		FileLock:   fileLock,
	}

	// 加载 merge 数据目录,将 merge 后的新文件替换原来的旧文件
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 加载用户数据文件
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	if options.IndexType != index.BPTree {
		// 如果发生过 merge 必定会存在hint索引文件，直接从中加载数据即可
		// 从 hint 文件加载索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		// 没有发生 merge 的数据文件，需要遍历文件加载索引
		//构建内存索引
		if err := db.loadIndexFromDataFile(); err != nil {
			return nil, err
		}

		// 在db实例启动完成后，将ioManager重置为普通的Io
		if db.Options.MMapAtStartUp {
			if err := db.resetIoType(); err != nil {
				return nil, err
			}
		}
	}
	// B+ 树的索引保存在磁盘上，不需要加载到内存
	if options.IndexType == index.BPTree {
		if err := db.loadSeqNoFile(); err != nil {
			return nil, err
		}
		// 获取当前活跃文件大小，更新活跃文件offset
		if db.ActiveFile != nil {
			size, err := db.ActiveFile.IOManager.Size()
			if err != nil {
				return nil, err
			}
			db.ActiveFile.WriteOffset = size
		}
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

// 获取所有的key
func (db *DB) ListKeys() [][]byte {
	iter := db.Index.Iterator(false)
	defer iter.Close()
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
	defer iter.Close()
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
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 将数据写入到当前活跃数据文件
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	//更新内存索引
	if oldValue := db.Index.Put(key, pos); oldValue != nil {
		db.ReclaimSize += int64(oldValue.Size)
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
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted}
	// 添加该记录,删除的这条记录也可以看作可删除的数据
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	db.ReclaimSize += int64(pos.Size)
	// 在内存索引中删除 key
	oldValue, ok := db.Index.Delete(key)
	if !ok {
		return errs.ErrIndexUpdateFailed
	}
	if oldValue != nil {
		db.ReclaimSize += int64(oldValue.Size)
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
	defer func() {
		// 将文件锁释放
		if err := db.FileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory, %v", err))
		}
	}()
	if db.ActiveFile == nil {
		return nil
	}
	db.Mutex.Lock()
	defer db.Mutex.Unlock()
	if err := db.Index.Close(); err != nil {
		return err
	}
	// 保存当前的事务序列号， B+树模式下，获取不到最新的事务序列号
	seqNoFile, err := data.OpenSeqNoFile(db.Options.DirPath)
	if err != nil {
		return err
	}
	// 将最新的事务序列号保存在特定文件中，取出时不用遍历所有文件
	seqNoLogRecord := &data.LogRecord{
		Key:   []byte(SeqNoKey),
		Value: []byte(strconv.FormatUint(db.SeqNo, 10)),
	}

	encoderLogRecord, _ := data.EncoderLogRecord(seqNoLogRecord)

	if err := seqNoFile.Write(encoderLogRecord); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
		return err
	}
	if err := seqNoFile.Close(); err != nil {
		return err
	}
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

/**
 * Stat
 * @Description: 返回数据库实例相关的统计信息
 * @receiver db
 * @return *Stat
 */
func (db *DB) Stat() *Stat {
	db.Mutex.RLock()
	defer db.Mutex.RUnlock()

	dataFiles := uint(len(db.OlderFiles))
	if db.ActiveFile != nil {
		dataFiles += 1
	}
	dirSize, err := utils.DirSize(db.Options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size : %v", err))
	}
	return &Stat{
		KeyNum:      uint(db.Index.Size()),
		DataFileNum: dataFiles,
		ReclaimSize: db.ReclaimSize,
		DiskSize:    dirSize,
	}
}

/**
 * BackUp
 * @Description: 备份数据库
 * @receiver db
 */
func (db *DB) BackUp(destDir string) error {
	db.Mutex.RLock()
	defer db.Mutex.RUnlock()
	extends := []string{FileLockName}
	return utils.CopyDir(db.Options.DirPath, destDir, extends)
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

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.Mutex.Lock()
	defer db.Mutex.Unlock()

	return db.appendLogRecord(logRecord)
}

/**
 * appendLogRecord
 * @Description: 添加文件写入记录(写入到活跃文件中)，返回文件索引信息
 * @receiver db
 * @return *data.LogRecordPos
 * @return error
 */
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
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
	// 记录写入的字节数
	db.BytesWrite += uint64(size)
	var needSync = db.Options.SyncWrite
	if !needSync && db.Options.BytesPerSync > 0 && db.BytesWrite >= db.Options.BytesPerSync {
		needSync = true
	}
	//根据用户配置决定是否在写入数据后进行数据持久化
	if needSync {
		if err := db.ActiveFile.Sync(); err != nil {
			return nil, err
		}
		//持久化后，将写入字节数清空
		if db.BytesWrite > 0 {
			db.BytesWrite = 0
		}
	}

	// 构造内存索引信息，即文件存放的文件id和在该文件内的偏移量
	pos := &data.LogRecordPos{
		Fid:    db.ActiveFile.FileId,
		Offset: writeOffset,
		Size:   uint32(size),
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
	dataFile, err := data.OpenDataFile(db.Options.DirPath, initialFileId, fio.StandardIoManager)
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
		ioType := fio.StandardIoManager
		if db.Options.MMapAtStartUp {
			ioType = fio.MMapIoManager
		}
		dataFile, err := data.OpenDataFile(db.Options.DirPath, uint32(fid), ioType)
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

	// 查看是否发生过 merge，如果发生过直接从未 merge 的文件加载索引
	hasMerge, noMergeFileId := false, uint32(0)
	mergeFileName := filepath.Join(db.Options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFileName); err == nil {
		fid, err := db.getNoMergeFileId(db.Options.DirPath)
		if err != nil {
			return err
		}
		noMergeFileId = fid
		hasMerge = true
	}

	updateIndex := func(key []byte, tye data.LogRecordType, logRecordPos *data.LogRecordPos) {
		var oldPos *data.LogRecordPos
		if tye == data.LogRecordDeleted {
			oldPos, _ = db.Index.Delete(key)
			// 当前标记key被删除的信息也是属于无用的信息
			db.ReclaimSize += int64(logRecordPos.Size)
		} else {
			// 没有删除将key添加至内存索引
			oldPos = db.Index.Put(key, logRecordPos)
		}
		if oldPos != nil {
			db.ReclaimSize += int64(oldPos.Size)
		}
	}
	//暂存事务数据
	transactionLogRecord := make(map[uint64][]*data.TransactionLogRecord)

	var currenSeqNo = nonTransactionSeqNo

	// 遍历所有文件id，处理文件记录
	for i, fid := range db.FileIds {
		var fileId = uint32(fid)
		// 如果已经发生 merge，小于noMergeFileId的文件索引已经通过 hint 文件加载
		if hasMerge && fileId < noMergeFileId {
			continue
		}
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
				Size:   uint32(size),
			}
			// 解析 key 拿到事务序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				// 直接更新内存索引
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// Type 为事务完成标志，将暂存数据取出更新内存索引
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionLogRecord[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					// 暂存的事务数据完成相应的操作后，将暂存数据删除
					delete(transactionLogRecord, seqNo)
				} else {
					logRecord.Key = realKey
					transactionLogRecord[seqNo] = append(transactionLogRecord[seqNo], &data.TransactionLogRecord{
						Pos:    logRecordPos,
						Record: logRecord,
					})
				}
			}

			// 更新 offset
			offset += size

			if seqNo > currenSeqNo {
				currenSeqNo = seqNo
			}
		}
		//defer dataFile.Close()
		//当前文件如果是活跃文件的话，需要更新WriteOffset，方便后面打开数据库时，定位文件写入位置
		if i == len(db.FileIds)-1 {
			db.ActiveFile.WriteOffset = offset
		}
	}
	// 更新整个 db 的索引序列号
	db.SeqNo = currenSeqNo
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
	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("invalid merge ratio, must between 0 and 1")
	}
	return nil
}

/**
 * loadSeqNoFile
 * @Description: 加载存放全局事务序列号的文件，拿到全局事务序列号
 * @receiver db
 * @return error
 */
func (db *DB) loadSeqNoFile() error {
	fileName := filepath.Join(db.Options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}
	seqNoFile, err := data.OpenSeqNoFile(db.Options.DirPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = seqNoFile.Close()
	}()
	logRecord, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}
	seqNo, err := strconv.ParseUint(string(logRecord.Value), 10, 64)
	if err != nil {
		return err
	}
	db.SeqNo = seqNo
	db.SeqNoFileExists = true
	return nil
}

/**
 * resetIoType
 * @Description:重置文件的IO类型为普通的类型
 * @receiver db
 * @return error
 */
func (db *DB) resetIoType() error {
	if db.ActiveFile == nil {
		return nil
	}
	if err := db.ActiveFile.SetIOManager(db.Options.DirPath, fio.StandardIoManager); err != nil {
		return err
	}
	for _, oldDataFile := range db.OlderFiles {
		if err := oldDataFile.SetIOManager(db.Options.DirPath, fio.StandardIoManager); err != nil {
			return err
		}
	}
	return nil
}
