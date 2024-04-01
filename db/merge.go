package db

import (
	"io"
	"kv_projects/data"
	"kv_projects/errs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	MergeDirName     = "-merge"
	MergeFinishedKey = "merge-finished"
)

func (db *DB) Merge() error {
	// 如果数据库为空，直接返回
	if db.ActiveFile == nil {
		return nil
	}
	db.Mutex.Lock()
	// 保证只有一个 merge 在进行，如果已经在merge则返回相应的错误
	if db.IsMerging {
		db.Mutex.Unlock()
		return errs.ErrMergeIsProgress
	}
	db.IsMerging = true
	defer func() {
		db.IsMerging = false
	}()
	// merge 基本流程
	/*
		1. 打开新的活跃文件
		2. 对之前的全部文件执行merge操作
	*/
	// 持久化当前活跃文件
	err := db.ActiveFile.Sync()
	if err != nil {
		db.Mutex.Unlock()
		return err
	}
	// 将当前活跃文件转为旧文件
	db.OlderFiles[db.ActiveFile.FileId] = db.ActiveFile

	// 打开新的活跃文件
	err = db.setActivateDataFile()
	if err != nil {
		db.Mutex.Unlock()
		return err
	}
	// 记录当前没有被 merge 的文件id
	noMergeFileId := db.ActiveFile.FileId

	// 取出所有需要 merge 的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.OlderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.Mutex.Unlock()

	// 待 merge 的文件从小到大排序依次 merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})
	mergePath := db.getMergePath()
	// 如果 mergePath 存在说明发生过merge，将原merge目录删除
	_, err = os.Stat(mergePath)
	if err == nil {
		if err = os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	// 新建一个 merge 目录
	err = os.MkdirAll(mergePath, os.ModePerm)
	if err != nil {
		return err
	}

	// 打开一个新的 bitcask 实例去执行merge操作
	mergeOptions := db.Options
	mergeOptions.DirPath = mergePath

	// 落盘设为false，手动控制落盘，防止merge过程出错
	mergeOptions.SyncWrite = false
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	// 打开 hint 文件存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	// 遍历处理每一个文件
	for _, file := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := file.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			//解析拿到实际的 key（不带事务序列号）
			realKey, _ := parseLogRecordKey(logRecord.Key)
			// 拿到key对应的内存索引信息
			logRecordPos := db.Index.Get(realKey)
			//判断数据是否需要重写
			if logRecordPos != nil && logRecordPos.Fid == file.FileId && logRecordPos.Offset == file.WriteOffset {
				//	merge时确定该数据有效，不在需要加入事务序列号
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				newLogRecordPos, err := mergeDB.appendLogRecordWithLock(logRecord)
				if err != nil {
					return err
				}
				// 将新的索引位置信息添加进 Hint(索引)文件中
				if err := hintFile.WriteHintFile(realKey, newLogRecordPos); err != nil {
					return err
				}
			}
			offset += size
		}
	}

	// 文件持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// 增加一个标识 merge
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}

	mergeFinRecord := &data.LogRecord{
		Key:   []byte(MergeFinishedKey),
		Value: []byte(strconv.Itoa(int(noMergeFileId))),
	}
	encoderLogRecord, _ := data.EncoderLogRecord(mergeFinRecord)
	err = mergeFinishedFile.Write(encoderLogRecord)
	if err != nil {
		return err
	}
	return nil
}

/**
 * getMergePath
 * @Description: 获取 merge 目录的路径
 * @receiver db
 * @return string
 */
func (db *DB) getMergePath() string {
	// path.clean,将路径最后的 / 去除，Dir 获取父目录
	dir := path.Dir(path.Clean(db.Options.DirPath))
	base := path.Base(db.Options.DirPath) // 获取当前文件目录
	return path.Join(dir, base+MergeDirName)
}

// 加载数据目录
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	//merge 目录不存在直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	// 获取 merge 目录下的所有文件
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 查找标识 merge 完成的文件，判断 merge 是否完成，同时将merge后的文件名保存
	var mergeFinished bool
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	// 如果 merge 没有完成，直接返回
	if !mergeFinished {
		return nil
	}

	// 获取没有被 merge 的文件 id
	noMergeFileId, err := db.getNoMergeFileId(mergePath)
	if err != nil {
		return err
	}
	//在原本目录下删除已经执行完 merge 的文件
	var fileId uint32 = 0
	for ; fileId < noMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.Options.DirPath, fileId)
		if _, err = os.Stat(fileName); err == nil {
			err := os.RemoveAll(fileName)
			if err != nil {
				return err
			}
		}
	}

	// 将 merge 之后的文件移动过来
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.Options.DirPath, fileName)
		// 将 oldpath 重命名（移动）为 newpath
		err := os.Rename(srcPath, destPath)
		if err != nil {
			return nil
		}
	}
	return nil

}

// 获取没有被 merge 的文件id
func (db *DB) getNoMergeFileId(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	noMergeFileId, err := strconv.Atoi(string(record.Value))
	return uint32((noMergeFileId)), nil
}

/**
 * loadIndexFromHintFile
 * @Description: 从 hint 文件加载索引
 * @receiver db
 * @return error
 */
func (db *DB) loadIndexFromHintFile() error {
	// 判断 hint 文件是否存在
	hintFileName := filepath.Join(db.Options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}
	// 打开 hint 索引文件
	hintFile, err := data.OpenHintFile(hintFileName)
	if err != nil {
		return err
	}

	// hint 文件中写入的是 logRecord，可以直接读取，key是真实的不加编码的 key
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// 解码拿到内存索引
		pos := data.DecoderLogRecordPos(logRecord.Value)
		db.Index.Put(logRecord.Key, pos)
		offset += size
	}
	return nil

}
