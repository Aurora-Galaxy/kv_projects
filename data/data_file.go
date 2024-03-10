package data

import "kv_projects/fio"

// 约定数据存储在以.data为后缀的文件内
const DataFileNameSuffix = ".data"

type DataFile struct {
	FileId      uint32        // 当前文件的id
	WriteOffset int64         // 文件写到的位置
	IOManager   fio.IOManager // 管理文件读写操作
}

// 打开新的数据文件
func OpenDataFile(dirPath string, filed uint32) (*DataFile, error) {
	return nil, nil
}

// 文件持久化操作
func (df *DataFile) Sync() error {
	return nil
}

// 文件写入操作
func (df *DataFile) Write(buf []byte) error {
	return nil
}

// 文件读取操作
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}
