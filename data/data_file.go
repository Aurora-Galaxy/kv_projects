package data

import (
	"fmt"
	"hash/crc32"
	"io"
	"kv_projects/errs"
	"kv_projects/fio"
	"path/filepath"
)

// 约定数据存储在以.data为后缀的文件内
const (
	DataFileNameSuffix    = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
)

type DataFile struct {
	FileId      uint32        // 当前文件的id
	WriteOffset int64         // 文件写到的位置
	IOManager   fio.IOManager // 管理文件读写操作
}

// 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := GetDataFileName(dirPath, fileId)
	return newDataFile(fileName, fileId)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

// 打开 hint 文件
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0)

}

// 打开标识 merge 完成文件
func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0)
}

func newDataFile(fileName string, fileId uint32) (*DataFile, error) {
	// 初始化 IOManager 管理器接口
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:      fileId,
		WriteOffset: 0,
		IOManager:   ioManager,
	}, nil
}

// 文件持久化操作
func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

// 关闭文件
func (df *DataFile) Close() error {
	return df.IOManager.Close()
}

// 文件写入操作
func (df *DataFile) Write(buf []byte) error {
	write, err := df.IOManager.Write(buf)
	if err != nil {
		return err
	}
	// 更新偏移量
	df.WriteOffset += int64(write)
	return nil
}

/**
 * ReadLogRecord
 * @Description: 根据offset从文件中取出对应的 LogRecord
 * @receiver df
 * @param offset
 * @return *LogRecord
 * @return int64, 代表整个 logRecord的长度
 * @return error
 */
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}
	// 如果读取的最大 header 长度超过文件的长度，直接读取到文件末尾即可
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	headerBuf, err := df.ReadNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}
	header, headerSize := DecoderLogRecord(headerBuf)
	// 表示读到文件末尾，直接返回EOF
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}
	// 取出对应的 key 和 value 的长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)

	// 该数据记录的总长度
	var logrecordSize = headerSize + keySize + valueSize
	var logRecord = &LogRecord{Type: header.recordType}
	// 开始读取真实的数据
	if keySize > 0 || valueSize > 0 {
		kvBuff, err := df.ReadNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		logRecord.Key = kvBuff[:keySize]
		logRecord.Value = kvBuff[keySize:]
	}

	// 校验数据的有效性 获取 除 crc 以外的内容计算校验值，与原本crc进行比较
	crc := GetLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, errs.ErrInvalidCRC
	}
	return logRecord, logrecordSize, nil
}

/**
 * WriteHintFile
 * @Description: 将 key 和其对应的文件索引信息写入 Hint 索引文件
 * @receiver df
 * @param key
 * @param pos
 * @return error
 */
func (df *DataFile) WriteHintFile(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncoderLogRecordPos(pos),
	}
	logRecord, _ := EncoderLogRecord(record)
	if err := df.Write(logRecord); err != nil {
		return err
	}
	return nil
}

// 读取 n 个byte
func (df *DataFile) ReadNBytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := df.IOManager.Read(b, offset)
	if err != nil {
		return nil, err
	}
	return b, nil
}
