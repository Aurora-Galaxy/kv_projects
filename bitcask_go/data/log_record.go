package data

// 墓碑值，标记文件是否被删除
type LogRecordType = byte

const (
	// 正常状态
	LogRecordNormal LogRecordType = iota
	// 删除状态
	LogRecordDeleted
)

// LogRecord
// @Description: 数据写入到文件的记录，类似日志的形式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos 数据内存索引，用于记录数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 记录数据存储到文件的id
	Offset int64  // 偏移量，记录数据在文件中的位置
}

// 对LogRecord进行编码,因为写入文件时，写入的是字节数组
func EncoderLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}
