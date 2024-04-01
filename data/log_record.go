package data

import (
	"encoding/binary"
	"hash/crc32"
)

// 墓碑值，标记文件是否被删除
type LogRecordType = byte

const (
	// 正常状态
	LogRecordNormal LogRecordType = iota
	// 删除状态
	LogRecordDeleted
	LogRecordTxnFinished
)

// 采用可变长编码
// crc , type , keySize , valueSize
// 4 + 1 + 5 + 5
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

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

// LogRecordHeader 代表 logRecord头部信息
type LogRecordHeader struct {
	crc        uint32        // 校验和
	recordType LogRecordType //墓碑值，标记该记录是否被删除
	keySize    uint32        // key 的长度
	valueSize  uint32        // value的长度
}

// 暂时存放事务数据
type TransactionLogRecord struct {
	Pos    *LogRecordPos
	Record *LogRecord
}

/**
 * EncoderLogRecord
 * @Description: 对LogRecord进行编码,因为写入文件时，写入的是字节数组
 * @param logRecord
 * @return []byte，编码后的结果
 * @return int64
 */
func EncoderLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// 初始化 header 长度的 []byte
	headerByte := make([]byte, maxLogRecordHeaderSize)

	// crc 需要最后计算，先从第五个字节开始存数据
	headerByte[4] = logRecord.Type

	// 变长存储 key 和 value 的 size
	var Index = 5
	// 将编码后的结果写入从Index开始的headerByte字节切片中，并返回写入数据的长度
	Index += binary.PutVarint(headerByte[Index:], int64(len(logRecord.Key)))
	Index += binary.PutVarint(headerByte[Index:], int64(len(logRecord.Value)))

	// size 代表真实 header 的大小，及压缩keySize 和 valueSize 之后的长度
	var size = Index + len(logRecord.Key) + len(logRecord.Value)

	// 真实返回的编码后的结果
	encoderLogRecord := make([]byte, size)
	copy(encoderLogRecord[:Index], headerByte[:Index])

	// 因为 key 和 value 均为 []byte，所以采用copy
	copy(encoderLogRecord[Index:], logRecord.Key)
	copy(encoderLogRecord[Index+len(logRecord.Key):], logRecord.Value)

	// 计算 crc 校验值
	crc := crc32.ChecksumIEEE(encoderLogRecord[4:])
	// 小端序，存放，高位存高字节
	binary.LittleEndian.PutUint32(encoderLogRecord[:4], crc)

	//fmt.Printf("encoder byte : %d , length : %d\n", crcHeader, int64(size))
	return encoderLogRecord, int64(size)
}

/**
 * DecoderLogRecord
 * @Description: 对字节数组的 Header 信息进行解码
 * @param buf
 * @return *LogRecordHeader
 * @return int64 ， header的实际长度
 */
func DecoderLogRecord(buf []byte) (*LogRecordHeader, int64) {
	// 字节数小于crc长度，数据无效
	if len(buf) < 4 {
		return nil, 0
	}

	header := &LogRecordHeader{
		// 将前4个字节按小端取出
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4], // 墓碑值
	}

	// 读取 varint协议压缩的key 和 value 的size
	var Index int = 5
	keySize, n := binary.Varint(buf[Index:])
	header.keySize = uint32(keySize)
	Index += n

	valueSize, n := binary.Varint(buf[Index:])
	header.valueSize = uint32(valueSize)
	Index += n

	// 返回 Header 及其长度
	return header, int64(Index)
}

/**
 * EncoderLogRecordPos
 * @Description: 对 logRecordPos 进行编码
 * @param pos
 * @return []byte，编码后的结果
 */
func EncoderLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	index := 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	// 返回编码结果
	return buf[:index]
}

func DecoderLogRecordPos(buf []byte) *LogRecordPos {
	index := 0
	fId, size := binary.Varint(buf[index:])
	offset, _ := binary.Varint(buf[index+size:])
	return &LogRecordPos{
		Fid:    uint32(fId),
		Offset: offset,
	}
}

/**
 * GetLogRecordCRC
 * @Description: 获取 LogRecord 中的crc校验值
 * @param lr
 * @param headerNotCRC
 * @return uint32
 */
func GetLogRecordCRC(lr *LogRecord, headerNotCRC []byte) uint32 {
	if lr == nil {
		return 0
	}
	// 该头部不包含crc，以及 key，value
	crc := crc32.ChecksumIEEE(headerNotCRC[:])
	// 使用 key 和 value 更新 crc 校验值
	//IEEETable 是 crc32 提供的一个预定义值，代表一个特定的 CRC 哈希函数。
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)
	return crc
}
