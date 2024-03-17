package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncoderLogRecord(t *testing.T) {

	//1. 正常情况
	logRecord1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask_go"),
		Type:  LogRecordNormal,
	}

	encoderLogRecord, n := EncoderLogRecord(logRecord1)
	assert.NotNil(t, encoderLogRecord)
	assert.Greater(t, len(encoderLogRecord), 5)
	assert.Equal(t, int64(21), n)

	// 2. value 为空的情况
	logRecord2 := &LogRecord{
		Key:   []byte("name"),
		Value: nil,
		Type:  LogRecordNormal,
	}

	encoderLogRecord, n = EncoderLogRecord(logRecord2)
	//t.Log(encoderLogRecord)
	assert.NotNil(t, encoderLogRecord)
	assert.Greater(t, len(encoderLogRecord), 5)
	assert.Equal(t, int64(11), n)

	// 3. key 和 value 被删除
	// todo
}

func TestDecoderLogRecord(t *testing.T) {
	headerBuf1 := []byte{86, 238, 133, 193, 0, 8, 20}
	decoderLogRecord, n := DecoderLogRecord(headerBuf1)
	assert.NotNil(t, decoderLogRecord)
	assert.Equal(t, int64(7), n)
	assert.Equal(t, uint32(4), decoderLogRecord.keySize)
	assert.Equal(t, uint32(10), decoderLogRecord.valueSize)
	assert.Equal(t, uint32(3246779990), decoderLogRecord.crc)

	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	decoderLogRecord, n = DecoderLogRecord(headerBuf2)
	//t.Log(decoderLogRecord)
	assert.NotNil(t, decoderLogRecord)
	assert.Equal(t, int64(7), n)
	assert.Equal(t, uint32(4), decoderLogRecord.keySize)
	assert.Equal(t, uint32(0), decoderLogRecord.valueSize)
	assert.Equal(t, uint32(240712713), decoderLogRecord.crc)
}

func TestGetLogRecordCRC(t *testing.T) {
	logRecord := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask_go"),
		Type:  LogRecordNormal,
	}
	headerBuf1 := []byte{86, 238, 133, 193, 0, 8, 20}
	logRecordCRC := GetLogRecordCRC(logRecord, headerBuf1[crc32.Size:])
	assert.NotNil(t, logRecordCRC)
	assert.Equal(t, uint32(3246779990), logRecordCRC)

	logRecord2 := &LogRecord{
		Key:   []byte("name"),
		Value: nil,
		Type:  LogRecordNormal,
	}
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	logRecordCRC = GetLogRecordCRC(logRecord2, headerBuf2[crc32.Size:])
	assert.NotNil(t, logRecordCRC)
	assert.Equal(t, uint32(240712713), logRecordCRC)
}
