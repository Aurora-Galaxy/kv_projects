package data

import (
	"github.com/stretchr/testify/assert"
	"kv_projects/fio"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	openDataFile, err := OpenDataFile("./temp", 0, fio.StandardIoManager)
	assert.Nil(t, err)
	assert.NotNil(t, openDataFile)

	openDataFile1, err := OpenDataFile("./temp", 999, fio.StandardIoManager)
	assert.Nil(t, err)
	assert.NotNil(t, openDataFile1)

}

func TestDataFile_Write(t *testing.T) {
	openDataFile, err := OpenDataFile("./temp", 0, fio.StandardIoManager)
	assert.Nil(t, err)
	assert.NotNil(t, openDataFile)

	err = openDataFile.Write([]byte("aaaa"))
	assert.Nil(t, err)

	err = openDataFile.Write([]byte("bbbb"))
	assert.Nil(t, err)

	err = openDataFile.Write([]byte("cccc"))
	assert.Nil(t, err)

}

func TestDataFile_Close(t *testing.T) {
	openDataFile, err := OpenDataFile("./temp", 0, fio.StandardIoManager)
	assert.Nil(t, err)
	assert.NotNil(t, openDataFile)

	err = openDataFile.Write([]byte("1243"))
	assert.Nil(t, err)

	err = openDataFile.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	openDataFile, err := OpenDataFile("./temp", 0, fio.StandardIoManager)
	assert.Nil(t, err)
	assert.NotNil(t, openDataFile)

	err = openDataFile.Write([]byte("457"))
	assert.Nil(t, err)

	err = openDataFile.Sync()
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile("./temp", 222, fio.StandardIoManager)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	logRecord := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("kv-project-go"),
		Type:  LogRecordNormal,
	}
	encoderLogRecord, size1 := EncoderLogRecord(logRecord)
	//t.Log(encoderLogRecord)
	err = dataFile.Write(encoderLogRecord)
	assert.Nil(t, err)

	res, size2, err := dataFile.ReadLogRecord(0)
	//t.Log(res, size2)
	assert.Nil(t, err)
	assert.Equal(t, logRecord, res)
	assert.Equal(t, size1, size2)

	logRecord2 := &LogRecord{
		Key:   []byte("name1"),
		Value: []byte("kv-project-go1"),
		Type:  LogRecordNormal,
	}
	encoderLogRecord, size3 := EncoderLogRecord(logRecord2)
	//t.Log(encoderLogRecord)
	err = dataFile.Write(encoderLogRecord)
	assert.Nil(t, err)

	res, size4, err := dataFile.ReadLogRecord(size1)
	//t.Log(res, size2)
	assert.Nil(t, err)
	assert.Equal(t, logRecord2, res)
	assert.Equal(t, size3, size4)

}
