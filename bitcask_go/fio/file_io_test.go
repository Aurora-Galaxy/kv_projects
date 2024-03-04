package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

// 销毁文件。销毁前必须要把文件句柄关闭
func destroyFile(filepath string) {
	if err := os.RemoveAll(filepath); err != nil {
		panic(err)
	}
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join(".\\tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}

func TestNewFileIOManager(t *testing.T) {
	path := filepath.Join(".\\tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	err = fio.Close()
	assert.Nil(t, err)

}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join(".\\tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	write, err := fio.Write([]byte("aaa"))
	assert.Nil(t, err)
	assert.Equal(t, write, 3)
	err = fio.Close()
	assert.Nil(t, err)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join(".\\tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	write, err := fio.Write([]byte("bbb"))
	assert.Nil(t, err)
	assert.Equal(t, write, 3)

	b := make([]byte, 3)
	read, err := fio.Read(b, 0)
	t.Log(read)
	t.Log(string(b))
	assert.Nil(t, err)
	assert.Equal(t, read, 3)
	assert.Equal(t, b, []byte("bbb"))
	err = fio.Close()
	assert.Nil(t, err)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join(".\\tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
	err = fio.Close()
	assert.Nil(t, err)
}


