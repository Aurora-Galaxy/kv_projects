package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

type MMap struct {
	readerAt *mmap.ReaderAt
}

func NewMMapIOManager(filename string) (*MMap, error) {
	// 文件不存在则创建
	f, err := os.OpenFile(filename, os.O_CREATE, 0744)
	defer func() {
		_ = f.Close()
	}()
	if err != nil {
		return nil, err
	}
	readAt, err := mmap.Open(filename)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readAt}, nil
}

/**
 * Read
 * @Description: 从给定位置读取文件内容，由[]byte 接收，返回写入内容的字节数
 * @param []byte
 * @param int64
 * @return int
 * @return errs
 */
func (m *MMap) Read(b []byte, offset int64) (int, error) {
	return m.readerAt.ReadAt(b, offset)
}

/**
 * Write
 * @Description: 不使用mmap进行写操作
 * @param []byte
 * @return int
 * @return errs
 */
func (m *MMap) Write([]byte) (int, error) {
	panic("not implement")
}

/**
 * Sync
 * @Description: 持久化数据，将内存缓冲区的数据持久化到内存当中
 * @return errs
 */
func (m *MMap) Sync() error {
	panic("not implement")
}

/**
 * Close
 * @Description: 关闭文件
 * @return errs
 */
func (m *MMap) Close() error {
	return m.readerAt.Close()
}

/**
 * Size
 * @Description: 获取当前文件的文件大小
 * @return int64
 * @return error
 */
func (m *MMap) Size() (int64, error) {
	return int64(m.readerAt.Len()), nil
}
