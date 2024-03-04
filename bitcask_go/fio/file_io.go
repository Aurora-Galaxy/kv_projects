package fio

import "os"

// FileIO
// @Description: 对go标准库文件io的封装
type FileIO struct {
	fd *os.File //系统文件描述符
}

/**
 * NewFileIO
 * @Description: 根据文件路径new一个文件IO接口
 * @param fileName
 * @return *FileIO
 * @return errs
 */
func NewFileIOManager(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_APPEND|os.O_RDWR, DataFilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

// 实现 IOmanager 接口

func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	// 从 offset 位置读取 len(b)长度的字节
	return fio.fd.ReadAt(b, offset)
}

func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIO) Close() error {
	return fio.fd.Close()
}
