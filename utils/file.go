package utils

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unsafe"
)

/**
 * DirSize
 * @Description: 获取目录占用空间的大小
 * @param dirPath
 * @return int64
 * @return error
 */
func DirSize(dirPath string) (uint64, error) {
	var size uint64
	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += uint64(info.Size())
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return size, nil
}

/**
 * AvailableDiskSize
 * @Description: 获取当前磁盘剩余容量大小,适用于windows系统
 * @return uint64
 * @return error
 */
func AvailableDiskSize() (uint64, error) {
	// 获取当前工作目录
	currenDir, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}
	// 获取当前文件夹在那个磁盘上
	volumeName := filepath.VolumeName(currenDir)
	h := syscall.MustLoadDLL("kernel32.dll")
	c := h.MustFindProc("GetDiskFreeSpaceExW")
	lpFreeBytesAvailable := int64(0)
	lpTotalNumberOfBytes := int64(0)
	lpTotalNumberOfFreeBytes := int64(0)
	r2, _, err := c.Call(uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(volumeName))),
		uintptr(unsafe.Pointer(&lpFreeBytesAvailable)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfBytes)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfFreeBytes)))
	if r2 == 0 {
		fmt.Println("Syscall6 调用失败:", err)
		return 0, err
	} else {
		//fmt.Printf("总空间: %.2f GB\n", float64(lpTotalNumberOfBytes)/float64(1024*1024*1024))
		//fmt.Printf("可用空间: %.2f GB\n", float64(lpFreeBytesAvailable)/float64(1024*1024*1024))
		//fmt.Printf("总可用空间: %.2f GB\n", float64(lpTotalNumberOfFreeBytes)/float64(1024*1024*1024))
		// 返回剩余字节数
		return uint64(lpTotalNumberOfFreeBytes), nil
	}
}

/**
 * CopyDir
 * @Description: 将给出的源路径内容拷贝到目的路径
 * @param src
 * @param dest
 * @param extends  需要排除的文件，不进行拷贝
 * @return error
 */
func CopyDir(src, dest string, extends []string) error {
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		err := os.MkdirAll(dest, os.ModePerm)
		if err != nil {
			return err
		}
	}
	// 递归遍历源路径，复制文件夹下全部内容
	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		// 获取文件名称
		//例：path:/data/test/000.data  src:/data/test/,在path中将src用空替换得到000.data文件名
		fileName := strings.Replace(path, src, "", 1)
		if fileName == "" {
			return nil
		}

		for _, e := range extends {
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}
			// 当前文件名如果和排除的文件名相同，直接返回nil
			if matched {
				return nil
			}
		}

		// 文件夹直接在备份的目录中创建
		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dest, info.Name()), info.Mode())
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dest, fileName), data, info.Mode())
	})
}
