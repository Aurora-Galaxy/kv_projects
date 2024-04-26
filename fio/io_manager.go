package fio

// 即用户具有读写权限，组用户和其它用户具有只读权限；
const DataFilePerm = 0644

type FileIOType = byte

const (
	StandardIoManager FileIOType = iota

	MMapIoManager
)

/**
 * IOManager
 * @Description: 抽象 IO 管理接口，可以接入不同IO类型，目前仅支持标准文件IO
 */
type IOManager interface {
	/**
	 * Read
	 * @Description: 从给定位置读取文件内容，由[]byte 接收，返回写入内容的字节数
	 * @param []byte
	 * @param int64
	 * @return int
	 * @return errs
	 */
	Read([]byte, int64) (int, error)

	/**
	 * Write
	 * @Description: 向文件中写入内容，返回写入内容的字节数
	 * @param []byte
	 * @return int
	 * @return errs
	 */
	Write([]byte) (int, error)

	/**
	 * Sync
	 * @Description: 持久化数据，将内存缓冲区的数据持久化到内存当中
	 * @return errs
	 */
	Sync() error

	/**
	 * Close
	 * @Description: 关闭文件
	 * @return errs
	 */
	Close() error

	/**
	 * Size
	 * @Description: 获取当前文件的文件大小
	 * @return int64
	 * @return error
	 */
	Size() (int64, error)
}

/**
 * NewIOManager
 * @Description: 初始化IOManager，后续添加标准可以做一个判断初始化不同的io类型
 * @param fileName
 * @return IOManager
 * @return error
 */
func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardIoManager:
		return NewFileIOManager(fileName)
	case MMapIoManager:
		return NewMMapIOManager(fileName)
	default:
		panic("unsupported io type")
	}
}
