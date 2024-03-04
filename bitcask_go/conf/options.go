package conf

//数据库启动时接收用户自定义的配置项

type Options struct {
	// 数据库的数据目录
	DirPath string

	// 活跃文件可以写入数据的大小
	DataFileSize int64

	// 用于每次写入数据后判断用户是否需要可以进行数据持久化
	SyncWrite bool
}
