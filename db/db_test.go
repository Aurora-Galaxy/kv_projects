package db

import (
	"github.com/stretchr/testify/assert"
	"kv_projects/conf"
	"kv_projects/errs"
	"kv_projects/utils"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 测试完成之后销毁 DB 数据目录
func destroyDB(db *DB) {
	if db != nil {
		if db.ActiveFile != nil {
			_ = db.Close()
		}
		_ = db.Close()
		err := os.RemoveAll(db.Options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestOpen(t *testing.T) {
	opts := conf.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	opts := conf.DefaultOptions
	//dir, _ := os.MkdirTemp("", "bitcask-go-put")
	opts.DirPath = "./temp"
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	//
	// 1.正常 Put 一条数据
	err = db.Put(utils.GetTestKey(1), utils.GetTestValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.重复 Put key 相同的数据
	err = db.Put(utils.GetTestKey(1), utils.GetTestValue(24))
	assert.Nil(t, err)
	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	// 3.key 为空
	err = db.Put(nil, utils.GetTestValue(24))
	assert.Equal(t, errs.ErrKeyIsEmpty, err)

	// 4.value 为空
	err = db.Put(utils.GetTestKey(22), nil)
	assert.Nil(t, err)
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Equal(t, 0, len(val3))
	assert.Nil(t, err)

	//5.写到数据文件末尾，进行文件切换
	for i := 0; i < 100; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
		assert.Nil(t, err)
	}
	//assert.Equal(t, 2, len(db.OlderFiles))

	// 6.重启后再 Put 数据
	err = db.Close()
	//err = db.ActiveFile.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	defer destroyDB(db2)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val4 := utils.GetTestValue(128)
	err = db2.Put(utils.GetTestKey(55), val4)
	assert.Nil(t, err)
	val5, err := db2.Get(utils.GetTestKey(55))
	assert.Nil(t, err)
	assert.Equal(t, val4, val5)
	//db.ActiveFile.IOManager.Close()

}

func TestDB_Get(t *testing.T) {
	opts := conf.DefaultOptions
	//dir, _ := os.MkdirTemp("", "bitcask-go-get")
	opts.DirPath = "./temp"
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常读取一条数据
	err = db.Put(utils.GetTestKey(11), utils.GetTestValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.读取一个不存在的 key
	val2, err := db.Get([]byte("some key unknown"))
	assert.Nil(t, val2)
	assert.Equal(t, errs.ErrKeyNotFound, err)

	// 3.值被重复 Put 后在读取
	err = db.Put(utils.GetTestKey(22), utils.GetTestValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.GetTestValue(24))
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val3)

	// 4.值被删除后再 Get
	err = db.Put(utils.GetTestKey(33), utils.GetTestValue(24))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(33))
	assert.Nil(t, err)
	val4, err := db.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val4))
	assert.Equal(t, errs.ErrKeyNotFound, err)

	// 5.转换为了旧的数据文件，从旧的数据文件上获取 value
	//for i := 100; i < 1000000; i++ {
	//	err := db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
	//	assert.Nil(t, err)
	//}
	//assert.Equal(t, 2, len(db.OlderFiles))
	//val5, err := db.Get(utils.GetTestKey(101))
	//assert.Nil(t, err)
	//assert.NotNil(t, val5)

	// 6.重启后，前面写入的数据都能拿到
	err = db.Close()
	//err = db.ActiveFile.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	val6, err := db2.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val6)
	assert.Equal(t, val1, val6)

	val7, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val7)
	assert.Equal(t, val3, val7)

	val8, err := db.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val8))
	assert.Equal(t, errs.ErrKeyNotFound, err)
}

func TestDB_Delete(t *testing.T) {
	opts := conf.DefaultOptions
	//dir, _ := os.MkdirTemp("", "bitcask-go-delete")
	opts.DirPath = "./temp"
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常删除一个存在的 key
	err = db.Put(utils.GetTestKey(11), utils.GetTestValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(11))
	assert.Nil(t, err)
	_, err = db.Get(utils.GetTestKey(11))
	assert.Equal(t, errs.ErrKeyNotFound, err)

	// 2.删除一个不存在的 key
	err = db.Delete([]byte("unknown key"))
	assert.NotNil(t, err)

	// 3.删除一个空的 key
	err = db.Delete(nil)
	assert.Equal(t, errs.ErrKeyIsEmpty, err)

	// 4.值被删除之后重新 Put
	err = db.Put(utils.GetTestKey(22), utils.GetTestValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(22))
	assert.Nil(t, err)

	err = db.Put(utils.GetTestKey(22), utils.GetTestValue(128))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(22))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// 5.重启之后，再进行校验
	err = db.Close()
	//err = db.ActiveFile.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	_, err = db2.Get(utils.GetTestKey(11))
	assert.Equal(t, errs.ErrKeyNotFound, err)

	val2, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.Equal(t, val1, val2)
}

func TestDB_ListKeys(t *testing.T) {
	opts := conf.DefaultOptions
	//dir, _ := os.MkdirTemp("", "bitcask-go-delete")
	opts.DirPath = "./temp"
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("aaa"), utils.GetTestValue(10))
	assert.Nil(t, err)
	/*
	 * 在这发生死锁的原因：put加上写锁，锁释放后。ListKeys会加读锁，但是没有释放，后面的put又加写锁造成死锁
	 * 解决方案：在ListKeys方法里，将读锁释放
	 */
	assert.NotNil(t, db.ListKeys())
	err = db.Put([]byte("aaa"), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bbb"), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("ccc"), utils.GetTestValue(10))
	assert.Nil(t, err)
	for _, v := range db.ListKeys() {
		t.Log(string(v))
		assert.NotNil(t, v)
	}
}

func TestDB_Fold(t *testing.T) {
	opts := conf.DefaultOptions
	//dir, _ := os.MkdirTemp("", "bitcask-go-delete")
	opts.DirPath = "./temp"
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("aaa"), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bbb"), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("ccc"), utils.GetTestValue(10))
	assert.Nil(t, err)

	err = db.Fold(func(key, value []byte) bool {
		t.Log("key = ", string(key))
		t.Log("value = ", string(value))
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		return true
	})
	assert.Nil(t, err)
}

func TestDB_Sync(t *testing.T) {
	opts := conf.DefaultOptions
	//dir, _ := os.MkdirTemp("", "bitcask-go-delete")
	opts.DirPath = "./temp"
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("aaa"), utils.GetTestValue(10))
	assert.Nil(t, err)

	err = db.Sync()
	assert.Nil(t, err)
}

func TestDB_Close(t *testing.T) {
	opts := conf.DefaultOptions
	//dir, _ := os.MkdirTemp("", "bitcask-go-delete")
	opts.DirPath = "./temp"
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("aaa"), utils.GetTestValue(10))
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)
}

func TestDB_FileLock(t *testing.T) {
	opts := conf.DefaultOptions
	//dir, _ := os.MkdirTemp("", "bitcask-go-delete")
	opts.DirPath = "./temp"
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	db2, err := Open(opts)
	assert.Nil(t, db2)
	assert.Equal(t, errs.ErrDatabaseIsUsing, err)

	err = db.Close()
	assert.Nil(t, err)

	db3, err := Open(opts)
	t.Log(db3)
	t.Log(err)
	err = db3.Close()
	assert.Nil(t, err)

}

func TestDB_OpenMMap(t *testing.T) {
	opts := conf.DefaultOptions
	opts.DirPath = "./temp"
	//使用 mmap   open time   1.028ms
	//不使用 mmap   open time  2.8033ms
	//opts.MMapAtStartUp = false

	now := time.Now()
	db, err := Open(opts)
	t.Log("open time ", time.Since(now))

	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Stat(t *testing.T) {
	opts := conf.DefaultOptions
	//dir, _ := os.MkdirTemp("", "bitcask-go-delete")
	opts.DirPath = "./temp"
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 1000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
		assert.Nil(t, err)
	}

	for i := 0; i < 1000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}

	for i := 1000; i < 2000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
		assert.Nil(t, err)
	}

	stat := db.Stat()
	t.Log(stat)
}

func TestDB_BackUp(t *testing.T) {
	opts := conf.DefaultOptions
	dir := filepath.Join("./temp", "bitcask-go-backup")
	opts.DataFileSize = 32 * 1024 * 1024
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(1024))
		assert.Nil(t, err)
	}

	err = db.BackUp(filepath.Join("./temp", "bitcask-go-test"))
	assert.Nil(t, err)

	defer func() {
		_ = db.Close()
	}()

	opts2 := conf.DefaultOptions
	dir2 := filepath.Join("./temp", "bitcask-go-test")
	opts.DataFileSize = 32 * 1024 * 1024
	opts2.DirPath = dir2
	db2, err := Open(opts2)

	for i := 0; i < 50000; i++ {
		val, err := db.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
	defer destroyDB(db2)
	assert.Nil(t, err)
	assert.NotNil(t, db2)

	defer func() {
		_ = db2.Close()
	}()
}
