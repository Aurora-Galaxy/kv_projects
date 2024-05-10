package benchmark

import (
	"fmt"
	"kv_projects/conf"
	"kv_projects/db"
	"kv_projects/errs"
	"kv_projects/utils"
	"math/rand"
	"path/filepath"
	"testing"
	"time"
)

// 存储引擎初始化方法
var Db *db.DB

func init() {
	var err error
	opts := conf.DefaultOptions
	dir := filepath.Join("./temp", "bitcask-go-http")
	opts.DataFileSize = 32 * 1024 * 1024
	opts.DirPath = dir
	Db, err = db.Open(opts)
	if err != nil {
		panic("db init failed")
	}
	fmt.Println("db init success")
}

func BenchmarkPut(b *testing.B) {
	// 将时间重置，从该行往下执行的时间算入测试时间
	b.ResetTimer()
	// 启用内存分配统计
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := Db.Put(utils.GetTestKey(i), utils.GetTestValue(i))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	for i := 0; i < 10000; i++ {
		_ = Db.Put(utils.GetTestKey(i), utils.GetTestValue(1024))
	}

	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := Db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != errs.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func BenchmarkDelete(b *testing.B) {
	for i := 0; i < 10000; i++ {
		_ = Db.Put(utils.GetTestKey(i), utils.GetTestValue(1024))
	}

	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := Db.Delete(utils.GetTestKey(rand.Int()))
		if err != nil && err != errs.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}
