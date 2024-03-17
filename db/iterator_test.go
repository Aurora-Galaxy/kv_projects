package db

import (
	"github.com/stretchr/testify/assert"
	"kv_projects/conf"
	"kv_projects/utils"
	"os"
	"testing"
)

func TestDB_NewUserIterator(t *testing.T) {
	opts := conf.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	userIterator := db.NewUserIterator(conf.DefaultIteratorOptions)
	assert.NotNil(t, userIterator)
	assert.False(t, userIterator.Valid())
}

func TestDB_UserIterator_One_Value(t *testing.T) {
	opts := conf.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.GetTestValue(3))
	assert.Nil(t, err)

	userIterator := db.NewUserIterator(conf.DefaultIteratorOptions)
	assert.NotNil(t, userIterator)
	assert.True(t, userIterator.Valid())

}

func TestDB_UserIterator_Multi_Value(t *testing.T) {
	opts := conf.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("aabc"), utils.GetTestValue(3))
	assert.Nil(t, err)
	err = db.Put([]byte("bbcd"), utils.GetTestValue(3))
	assert.Nil(t, err)
	err = db.Put([]byte("ccde"), utils.GetTestValue(3))
	assert.Nil(t, err)
	err = db.Put([]byte("cdef"), utils.GetTestValue(3))
	assert.Nil(t, err)
	err = db.Put([]byte("cefg"), utils.GetTestValue(3))
	assert.Nil(t, err)

	// 正向迭代
	userIterator := db.NewUserIterator(conf.DefaultIteratorOptions)
	assert.NotNil(t, userIterator)
	assert.True(t, userIterator.Valid())

	for userIterator.Rewind(); userIterator.Valid(); userIterator.Next() {
		assert.NotNil(t, userIterator.Key())
	}
	userIterator.Rewind()
	for userIterator.Seek([]byte("b")); userIterator.Valid(); userIterator.Next() {
		t.Log(string(userIterator.Key()))
	}

	// 反向迭代
	conf.DefaultIteratorOptions.Reverse = true
	userIterator2 := db.NewUserIterator(conf.DefaultIteratorOptions)
	assert.NotNil(t, userIterator2)
	assert.True(t, userIterator2.Valid())

	for userIterator2.Rewind(); userIterator2.Valid(); userIterator2.Next() {
		assert.NotNil(t, userIterator2.Key())
	}
	userIterator2.Rewind()
	for userIterator2.Seek([]byte("b")); userIterator2.Valid(); userIterator2.Next() {
		t.Log(string(userIterator2.Key()))
	}

	// 指定 key 前缀 测试
	opt1 := conf.DefaultIteratorOptions
	opt1.Prefix = []byte("c")
	userIterator3 := db.NewUserIterator(opt1)
	assert.NotNil(t, userIterator3)
	assert.True(t, userIterator3.Valid())
	for userIterator3.Rewind(); userIterator3.Valid(); userIterator3.Next() {
		t.Log(string(userIterator3.Key()))
	}

}
