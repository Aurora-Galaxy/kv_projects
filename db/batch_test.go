package db

import (
	"github.com/stretchr/testify/assert"
	"kv_projects/conf"
	"kv_projects/errs"
	"kv_projects/utils"
	"testing"
)

func TestWriteBatch_Commit(t *testing.T) {
	opts := conf.DefaultOptions
	//dir, _ := os.MkdirTemp("", "bitcask-go-put")
	opts.DirPath = "./temp"
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 写数据后不提交
	wb := db.NewWriteBatch(&conf.DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(10), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(11))
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(10))
	//t.Log(value)
	//t.Log(err)
	assert.Equal(t, errs.ErrKeyNotFound, err)

	// 写数据后 commit
	err = wb.Commit()
	assert.Nil(t, err)
	val, err := db.Get(utils.GetTestKey(10))
	//t.Log(string(val))
	assert.Nil(t, err)
	assert.NotNil(t, val)

	// 删除数据后，commit
	wb2 := db.NewWriteBatch(&conf.DefaultWriteBatchOptions)
	err = wb2.Delete(utils.GetTestKey(10))
	assert.Nil(t, err)

	err = wb2.Commit()
	assert.Nil(t, err)
	val, err = db.Get(utils.GetTestKey(10))
	//t.Log(string(val))
	//t.Log(err)
	assert.Equal(t, errs.ErrKeyNotFound, err)
	assert.Nil(t, val)
}

func TestWriteBatch_Commit2(t *testing.T) {
	opts := conf.DefaultOptions
	//dir, _ := os.MkdirTemp("", "bitcask-go-put")
	opts.DirPath = "./temp"
	//opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.GetTestValue(1))
	assert.Nil(t, err)

	wt := db.NewWriteBatch(&conf.DefaultWriteBatchOptions)
	err = wt.Put(utils.GetTestKey(2), utils.GetTestValue(1))
	assert.Nil(t, err)

	err = wt.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = wt.Commit()
	assert.Nil(t, err)

	err = wt.Put(utils.GetTestKey(2), utils.GetTestValue(1))
	assert.Nil(t, err)
	err = wt.Commit()
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)
	//重启
	db2, err := Open(opts)
	defer destroyDB(db2)
	assert.Nil(t, err)
	assert.NotNil(t, db2)

	val, err := db2.Get(utils.GetTestKey(1))
	//t.Log(val)
	//t.Log(err)
	assert.Nil(t, val)
	assert.Equal(t, errs.ErrKeyNotFound, err)

	assert.Equal(t, uint64(2), db2.SeqNo)

}
