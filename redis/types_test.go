package redis

import (
	"github.com/stretchr/testify/assert"
	"kv_projects/conf"
	"kv_projects/db"
	"kv_projects/errs"
	"kv_projects/utils"
	"os"
	"testing"
	"time"
)

func destroyDB(db *db.DB) {
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

func TestRedisDataStructure_Get(t *testing.T) {
	opts := conf.DefaultOptions
	opts.DirPath = "./temp"
	rds, err := NewRedisDataStructure(opts)
	defer destroyDB(rds.db)
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), utils.GetTestValue(100), 0)
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(2), utils.GetTestValue(100), time.Second*5)
	assert.Nil(t, err)

	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	val2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	_, err = rds.Get(utils.GetTestKey(33))
	assert.Equal(t, errs.ErrKeyNotFound, err)
}

func TestRedisDataStructure_Del_Type(t *testing.T) {
	opts := conf.DefaultOptions
	opts.DirPath = "./temp"
	rds, err := NewRedisDataStructure(opts)
	defer destroyDB(rds.db)
	assert.Nil(t, err)

	// del
	err = rds.Delete(utils.GetTestKey(1))
	assert.NotNil(t, err)

	err = rds.Set(utils.GetTestKey(1), utils.GetTestValue(100), 0)
	assert.Nil(t, err)

	// type
	typ, err := rds.Type(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, String, typ)

	err = rds.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	_, err = rds.Get(utils.GetTestKey(1))
	assert.Equal(t, errs.ErrKeyNotFound, err)
}

func TestRedisDataStructure_HGet(t *testing.T) {
	opts := conf.DefaultOptions
	opts.DirPath = "./temp"
	rds, err := NewRedisDataStructure(opts)
	defer destroyDB(rds.db)
	assert.Nil(t, err)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.GetTestValue(100))
	assert.Nil(t, err)
	assert.True(t, ok1)

	v1 := utils.GetTestValue(100)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	assert.Nil(t, err)
	assert.False(t, ok2)

	v2 := utils.GetTestValue(100)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	assert.Nil(t, err)
	assert.True(t, ok3)

	val1, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.Equal(t, v1, val1)

	val2, err := rds.HGet(utils.GetTestKey(1), []byte("field2"))
	assert.Nil(t, err)
	assert.Equal(t, v2, val2)

	_, err = rds.HGet(utils.GetTestKey(1), []byte("field-not-exist"))
	assert.Equal(t, errs.ErrKeyNotFound, err)
}

func TestRedisDataStructure_HDel(t *testing.T) {
	opts := conf.DefaultOptions
	opts.DirPath = "./temp"
	rds, err := NewRedisDataStructure(opts)
	defer destroyDB(rds.db)
	assert.Nil(t, err)

	del1, err := rds.HDel(utils.GetTestKey(200), nil)
	assert.Nil(t, err)
	assert.False(t, del1)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.GetTestValue(100))
	assert.Nil(t, err)
	assert.True(t, ok1)

	v1 := utils.GetTestValue(100)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	assert.Nil(t, err)
	assert.False(t, ok2)

	v2 := utils.GetTestValue(100)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	assert.Nil(t, err)
	assert.True(t, ok3)

	del2, err := rds.HDel(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.True(t, del2)
}

func TestRedisDataStructure_SIsMember(t *testing.T) {
	opts := conf.DefaultOptions
	opts.DirPath = "./temp"
	rds, err := NewRedisDataStructure(opts)
	defer destroyDB(rds.db)
	assert.Nil(t, err)

	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	// 重复提交，即重新添加已有元素，返回false
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(2), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-not-exist"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestRedisDataStructure_SRem(t *testing.T) {
	opts := conf.DefaultOptions
	opts.DirPath = "./temp"
	rds, err := NewRedisDataStructure(opts)
	defer destroyDB(rds.db)
	assert.Nil(t, err)

	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SRem(utils.GetTestKey(2), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SRem(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestRedisDataStructure_LPop(t *testing.T) {
	opts := conf.DefaultOptions
	opts.DirPath = "./temp"
	rds, err := NewRedisDataStructure(opts)
	defer destroyDB(rds.db)
	assert.Nil(t, err)

	res, err := rds.LPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), res)
	res, err = rds.LPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), res)
	res, err = rds.LPush(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), res)

	val, err := rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
}

func TestRedisDataStructure_RPop(t *testing.T) {
	opts := conf.DefaultOptions
	opts.DirPath = "./temp"
	rds, err := NewRedisDataStructure(opts)
	defer destroyDB(rds.db)
	assert.Nil(t, err)

	res, err := rds.RPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), res)
	res, err = rds.RPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), res)
	res, err = rds.RPush(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), res)

	val, err := rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
}

func TestRedisDataStructure_ZScore(t *testing.T) {
	opts := conf.DefaultOptions
	opts.DirPath = "./temp"
	rds, err := NewRedisDataStructure(opts)
	defer destroyDB(rds.db)
	assert.Nil(t, err)

	ok, err := rds.ZAdd(utils.GetTestKey(1), 113, []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.ZAdd(utils.GetTestKey(1), 333, []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.ZAdd(utils.GetTestKey(1), 98, []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	score, err := rds.ZScore(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, float64(333), score)
	score, err = rds.ZScore(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, float64(98), score)
}
