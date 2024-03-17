package index

import (
	"github.com/stretchr/testify/assert"
	"kv_projects/data"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBtree()

	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 1000,
	})
	assert.True(t, res2)

}

func TestBTree_Get(t *testing.T) {
	bt := NewBtree()

	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res1)
	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	//测试key对应值的修改
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    2,
		Offset: 200,
	})
	assert.True(t, res3)
	pos2 := bt.Get([]byte("a"))
	t.Log(pos2.Fid, pos2.Offset)
	assert.Equal(t, uint32(2), pos2.Fid)
	assert.Equal(t, int64(200), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBtree()

	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res1)
	delRes1 := bt.Delete(nil)
	assert.True(t, delRes1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 1000,
	})
	assert.True(t, res2)
	delRes2 := bt.Delete([]byte("a"))
	assert.True(t, delRes2)
}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBtree()
	// 1. btree 为空的情况
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	//2. 用数据的情况
	bt1.Put([]byte("test"), &data.LogRecordPos{
		Fid:    1,
		Offset: 0,
	})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	//t.Log(iter2.Key())
	//t.Log(iter2.Value())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.False(t, iter2.Valid())

	// 多条数据
	bt1.Put([]byte("aabb"), &data.LogRecordPos{
		Fid:    1,
		Offset: 0,
	})
	bt1.Put([]byte("bbcc"), &data.LogRecordPos{
		Fid:    2,
		Offset: 0,
	})
	bt1.Put([]byte("ccdd"), &data.LogRecordPos{
		Fid:    3,
		Offset: 0,
	})
	bt1.Put([]byte("ddee"), &data.LogRecordPos{
		Fid:    4,
		Offset: 0,
	})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		t.Log(string(iter3.Key()))
		assert.NotNil(t, iter3.Key())
		assert.NotNil(t, iter3.Value())
	}
	// 打印 key 大于等于 cc
	for iter3.Seek([]byte("cc")); iter3.Valid(); iter3.Next() {
		t.Log(string(iter3.Key()))
		assert.NotNil(t, iter3.Key())
	}

	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		//assert.NotNil(t, iter4.Key())
		//assert.NotNil(t, iter4.Value())
		t.Log(string(iter4.Key()))
	}
	// 打印 key 小于等于 cc 的
	for iter4.Seek([]byte("cc")); iter4.Valid(); iter4.Next() {
		t.Log(string(iter4.Key()))
		assert.NotNil(t, iter4.Key())
	}

}
