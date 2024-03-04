package index

import (
	"bitcask_go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBtree()

	res1 := bt.Put(nil, &data.LogRecord{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecord{
		Fid:    1,
		Offset: 1000,
	})
	assert.True(t, res2)

}

func TestBTree_Get(t *testing.T) {
	bt := NewBtree()

	res1 := bt.Put(nil, &data.LogRecord{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res1)
	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	//测试key对应值的修改
	res2 := bt.Put([]byte("a"), &data.LogRecord{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecord{
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

	res1 := bt.Put(nil, &data.LogRecord{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res1)
	delRes1 := bt.Delete(nil)
	assert.True(t, delRes1)

	res2 := bt.Put([]byte("a"), &data.LogRecord{
		Fid:    1,
		Offset: 1000,
	})
	assert.True(t, res2)
	delRes2 := bt.Delete([]byte("a"))
	assert.True(t, delRes2)
}
