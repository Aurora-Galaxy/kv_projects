package redis

import (
	"encoding/binary"
	"kv_projects/utils"
	"math"
)

/* 元数据结构
        +----------+------------+-----------+-----------+
key =>  |   type   |  expire    |  version  |  size     |
        | (1byte)  | (8byte)    |  (8byte)  | (Sbyte)   |
        +----------+------------+-----------+-----------+
*/

const (
	maxMetaDataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetaSize = binary.MaxVarintLen64 * 2
	initialListMask   = math.MaxUint64 / 2
)

type MetaData struct {
	dataType byte   // 数据类型
	expire   int64  // 过期时间
	version  int64  // 版本号
	size     uint32 // 数据量
	head     uint64 // List 数据结构使用
	tail     uint64 // List 数据结构使用
}

/**
 * encoder
 * @Description: 编码元数据
 * @receiver md
 * @return []byte
 */
func (md *MetaData) encoderMetadata() []byte {
	var size = maxMetaDataSize
	if md.dataType == List {
		size += extraListMetaSize
	}
	buffer := make([]byte, size)
	buffer[0] = md.dataType
	var index = 1
	index += binary.PutVarint(buffer[index:], md.expire)
	index += binary.PutVarint(buffer[index:], md.version)
	index += binary.PutVarint(buffer[index:], int64(md.size))

	if md.dataType == List {
		index += binary.PutUvarint(buffer[index:], md.head)
		index += binary.PutUvarint(buffer[index:], md.tail)
	}
	return buffer[:index]
}

/**
 * decoderMetadata
 * @Description: 将数据解码为 metadata 格式
 * @receiver md
 * @return *MetaData
 */
func decoderMetadata(buffer []byte) *MetaData {
	dataType := buffer[0]
	var index = 1
	expire, n := binary.Varint(buffer[index:])
	index += n
	version, n := binary.Varint(buffer[index:])
	index += n
	size, n := binary.Varint(buffer[index:])
	index += n

	var head uint64 = 0
	var tail uint64 = 0
	if dataType == List {
		head, n = binary.Uvarint(buffer[index:])
		index += n
		tail, _ = binary.Uvarint(buffer[index:])
	}

	return &MetaData{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}

/*
	+---------------+

key|version|field => |     value     |

	+---------------+
*/
type hashInternalKey struct {
	key     []byte
	version int64
	filed   []byte
}

func (hk *hashInternalKey) encoder() []byte {
	buffer := make([]byte, len(hk.key)+len(hk.filed)+8)
	index := 0
	copy(buffer[index:index+len(hk.key)], hk.key)
	index += len(hk.key)

	//version
	// 将int64 按照小端序存入字节切片
	binary.LittleEndian.PutUint64(buffer[index:index+8], uint64(hk.version))
	index += 8

	copy(buffer[index:], hk.filed)
	return buffer
}

/*
	+---------------+

key|version|member|member size => |     NULL      |

	+---------------+
*/
type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

func (sk *setInternalKey) encoder() []byte {
	buffer := make([]byte, len(sk.key)+len(sk.member)+8+4)
	index := 0
	copy(buffer[index:index+len(sk.key)], sk.key)
	index += len(sk.key)

	//version
	// 将int64 按照小端序存入字节切片
	binary.LittleEndian.PutUint64(buffer[index:index+8], uint64(sk.version))
	index += 8

	copy(buffer[index:], sk.member)
	index += len(sk.member)

	// member size按照小端序存入字节切片
	binary.LittleEndian.PutUint32(buffer[index:], uint32(len(sk.member)))

	return buffer
}

/*
	+---------------+

key|version|index => |     value     |

	+---------------+
*/
type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (lk *listInternalKey) encoder() []byte {
	buffer := make([]byte, len(lk.key)+8+8)
	index := 0
	copy(buffer[index:index+len(lk.key)], lk.key)
	index += len(lk.key)

	//version
	// 将int64 按照小端序存入字节切片
	binary.LittleEndian.PutUint64(buffer[index:index+8], uint64(lk.version))
	index += 8

	// member size按照小端序存入字节切片
	binary.LittleEndian.PutUint64(buffer[index:], lk.index)

	return buffer
}

type zSetInternalKey struct {
	key     []byte
	version int64
	score   float64
	member  []byte
}

/*
	+---------------+

key|version|member => |     score     |   (1)

	+---------------+
*/
func (zk *zSetInternalKey) encoderWithMember() []byte {
	buffer := make([]byte, len(zk.key)+len(zk.member)+8)
	index := 0
	copy(buffer[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	//version
	// 将int64 按照小端序存入字节切片
	binary.LittleEndian.PutUint64(buffer[index:index+8], uint64(zk.version))
	index += 8

	// member
	copy(buffer[index:], zk.member)

	return buffer
}

/*
	+---------------+

key|version|score|member|member size  => |      null     |   (2)

	+---------------+
*/
func (zk *zSetInternalKey) encoderWithScore() []byte {
	scoreBuf := utils.FloatToBytes(zk.score)
	buffer := make([]byte, len(zk.key)+len(zk.member)+len(scoreBuf)+8+4)
	index := 0
	copy(buffer[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	//version
	// 将int64 按照小端序存入字节切片
	binary.LittleEndian.PutUint64(buffer[index:index+8], uint64(zk.version))
	index += 8

	// score
	copy(buffer[index:index+len(scoreBuf)], scoreBuf)
	index += len(scoreBuf)

	// member
	copy(buffer[index:index+len(zk.member)], zk.member)
	index += len(zk.member)

	// member size
	binary.LittleEndian.PutUint32(buffer[index:], uint32(len(zk.member)))

	return buffer
}
