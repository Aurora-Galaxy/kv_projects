package redis

import (
	"encoding/binary"
	"kv_projects/conf"
	"kv_projects/db"
	"kv_projects/errs"
	"kv_projects/utils"
	"time"
)

type RedisType = byte

// 定义和 redis 类似的结构
const (
	String RedisType = iota + 1
	Hash
	Set
	List
	ZSet
)

type RedisDataStructure struct {
	db *db.DB
}

/**
 * NewRedisDataDb
 * @Description: 初始化 redis 数据结构服务
 * @param opt
 * @return *RedisDataStructure
 * @return error
 */
func NewRedisDataStructure(opt conf.Options) (*RedisDataStructure, error) {
	Db, err := db.Open(opt)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{db: Db}, nil
}

// 关闭数据库连接
func (rds *RedisDataStructure) Close() error {
	return rds.db.Close()
}

// ============================String 数据结构================================
// String 类型
//
//	+----------+------------+--------------------+
//
// key =>  |   type   |  expire    |       payload      |
//
//	       | (1byte)  | (Ebyte)    |       (Nbyte)      |
//	+----------+------------+--------------------+
func (rds *RedisDataStructure) Set(key, value []byte, ttl time.Duration) error {
	if value == nil {
		return nil
	}

	// 将type ， ttl ， value进行编码
	buffer := make([]byte, binary.MaxVarintLen64+1)
	// 第一个byte存储类型
	buffer[0] = String
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		// 数据过期时间
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buffer[index:], expire)
	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buffer[:index])
	copy(encValue[index:], value)

	//调用接口将编码后的数据存入数据库
	return rds.db.Put(key, encValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	res, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}
	if res[0] != String {
		return nil, errs.ErrWrongOperationType
	}
	var index = 1
	expire, n := binary.Varint(res[index:])
	index += n
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, errs.ErrDataExpired
	}
	return res[index:], nil
}

// ============================Hash 数据结构================================
// Hash 类型
/*
        +----------+------------+-----------+-----------+
key =>  |   type   |  expire    |  version  |  size     |
        | (1byte)  | (8byte)    |  (8byte)  | (Sbyte)   |
        +----------+------------+-----------+-----------+
size 代表此 key 下有多少条数据
                     +---------------+
key|version|field => |     value     |
                     +---------------+
*/
func (rds *RedisDataStructure) HSet(key, filed, value []byte) (bool, error) {
	// 先查找元数据
	meta, err := rds.FindMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	// 构造 hashInternalKey
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   filed,
	}
	// 将 hashInternalKey 进行编码
	encKey := hk.encoder()

	var exist = true
	if _, err := rds.db.Get(encKey); err == errs.ErrKeyNotFound {
		exist = false
	}

	wb := rds.db.NewWriteBatch(&conf.DefaultWriteBatchOptions)
	// 如果不存在则更新元数据
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encoderMetadata())
	}
	_ = wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	// 更新数据会返回 false
	return !exist, nil

}

func (rds *RedisDataStructure) HGet(key, filed []byte) ([]byte, error) {
	// 先查找元数据
	meta, err := rds.FindMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	// key 中没有数据
	if meta.size == 0 {
		return nil, nil
	}
	// 构造 hashInternalKey
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   filed,
	}
	// 根据编码后的key查找对应数据
	return rds.db.Get(hk.encoder())
}

func (rds *RedisDataStructure) HDel(key, filed []byte) (bool, error) {
	// 先查找元数据
	meta, err := rds.FindMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	// key 中没有数据
	if meta.size == 0 {
		return false, nil
	}
	// 构造 hashInternalKey
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   filed,
	}
	// 编码
	encKey := hk.encoder()

	// 判断编码后的key是否存在
	var exist = true
	if _, err = rds.db.Get(encKey); err == errs.ErrKeyNotFound {
		exist = false
	}
	// 存在即删除并修改元数据
	if exist {
		wb := rds.db.NewWriteBatch(&conf.DefaultWriteBatchOptions)
		meta.size--
		// 修改元数据
		_ = wb.Put(key, meta.encoderMetadata())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}
	return exist, nil
}

// ============================Set 数据结构================================
// Set 类型
/*
        +----------+------------+-----------+-----------+
key =>  |   type   |  expire    |  version  |  size     |
        | (1byte)  | (Ebyte)    |  (8byte)  | (Sbyte)   |
        +----------+------------+-----------+-----------+

                                  +---------------+
key|version|member|member size => |     NULL      |
                                  +---------------+
*/
func (rds *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
	// 先查找元数据
	meta, err := rds.FindMetadata(key, Set)
	if err != nil {
		return false, err
	}

	// 构造 setInternalKey
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	// 将 setInternalKey 进行编码
	encKey := sk.encoder()

	var ok bool
	if _, err := rds.db.Get(encKey); err == errs.ErrKeyNotFound {
		wb := rds.db.NewWriteBatch(&conf.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encoderMetadata())
		_ = wb.Put(encKey, nil)
		if err := wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}
	return ok, nil
}

// 判断 member 是不是属于 key 的member
func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {
	meta, err := rds.FindMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	// 构造 setInternalKey
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	encKey := sk.encoder()
	_, err = rds.db.Get(encKey)
	if err != nil && err != errs.ErrKeyNotFound {
		return false, err
	}
	if err == errs.ErrKeyNotFound {
		return false, nil
	}
	return true, nil
}

// 删除 key 中的 member
func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {
	meta, err := rds.FindMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	// 构造 setInternalKey
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	encKey := sk.encoder()

	if _, err = rds.db.Get(encKey); err == errs.ErrKeyNotFound {
		return false, nil
	}
	// 修改元数据
	wb := rds.db.NewWriteBatch(&conf.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encoderMetadata())
	_ = wb.Delete(encKey)
	if err := wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// ============================List 数据结构================================
// List 类型
/*
	+----------+------------+-----------+-----------+-----------+-----------+

key =>  |   type   |  expire    |  version  |  size     |  head     |  tail     |

	| (1byte)  | (Ebyte)    |  (8byte)  | (Sbyte)   | (8byte)   | (8byte)   |
	+----------+------------+-----------+-----------+-----------+-----------+
    +---------------+

key|version|index => |     value     |

	+---------------+
*/
func (rds *RedisDataStructure) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

func (rds *RedisDataStructure) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

func (rds *RedisDataStructure) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

func (rds *RedisDataStructure) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

/**
 * pushInner
 * @Description: 插入数据
 * @receiver rds
 * @param key
 * @param element
 * @param isLeft  true 则从左边插入，false从右边插入
 * @return uint32 当前 key 对应 list 的长度
 * @return error
 */
func (rds *RedisDataStructure) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	// 查找元数据
	meta, err := rds.FindMetadata(key, List)
	if err != nil {
		return 0, err
	}
	// 构造 listInternalKey
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		// head 存储数据就在 head 当前位置
		lk.index = meta.head - 1
	} else {
		// tail 存放数据后，tail 会向后移动 1 位，及数据存储在 tail 的前 1 位
		lk.index = meta.tail
	}

	// 更新元数据和数据部分
	wb := rds.db.NewWriteBatch(&conf.DefaultWriteBatchOptions)
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	_ = wb.Put(key, meta.encoderMetadata())
	_ = wb.Put(lk.encoder(), element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}

	return meta.size, nil
}

func (rds *RedisDataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {
	// 查找元数据
	meta, err := rds.FindMetadata(key, List)
	if err != nil {
		return nil, err
	}

	if meta.size == 0 {
		return nil, nil
	}

	// 构造 listInternalKey
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}

	// 更新元数据和数据部分
	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}

	// 获取弹出的元素
	element, err := rds.db.Get(lk.encoder())
	if err != nil {
		return nil, err
	}
	// 更新元数据
	err = rds.db.Put(key, meta.encoderMetadata())
	if err != nil {
		return nil, err
	}
	return element, nil
}

// ============================ZSet 数据结构================================
/*
        +----------+------------+-----------+-----------+
key =>  |   type   |  expire    |  version  |  size     |
        | (1byte)  | (Ebyte)    |  (8byte)  | (Sbyte)   |
        +----------+------------+-----------+-----------+
                      +---------------+
key|version|member => |     score     |   (1)
                      +---------------+

                                         +---------------+
key|version|score|member|member size  => |      null     |   (2)
                                         +---------------+
*/

func (rds *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	// 查找元数据
	meta, err := rds.FindMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	// 构造 zSetInternalKey
	zk := &zSetInternalKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}

	// 查看是否已经存在
	var exist = true
	val, err := rds.db.Get(zk.encoderWithMember())
	if err != nil && err != errs.ErrKeyNotFound {
		return false, err
	}
	if err == errs.ErrKeyNotFound {
		exist = false
	}
	// key 存在且 score 和原数据相同，没有更新，依旧返回false
	if exist {
		if score == utils.FloatFromBytes(val) {
			return false, nil
		}
	}

	wb := rds.db.NewWriteBatch(&conf.DefaultWriteBatchOptions)
	// 不存在则更新元数据和数据部分
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encoderMetadata())
	}
	if exist {
		// 更新 score 的情况，先将原本数据删除
		oldKey := &zSetInternalKey{
			key:     key,
			version: meta.version,
			score:   utils.FloatFromBytes(val),
			member:  nil,
		}
		_ = wb.Delete(oldKey.encoderWithMember())
	}
	_ = wb.Put(zk.encoderWithMember(), utils.FloatToBytes(score))
	_ = wb.Put(zk.encoderWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *RedisDataStructure) ZScore(key, member []byte) (float64, error) {
	// 查找元数据
	meta, err := rds.FindMetadata(key, ZSet)
	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, err
	}
	// 构造 zSetInternalKey
	zk := &zSetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	scoreBytes, err := rds.db.Get(zk.encoderWithMember())
	if err != nil {
		return -1, err
	}

	return utils.FloatFromBytes(scoreBytes), nil
}
