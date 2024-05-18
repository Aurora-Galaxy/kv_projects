package redis

import (
	"kv_projects/errs"
	"time"
)

/**
 * FindMetadata
 * @Description: 查找 key 对应的元数据,存在直接解码返回，不存在则初始化
 * @receiver rds
 * @param key
 * @param dataType
 * @return *MetaData
 * @return error
 */
func (rds *RedisDataStructure) FindMetadata(key []byte, dataType RedisType) (*MetaData, error) {
	metaBuffer, err := rds.db.Get(key)
	if err != nil && err != errs.ErrKeyNotFound {
		return nil, err
	}
	var meta *MetaData
	exist := true
	if err == errs.ErrKeyNotFound {
		exist = false
	} else {
		meta = decoderMetadata(metaBuffer)
		// 判断数据类型
		if meta.dataType != dataType {
			return nil, errs.ErrWrongOperationType
		}
		// 判断数据是否过期
		if meta.expire > 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}
	// key 对应内容不存在，构建原始metaData
	if !exist {
		meta = &MetaData{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMask
			meta.tail = initialListMask
		}
	}
	return meta, nil
}
