package redis

import "kv_projects/errs"

//添加通用命令 delete 和 type

/**
 * Delete
 * @Description: 通用方法，删除 key 对应的内容
 * @receiver rds
 * @param key
 * @return error
 */
func (rds *RedisDataStructure) Delete(key []byte) error {
	return rds.db.Delete(key)
}

/**
 * Type
 * @Description: 获取当前 key 对应数据的类型
 * @receiver rds
 * @param key
 * @return RedisType
 * @return error
 */
func (rds *RedisDataStructure) Type(key []byte) (RedisType, error) {
	res, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(res) == 0 {
		return 0, errs.ErrValueIsNull
	}
	return res[0], nil
}
