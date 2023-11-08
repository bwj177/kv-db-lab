package redis

import (
	"kv-db-lab/constant"
	"kv-db-lab/model"
	"kv-db-lab/pkg"
)

// ======================= ZSet 数据结构 =======================

func (rds *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Zset)
	if err != nil {
		return false, err
	}

	// 构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}

	var exist = true
	// 查看是否已经存在
	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil && err != constant.ErrNotExist {
		return false, err
	}
	if err == constant.ErrNotExist {
		exist = false
	}
	if exist {
		if score == pkg.FloatFromBytes(value) {
			return false, nil
		}
	}

	// 更新元数据和数据
	wb := rds.db.NewWriteBatch(model.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	if exist {
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   pkg.FloatFromBytes(value),
		}
		_ = wb.Delete(oldKey.encodeWithScore())
	}
	_ = wb.Put(zk.encodeWithMember(), pkg.Float64ToBytes(score))
	_ = wb.Put(zk.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (rds *RedisDataStructure) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetaData(key, Zset)
	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, nil
	}

	// 构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return pkg.FloatFromBytes(value), nil
}
