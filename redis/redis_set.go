package redis

import (
	"kv-db-lab/constant"
	"kv-db-lab/model"
)

// ======================= Set 数据结构 =================================================================

func (rds *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
	// 查找元数据
	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return false, err
	}

	// 构造一个数据部分的 key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	var ok bool
	if _, err = rds.db.Get(sk.encode()); err == constant.ErrNotExist {
		// 不存在的话则更新
		wb := rds.db.NewWriteBatch(model.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(sk.encode(), nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}

	return ok, nil
}

func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	// 构造一个数据部分的 key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = rds.db.Get(sk.encode())
	if err != nil && err != constant.ErrNotExist {
		return false, err
	}
	if err == constant.ErrNotExist {
		return false, nil
	}
	return true, nil
}

func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	// 构造一个数据部分的 key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	if _, err = rds.db.Get(sk.encode()); err == constant.ErrNotExist {
		return false, nil
	}

	// 更新
	wb := rds.db.NewWriteBatch(model.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}
