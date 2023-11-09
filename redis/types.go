package redis

import (
	"kv-db-lab/constant"
	"kv-db-lab/storage"
)

type dataType = byte

const (
	String dataType = iota
	List
	Hash
	Set
	Zset
)

// RedisDataStructure
//
//	@Description: 对redis常用数据结构以及相应API进行实现
type RedisDataStructure struct {
	db *storage.Engine
}

func NewRedisDateStructure(db *storage.Engine) (*RedisDataStructure, error) {
	if db == nil {
		return nil, constant.ErrEmptyParam
	}
	return &RedisDataStructure{db: db}, nil
}

func (rds *RedisDataStructure) Close() error {
	return rds.db.Close()
}
