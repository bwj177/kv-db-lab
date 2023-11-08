package redis

import (
	"errors"
)

func (rds *RedisDataStructure) Del(key []byte) error {
	return rds.db.Delete(key)
}

func (rds *RedisDataStructure) Type(key []byte) (dataType, error) {
	value, err := rds.db.Get(key)
	if err != nil {
		return 8, err
	}
	if value == nil {
		return 8, errors.New("value is empty")
	}

	return value[0], nil
}
