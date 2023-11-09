package redis

import (
	"encoding/binary"
	"fmt"
	"kv-db-lab/constant"
	"time"
)

//============================String============================

// Set

// value格式： type | expireTime | value

//

//	1byte    0~64byte  len(value)

func (rds *RedisDataStructure) Set(key []byte, value []byte, ttl time.Duration) error {

	if value == nil {

		return nil

	}

	// 编码value：type+expire+payload  int64+1 ===》 expiration+type

	buf := make([]byte, binary.MaxVarintLen64+1)

	buf[0] = String

	var index = 1

	// 编码过期时间写入byte

	var expireTime int64 = 0

	if ttl != 0 {

		expireTime = time.Now().Add(ttl).UnixNano()

	}

	index += binary.PutVarint(buf[index:], expireTime)

	encValue := make([]byte, index+len(value))

	copy(encValue[:index], buf[:index])

	copy(encValue[index:], value)

	// 调用存储接口进行写入

	return rds.db.Put(key, encValue)

}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {

	encValue, err := rds.db.Get(key)

	if err != nil {

		return nil, err

	}

	// 对encValue进行解码

	dataType := encValue[0]

	fmt.Println(key)

	if dataType != String {

		return nil, constant.ErrWrongTypeOp

	}

	var index = 1

	expireTime, n := binary.Varint(encValue[index:])

	index += n

	// 若数据已过期

	if expireTime != 0 && expireTime < time.Now().UnixNano() {

		err := rds.db.Delete(key)

		if err != nil {

			return nil, err

		}

		return nil, constant.ErrExpireTime

	}

	value := encValue[index:]

	return value, nil

}
