package redis

import (
	"encoding/binary"
	"kv-db-lab/constant"
	"kv-db-lab/pkg"
	"math"
	"time"
)

type metaData struct {
	dataType byte   // 数据类型
	expire   int64  // 过期时间
	version  int64  // 版本号
	size     uint32 //数据量
	head     uint64 // list数据结构的元数据
	tail     uint64 // list数据结构的元数据
}

const metaDataSize = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
const extraListDataSize = binary.MaxVarintLen64 * 2

const initialListMark uint64 = math.MaxUint64 / 2

func (md *metaData) encode() []byte {
	var size = metaDataSize
	if md.dataType == List {
		size += extraListDataSize
	}

	buf := make([]byte, size)
	buf[0] = md.dataType
	var idx = 1
	idx += binary.PutVarint(buf[idx:], md.expire)
	idx += binary.PutVarint(buf[idx:], md.version)
	idx += binary.PutVarint(buf[idx:], int64(md.size))

	if md.dataType == List {
		idx += binary.PutVarint(buf[idx:], int64(md.head))
		idx += binary.PutVarint(buf[idx:], int64(md.tail))
	}

	return buf[:idx]
}

func decodeMetaData(buf []byte) *metaData {
	dataType := buf[0]

	var idx = 1
	expire, n := binary.Varint(buf[idx:])
	idx += n

	version, n := binary.Varint(buf[idx:])
	idx += n

	size, n := binary.Varint(buf[idx:])
	idx += n

	var head, tail int64
	if dataType == List {
		head, n = binary.Varint(buf[idx:])
		idx += n
		tail, n = binary.Varint(buf[idx:])
	}

	return &metaData{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     uint64(head),
		tail:     uint64(tail),
	}
}

// findMetaData
//
//	@Description: 查找元数据信息
//	@receiver rds
//	@param key
//	@param metaDataType
//	@return *metaData
//	@return error
func (rds *RedisDataStructure) findMetaData(key []byte, metaDataType dataType) (*metaData, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && err != constant.ErrNotExist {
		return nil, err
	}

	var meta *metaData
	// 标识数据是否存在
	var exist = true

	if err == constant.ErrNotExist {
		exist = false
	} else {
		// 校验操作是否合法
		meta = decodeMetaData(metaBuf)
		if meta.dataType != metaDataType {
			return nil, constant.ErrWrongTypeOp
		}

		// 查看数据是否过期
		if meta.expire != 0 && meta.expire < time.Now().UnixNano() {
			exist = false
		}
	}

	// 如果数据不存在
	if !exist {
		meta = &metaData{
			dataType: metaDataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}

		// 若类型为list需要赋值头尾节点
		if metaDataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}

// hash数据结构存储到文件时的key
type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+len(hk.field)+8)
	// key
	var index = 0
	copy(buf[index:index+len(hk.key)], hk.key)
	index += len(hk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8

	// field
	copy(buf[index:], hk.field)

	return buf
}

// setInternalKey
// @Description: set数据类型的key结构
type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

func (sk *setInternalKey) encode() []byte {
	buf := make([]byte, len(sk.key)+len(sk.member)+8+4)
	// key
	var index = 0
	copy(buf[index:index+len(sk.key)], sk.key)
	index += len(sk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(sk.version))
	index += 8

	// member
	copy(buf[index:index+len(sk.member)], sk.member)
	index += len(sk.member)

	// member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(sk.member)))

	return buf
}

// listInternalKey
// @Description: list数据类型下的key结构
type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (lk *listInternalKey) encode() []byte {
	buf := make([]byte, len(lk.key)+8+8)

	// key
	var index = 0
	copy(buf[index:index+len(lk.key)], lk.key)
	index += len(lk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lk.version))
	index += 8

	// index
	binary.LittleEndian.PutUint64(buf[index:], lk.index)

	return buf
}

type ZsetInternalKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64
}

func (zk *ZsetInternalKey) encodeWithMember() []byte {
	buf := make([]byte, len(zk.key)+len(zk.member)+8)

	// key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	// member
	copy(buf[index:], zk.member)

	return buf
}

func (zk *ZsetInternalKey) encodeWithScore() []byte {
	scoreBuf := pkg.Float64ToBytes(zk.score)
	buf := make([]byte, len(zk.key)+len(zk.member)+len(scoreBuf)+8+4)

	// key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	// score
	copy(buf[index:index+len(scoreBuf)], scoreBuf)
	index += len(scoreBuf)

	// member
	copy(buf[index:index+len(zk.member)], zk.member)
	index += len(zk.member)

	// member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(zk.member)))

	return buf
}

// DecodeZsetWithScore (key)key |version | score | member |member size =>(value)NULL
func DecodeZsetWithScore(encKey []byte, zsetKey []byte) *ZsetInternalKey {
	//从version开始解码
	index := len(zsetKey)
	// 需要长度从右到左编码
	L := len(encKey)

	version := binary.LittleEndian.Uint64(encKey[index : index+8])
	index += 8

	rightIndex := L
	// memberSize
	memberSize := binary.LittleEndian.Uint32(encKey[rightIndex-4 : rightIndex])
	rightIndex -= 4

	// member
	member := encKey[rightIndex-int(memberSize) : rightIndex]
	rightIndex -= int(memberSize)

	// score
	score := encKey[index:rightIndex]

	return &ZsetInternalKey{
		key:     zsetKey,
		version: int64(version),
		member:  member,
		score:   pkg.FloatFromBytes(score),
	}
}
