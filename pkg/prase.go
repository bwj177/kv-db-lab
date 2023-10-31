package pkg

import "encoding/binary"

func PraseKey(key []byte) ([]byte, uint64) {
	transID, n := binary.Uvarint(key)
	k := key[n:]
	return k, transID
}

// LogRecordKeySeq key+transID编码
func LogRecordKeySeq(key []byte, transID uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)

	// 拿到小端序存储事务id的字节数组长度与字节数组seq便于编码
	n := binary.PutUvarint(seq, transID)

	// 拼接encKey
	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:])
	copy(encKey[n:], key)

	return encKey
}
