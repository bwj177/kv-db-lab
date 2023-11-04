package model

import (
	"encoding/binary"
	"hash/crc32"
	"kv-db-lab/constant"
)

// LogRecordPos 描述数据在磁盘中位置
type LogRecordPos struct {
	// 表述数据存储的文件ID
	FileID uint

	// 表示数据在该文件中偏移量
	Offset int64

	// 数据的大小
	Size int64
}

// LogRecord 数据记录格式
type LogRecord struct {
	Key    []byte
	Value  []byte
	Status constant.LogRecordStatus
}

// 事务Record数据
type TransRecord struct {
	LogRecord *LogRecord
	Pos       *LogRecordPos
}

// LogRecordHeader 数据记录头信息
type LogRecordHeader struct {
	crc        uint32                   // crc校验值
	recordType constant.LogRecordStatus // 标识record的类型
	keySize    uint32
	valueSize  uint32
}

// EncodeLogRecord 对LogRecord进行编码
/*
crc校验值 | type类型 | keySize | valueSize | key | value
  4          1          var       var       var   var    (byte)
*/
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	key := logRecord.Key
	value := logRecord.Value
	status := logRecord.Status

	// 先申请record可能的最大字节数
	header := make([]byte, constant.MaxLogRecordHeaderSize)

	// type已知，优先存储
	index := 4
	header[index] = byte(status)
	index += 1

	// 向[]byte依次写入可变长的字段,此方法返回写入数据长度-> index
	index += binary.PutVarint(header[index:], int64(len(key)))
	index += binary.PutVarint(header[index:], int64(len(value)))

	// size: 需要编码的header的总长度
	size := index + len(key) + len(value)

	encBytes := make([]byte, size)
	// 将header部分内容拷入encBytes
	copy(encBytes, header[:index])

	// 将kv拷入
	copy(encBytes[index:], key)
	copy(encBytes[index+len(key):], value)

	// crc校验码生产
	crc := crc32.ChecksumIEEE(encBytes[4:])

	// 小端序插入crc的值 -> crcBytes
	binary.LittleEndian.PutUint32(encBytes[:4], crc)
	return encBytes, int64(size)
}

// 对字节数组中header信息进行解码
func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) < 4 {
		return nil, 0
	}

	logRecordHeader := new(LogRecordHeader)
	logRecordHeader.crc = binary.LittleEndian.Uint32(buf[:4])
	logRecordHeader.recordType = constant.LogRecordStatus(buf[4])

	// 通过binary包中api将可变长的数据读出
	index := 5
	keySize, n := binary.Varint(buf[index:])
	index += n
	valueSize, n := binary.Varint(buf[index:])
	index += n

	logRecordHeader.keySize = uint32(keySize)
	logRecordHeader.valueSize = uint32(valueSize)

	return logRecordHeader, int64(index)
}

func getLogRecordCRC(record *LogRecord, header []byte) uint32 {
	if record == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[4:])
	crc = crc32.Update(crc, crc32.IEEETable, record.Key)
	crc = crc32.Update(crc, crc32.IEEETable, record.Value)

	return crc
}

// EncodeLogRecordPos
//
//	@Description: 对记录的位置信息进行编码
//	@param logRecordPos
//	@return []byte
func EncodeLogRecordPos(logRecordPos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64*2)

	var index int
	index += binary.PutVarint(buf, int64(logRecordPos.FileID))
	index += binary.PutVarint(buf, logRecordPos.Offset)
	index += binary.PutVarint(buf, logRecordPos.Size)
	return buf[:index]
}

// DecodeLogRecordPos
//
//	@Description: 对记录的位置信息进行解码
//	@param encByte
//	@return *LogRecordPos
func DecodeLogRecordPos(encByte []byte) *LogRecordPos {
	var index int
	fileID, size := binary.Varint(encByte[index:])

	index += size

	offset, size := binary.Varint(encByte[index:])
	index += size

	posSize, _ := binary.Varint(encByte[index:])
	return &LogRecordPos{
		FileID: uint(fileID),
		Offset: offset,
		Size:   posSize,
	}
}
