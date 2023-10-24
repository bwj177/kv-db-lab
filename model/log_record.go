package model

import "kv-db-lab/constant"

// LogRecordPos 描述数据在磁盘中位置
type LogRecordPos struct {
	// 表述数据存储的文件ID
	FileID uint

	// 表示数据在该文件中偏移量
	Offset int64
}

// LogRecord 数据记录格式
type LogRecord struct {
	Key    []byte
	Value  []byte
	Status constant.LogRecordStatus
}

// LogRecordHeader 数据记录头信息
type LogRecordHeader struct {
	crc        uint32                   // crc校验值
	recordType constant.LogRecordStatus // 标识record的类型
	keySize    uint32
	valueSize  uint32
}

// EncodeLogRecord 对LogRecord进行编码byte字节
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

// 对字节数组中header信息进行解码
func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	return nil, 0
}

func getLogRecordCRC(record LogRecord, buf []byte) uint32 {
	return 0
}
