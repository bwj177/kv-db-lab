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

// EncodeLogRecord 对LogRecord进行编码byte字节
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}
