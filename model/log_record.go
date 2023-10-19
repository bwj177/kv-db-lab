package model

type LogRecordPos struct {
	// 表述数据存储的文件ID
	FileID uint

	// 表示数据在该文件中偏移量
	Offset int64
}
