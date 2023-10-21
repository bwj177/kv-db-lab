package model

type Options struct {
	DirPath      string // 数据库文件目录
	DataFileSize int64  // 数据存放数据阈值
	SyncWrites   bool   // 写入数据是否需要持久化
}
