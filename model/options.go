package model

type Options struct {
	DirPath      string    // 数据库文件目录
	DataFileSize int64     // 数据存放数据阈值
	SyncWrites   bool      // 写入数据是否需要持久化
	Index        IndexType //
}

type IndexType = uint8

const (
	// Btree 如不传入默认0值使用Btree索引
	Btree IndexType = iota

	ART

	// ................

)
