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

// IteratorOptions
//
//	IteratorOptions
//	@Description: 迭代器配置项
type IteratorOptions struct {
	// 指定索引迭代
	Prefix []byte

	// 顺序
	Reverse bool
}

var DefaultOptions = &Options{
	DirPath:      "./../test_file",
	DataFileSize: 1024 * 1024,
	SyncWrites:   true,
	Index:        0,
}

var DefaultIteratorOptions = &IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
