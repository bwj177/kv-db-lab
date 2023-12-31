package model

import "kv-db-lab/constant"

type Options struct {
	DirPath            string    // 数据库文件目录
	DataFileSize       int64     // 数据存放数据阈值
	SyncWrites         bool      // 写入数据是否需要持久化
	Index              IndexType // 文件IO类型，主要区分启动load时的索引类型
	DateFileMergeRatio float32   //标识无效数据的阈值，超过阈值才允许进行merge，否则不允许，merge过于频繁影响性能
}

type IndexType = uint8

const (
	// Btree 如不传入默认0值使用Btree索引
	Btree IndexType = iota

	ART

	BPlusTree

	// ................

)

// IteratorOptions
//
//	@Description: 迭代器配置项
type IteratorOptions struct {
	// 指定索引迭代
	Prefix []byte

	// 顺序
	Reverse bool
}

// WriteBatchOptions
//
//	@Description: 批量写入配置项
type WriteBatchOptions struct {
	// 是否自动持久化数据到磁盘
	SyncWrite bool

	// 一个批次的最大写入量
	MaxBatchSize uint
}

var DefaultOptions = &Options{
	DirPath:            "./../test_file",
	DataFileSize:       1024 * 1024,
	SyncWrites:         true,
	Index:              0,
	DateFileMergeRatio: constant.DefaultMergeRatio,
}

var DefaultIteratorOptions = &IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = &WriteBatchOptions{
	SyncWrite:    true,
	MaxBatchSize: 1000,
}
