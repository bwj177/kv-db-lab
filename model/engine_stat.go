package model

// EngineStat
//
//	EngineStat
//	@Description: 存储kv引擎中一些指标信息
type EngineStat struct {
	KeyNum          uint  // 存储key数量
	DateFileNum     uint  // 存储文件数量
	ReclaimableSize int64 // 可回收的无效数据大小
	DiskSize        int64 // 引擎目录下所有文件占用的内存大小
}
