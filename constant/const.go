package constant

type LogRecordStatus byte

const (
	LogRecordNormal LogRecordStatus = 100 + iota
	LogRecordDelete
)

// DefaultFileSize 默认数据文件写入阈值
var DefaultFileSize int64 = 1024 * 1024

// DefaultDegree Btree默认Degree
var DefaultDegree int = 32

var DataFileSuffix string = ".data"
