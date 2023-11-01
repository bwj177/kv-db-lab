package constant

import "encoding/binary"

type LogRecordStatus byte

const (
	LogRecordNormal LogRecordStatus = 100 + iota
	LogRecordDelete
)

// DefaultFileMode 默认创建文件的权限
const DefaultFileMode = 0644
const DefaultDirMode = 0755

// DefaultFileSize 默认数据文件写入阈值
var DefaultFileSize int64 = 1024 * 1024

// DefaultDegree Btree默认Degree
var DefaultDegree int = 32

var DataFileSuffix string = ".data"

// MaxLogRecordHeaderSize size = crc + type + keySize +valueSize
const MaxLogRecordHeaderSize int64 = 4 + 1 + binary.MaxVarintLen32 + binary.MaxVarintLen32

var TxFinKey = []byte("finishedTx")

const NoneTransactionID = 0

// MergeSuffix 用于merge文件的命名后缀
const MergeSuffix = "-merge"

// HintFileName 用于hint文件的命名
const HintFileName = "hint-index"

// MergeFinishedName 用于标识merge成功文件的文件命名
const MergeFinishedName = "merge-finished"

// NowTxIDFileName 标识 记录close时事务ID的文件名
const NowTxIDFileName = "txID-Now"

// MergeFinishedKey 用于 命名merge成功文件标识写入的key
const MergeFinishedKey = "MERGE.FINISHED"

const BPlusIndexName = "BPlusTree"

const DefaultIndexBucketName = "default-bucket"
