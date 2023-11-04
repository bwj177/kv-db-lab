package index

import (
	"bytes"
	"github.com/google/btree"
	"kv-db-lab/constant"
	"kv-db-lab/model"
)

// Indexer
// @Description: 索引存储公用接口
type Indexer interface {
	Put(key []byte, pos *model.LogRecordPos) *model.LogRecordPos
	Get(key []byte) *model.LogRecordPos
	Delete(key []byte) *model.LogRecordPos

	Iterator(reverse bool) Iterator

	// Size 返回Btree存储数据数量
	Size() int

	Close() error
}

func NewIndexer(tp model.IndexType, dirPath string) Indexer {
	switch tp {
	case model.Btree:
		// 使用该方式则使用默认节点数
		return NewBTree(constant.DefaultDegree)
	case model.ART:
		return NewRadixTree()
	case model.BPlusTree:
		return NewBPlusTree(dirPath)
	default:
		return nil
	}

}

// Item 实现google-btree中item接口
type Item struct {
	key []byte
	pos *model.LogRecordPos
}

// Less 排序比较规则定义
func (i Item) Less(than btree.Item) bool {
	return bytes.Compare(i.key, than.(Item).key) == -1
}

// Iterator
// @Description: 索引迭代器接口
type Iterator interface {
	// Rewind rewind 重新回到迭代器的起点
	Rewind()

	// Seek 根据传入key找到第一个大于等于key的目标key，从此key开始遍历
	Seek(key []byte)

	// Next 迭代到下一个key
	Next()

	// Valid 是否已经遍历完了所以key
	Valid() bool

	// Key 当前遍历位置的key数据
	Key() []byte

	// Value 当前遍历位置的Value数据
	Value() *model.LogRecordPos

	// Close 关闭迭代器
	Close()
}
