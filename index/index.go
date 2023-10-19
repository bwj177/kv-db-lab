package index

import (
	"bytes"
	"github.com/google/btree"
	"kv-db-lab/model"
)

// Indexer
// @Description: 索引存储公用接口
type Indexer interface {
	Put(key []byte, pos *model.LogRecordPos) bool
	Get(key []byte) *model.LogRecordPos
	Delete(key []byte) bool
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
