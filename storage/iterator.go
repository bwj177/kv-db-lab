package storage

import (
	"bytes"
	"kv-db-lab/index"
	"kv-db-lab/model"
)

// Iterate 对外(用户)使用的Iterate结构
type Iterate struct {
	indexIter index.Iterator
	engine    *Engine
	options   *model.IteratorOptions
}

func (it *Iterate) Rewind() {
	it.indexIter.Rewind()
	it.SkipToNext()
}

func (it *Iterate) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.SkipToNext()
}

func (it *Iterate) Next() {
	it.indexIter.Next()
	it.SkipToNext()
}

func (it *Iterate) Valid() bool {
	return it.indexIter.Valid()
}

func (it *Iterate) Key() []byte {
	return it.indexIter.Key()
}

// Value 不同于索引迭代器，这里的数据迭代器需要的值是实际存储数据而非pos
func (it *Iterate) Value() ([]byte, error) {
	pos := it.indexIter.Value()

	it.engine.lock.RLock()
	defer it.engine.lock.RUnlock()

	return it.engine.GetByRecordPos(pos)

}

// Close 释放values数组消耗的内存
func (it *Iterate) Close() {
	it.indexIter.Close()
}

// SkipToNext
//
//	@Description: 根据用户初始化传入的前缀，跳过不符合该前缀的key
//	@receiver it
func (it *Iterate) SkipToNext() {
	prefix := it.options.Prefix
	prefixLen := len(it.options.Prefix)
	key := it.Key()
	keyLen := len(it.Key())

	if prefix == nil {
		return
	}

	for ; it.Valid(); it.Next() {
		// 满足前缀则不需要跳过
		if keyLen >= prefixLen && bytes.Compare(key[:prefixLen], prefix) == 0 {
			break
		}
	}
}
