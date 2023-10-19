package index

import (
	"github.com/google/btree"
	"kv-db-lab/model"
	"sync"
)

// BTree Btree 封装基于btree数据结构的索引结构
type BTree struct {
	// google开源 btree数据结构
	tree *btree.BTree

	// btree并发写非并发安全
	lock *sync.RWMutex
}

func (B BTree) Put(key []byte, pos *model.LogRecordPos) bool {
	item := Item{
		key: key,
		pos: pos,
	}
	B.lock.Lock()
	defer B.lock.Unlock()

	// ReplaceOrInsert: 已存在时替换并返回原有值，否则返回空值
	B.tree.ReplaceOrInsert(item)
	return true
}

func (B BTree) Get(key []byte) *model.LogRecordPos {
	item := Item{
		key: key,
	}
	btreeItem := B.tree.Get(item)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(Item).pos
}

func (B BTree) Delete(key []byte) bool {
	item := Item{
		key: key,
	}
	B.lock.Lock()
	defer B.lock.Unlock()
	btreeItem := B.tree.Delete(item)
	return btreeItem != nil
}

// NewBTree
//
//	@Description: init btree
//	@param degree : b树叶子节点数量
//	@return *BTree
func NewBTree(degree int) *BTree {
	return &BTree{
		tree: btree.New(degree),
		lock: &sync.RWMutex{},
	}
}
