package index

import (
	"bytes"
	"github.com/google/btree"
	"kv-db-lab/constant"
	"kv-db-lab/model"
	"sort"
	"sync"
)

// BTree Btree 封装基于btree数据结构的索引结构
type BTree struct {
	// google开源 btree数据结构
	tree *btree.BTree

	// btree并发写非并发安全
	lock *sync.RWMutex
}

func (B *BTree) Put(key []byte, pos *model.LogRecordPos) bool {
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

func (B *BTree) Get(key []byte) *model.LogRecordPos {
	item := Item{
		key: key,
	}
	btreeItem := B.tree.Get(item)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(Item).pos
}

func (B *BTree) Delete(key []byte) bool {
	item := Item{
		key: key,
	}
	B.lock.Lock()
	defer B.lock.Unlock()
	btreeItem := B.tree.Delete(item)
	return btreeItem != nil
}

func (B *BTree) Size() int {
	return B.tree.Len()
}

// NewBTree
//
//	@Description: init btree
//	@param degree : b树叶子节点数量
//	@return *BTree
func NewBTree(degree int) *BTree {
	// 若传入degree不合法则则使用默认值
	if degree <= 0 {
		degree = constant.DefaultDegree
	}

	return &BTree{
		tree: btree.New(degree),
		lock: &sync.RWMutex{},
	}
}

func (B *BTree) Iterator(reverse bool) Iterator {
	if B.tree == nil {
		return nil
	}
	B.lock.RLock()
	defer B.lock.RUnlock()
	return newBtreeIterator(reverse, B.tree)
}

//================BTreeIterator================================

// BTreeIterator
//
//	BTreeIterator
//	@Description: 索引迭代器的实现
type BTreeIterator struct {
	// 当前遍历的下标位置
	currIndex int

	// 标识是否反序遍历
	reverse bool

	// key位置索引信息
	values []Item
}

func newBtreeIterator(reverse bool, tree *btree.BTree) *BTreeIterator {
	idx := 0
	values := make([]Item, tree.Len())

	saveValueFn := func(item btree.Item) bool {
		values[idx] = item.(Item)
		idx += 1
		return true
	}

	//在迭代时将数据存入values中
	if reverse {
		tree.Descend(saveValueFn)
	} else {
		tree.Ascend(saveValueFn)
	}

	return &BTreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (B *BTreeIterator) Rewind() {
	B.currIndex = 0
}

func (B *BTreeIterator) Seek(key []byte) {
	var start int
	if B.reverse {
		start = sort.Search(len(B.values), func(i int) bool {
			return bytes.Compare(B.values[i].key, key) <= 0
		})
	} else {
		start = sort.Search(len(B.values), func(i int) bool {
			return bytes.Compare(B.values[i].key, key) > 0
		})
	}
	B.currIndex = start
}

func (B *BTreeIterator) Next() {
	B.currIndex += 1
}

func (B *BTreeIterator) Valid() bool {
	return B.currIndex < len(B.values)
}

func (B *BTreeIterator) Key() []byte {
	return B.values[B.currIndex].key
}

func (B *BTreeIterator) Value() *model.LogRecordPos {
	return B.values[B.currIndex].pos
}

func (B *BTreeIterator) Close() {
	B.values = nil
}
