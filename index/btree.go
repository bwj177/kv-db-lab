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

func (B *BTree) Put(key []byte, pos *model.LogRecordPos) *model.LogRecordPos {
	item := Item{
		key: key,
		pos: pos,
	}
	B.lock.Lock()
	defer B.lock.Unlock()

	// ReplaceOrInsert: 已存在时替换并返回原有值，否则返回空值
	oldItem := B.tree.ReplaceOrInsert(item)
	if oldItem == nil {
		return nil
	}

	oldPos := oldItem.(Item).pos
	return oldPos
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

func (B *BTree) Delete(key []byte) *model.LogRecordPos {
	item := Item{
		key: key,
	}
	B.lock.Lock()
	defer B.lock.Unlock()
	btreeItem := B.tree.Delete(item)

	if btreeItem == nil {
		return nil
	}

	return btreeItem.(Item).pos
}

func (B *BTree) Size() int {
	return B.tree.Len()
}

func (B *BTree) Close() error {
	return nil
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
	CurrIndex int

	// 标识是否反序遍历
	Reverse bool

	// key位置索引信息
	Values []Item
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
		CurrIndex: 0,
		Reverse:   reverse,
		Values:    values,
	}
}

func (B *BTreeIterator) Rewind() {
	B.CurrIndex = 0
}

func (B *BTreeIterator) Seek(key []byte) {
	var start int
	if B.Reverse {
		start = sort.Search(len(B.Values), func(i int) bool {
			return bytes.Compare(B.Values[i].key, key) <= 0
		})
	} else {
		start = sort.Search(len(B.Values), func(i int) bool {
			return bytes.Compare(B.Values[i].key, key) > 0
		})
	}
	B.CurrIndex = start
}

func (B *BTreeIterator) Next() {
	B.CurrIndex += 1
}

func (B *BTreeIterator) Valid() bool {
	return B.CurrIndex < len(B.Values)
}

func (B *BTreeIterator) Key() []byte {
	return B.Values[B.CurrIndex].key
}

func (B *BTreeIterator) Value() *model.LogRecordPos {
	return B.Values[B.CurrIndex].pos
}

func (B *BTreeIterator) Close() {
	B.Values = nil
}

// ==================Zset Iterator =================

//func NewZSetIterator(tree *btree.BTree, suffix []byte, zsetKey []byte) *BTreeIterator {
//	idx := 0
//	values := make([]Item, tree.Len())
//
//	saveValueFn := func(item btree.Item) bool {
//		enckey := item.(Item).key
//		// 对encKey进行编码，取出对应字段的值
//		zsetInternalKey := redis.DecodeZsetWithScore(enckey, zsetKey)
//
//		// 判断是否为key+version的前缀
//		if item.(Item).pos==nil && zsetInternalKey
//		if string(key[:len(zsetKey)+8]) == string(suffix) {
//			// 查看是否为存有score的数据部分,如果有则该key为需要的部分
//			if item.(Item).pos == nil {
//				// 取出enckey中的score和key
//				binary.LittleEndian.Uint32(buf[:4])
//			}
//		}
//
//		values[idx] = item.(Item)
//		idx += 1
//		return true
//	}
//
//	//在迭代时将数据存入values中,倒叙插入方便实现ZPopmax
//	tree.Descend(saveValueFn)
//
//	return &BTreeIterator{
//		CurrIndex: 0,
//		Reverse:   true,
//		Values:    values,
//	}
//}
