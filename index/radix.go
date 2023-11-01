package index

import (
	"bytes"
	rdx "github.com/plar/go-adaptive-radix-tree"
	"kv-db-lab/model"
	"sort"
	"sync"
)

// RadixTree 基数数存储内存索引的实现
type RadixTree struct {
	tree rdx.Tree
	lock *sync.RWMutex
}

func NewRadixTree() *RadixTree {
	return &RadixTree{
		tree: rdx.New(),
		lock: new(sync.RWMutex),
	}
}

func (r *RadixTree) Put(key []byte, pos *model.LogRecordPos) bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.tree.Insert(key, pos)
	return true
}

func (r *RadixTree) Get(key []byte) *model.LogRecordPos {
	r.lock.RLock()
	defer r.lock.RUnlock()
	pos, isFound := r.tree.Search(key)
	if isFound == false {
		return nil
	}
	return pos.(*model.LogRecordPos)
}

func (r *RadixTree) Delete(key []byte) bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	_, deleted := r.tree.Delete(key)
	return deleted
}

func (r *RadixTree) Iterator(reverse bool) Iterator {
	if r.tree == nil {
		return nil
	}
	r.lock.RLock()
	defer r.lock.RUnlock()
	return newRadixIterator(reverse, r.tree)

}

func (r *RadixTree) Size() int {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.tree.Size()
}

//================RadixIterator=================================================

// RadixIterator
//
//	RadixIterator
//	@Description: 索引迭代器的实现
type RadixIterator struct {
	// 当前遍历的下标位置
	currIndex int

	// 标识是否反序遍历
	reverse bool

	// key位置索引信息
	values []*Item
}

func newRadixIterator(reverse bool, tree rdx.Tree) *RadixIterator {
	// 顺序存放index
	idx := 0

	// 将内存索引数据全存入自定义迭代数组中进行迭代
	values := make([]*Item, tree.Size())

	// 逆序遍历
	if reverse {
		idx = tree.Size() - 1
	}

	// 在radix顺序遍历的API中传入的回调函数
	saveValuesFn := func(node rdx.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*model.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx -= 1
		} else {
			idx += 1
		}
		return true
	}

	tree.ForEach(saveValuesFn)

	return &RadixIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (r *RadixIterator) Rewind() {
	r.currIndex = 0
}

func (r *RadixIterator) Seek(key []byte) {
	var start int
	if r.reverse {
		start = sort.Search(len(r.values), func(i int) bool {
			return bytes.Compare(r.values[i].key, key) <= 0
		})
	} else {
		start = sort.Search(len(r.values), func(i int) bool {
			return bytes.Compare(r.values[i].key, key) > 0
		})
	}
	r.currIndex = start
}

func (r *RadixIterator) Next() {
	r.currIndex += 1
}

func (r *RadixIterator) Valid() bool {
	return r.currIndex < len(r.values)
}

func (r *RadixIterator) Key() []byte {
	return r.values[r.currIndex].key
}

func (r *RadixIterator) Value() *model.LogRecordPos {
	return r.values[r.currIndex].pos
}

func (r *RadixIterator) Close() {
	r.values = nil
}
