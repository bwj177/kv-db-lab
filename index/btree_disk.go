package index

import (
	"go.etcd.io/bbolt"
	"kv-db-lab/constant"
	"kv-db-lab/model"
	"path/filepath"
)

// BPlusTree
// @Description: 封装了基于b+树存储结构将索引存储在磁盘中，规避数据量过大，索引数量大于内存
type BPlusTree struct {
	tree *bbolt.DB
}

// NewBPlusTree 初始化b+TreeDB
func NewBPlusTree(dirPath string) *BPlusTree {
	db, err := bbolt.Open(filepath.Join(dirPath, constant.BPlusIndexName), constant.DefaultFileMode, nil)
	if err != nil {
		panic("failed open the bboltDB")
	}

	// 创建对应的分区
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(constant.DefaultIndexBucketName))
		return err
	}); err != nil {
		panic("failed create index bucket")
	}
	return &BPlusTree{tree: db}
}

func (bpt *BPlusTree) Put(key []byte, pos *model.LogRecordPos) bool {
	// update实际就是一个更新的事务，一并执行update、delete、put等操作，保证原子性
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(constant.DefaultIndexBucketName))
		return bucket.Put(key, model.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put index to b+TreeIndex")
	}
	return true
}

func (bpt *BPlusTree) Get(key []byte) *model.LogRecordPos {
	var pos *model.LogRecordPos
	// view与update类似，只读
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(constant.DefaultIndexBucketName))
		posByte := bucket.Get(key)
		if len(posByte) != 0 {
			pos = model.DecodeLogRecordPos(posByte)
		}
		return nil
	}); err != nil {
		panic("failed read index from b+Tree")
	}

	return pos
}

func (bpt *BPlusTree) Delete(key []byte) bool {
	// update实际就是一个更新的事务，一并执行update、delete、put等操作，保证原子性
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(constant.DefaultIndexBucketName))
		return bucket.Delete(key)
	}); err != nil {
		panic("failed to delete index to b+TreeIndex")
	}
	return true
}

func (bpt *BPlusTree) Size() int {
	var size int
	// view与update类似，只读
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(constant.DefaultIndexBucketName))
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed read key size from b+Tree")
	}
	return size
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bpt.tree, reverse)
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

// ==============================索引迭代器================================================================
// B+树迭代器
type bptreeIterator struct {
	tx        *bbolt.Tx
	cursor    *bbolt.Cursor
	reverse   bool
	currKey   []byte
	currValue []byte
}

func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}
	bpi := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket([]byte(constant.DefaultIndexBucketName)).Cursor(),
		reverse: reverse,
	}
	bpi.Rewind()
	return bpi
}

func (bpi *bptreeIterator) Rewind() {
	if bpi.reverse {
		bpi.currKey, bpi.currValue = bpi.cursor.Last()
	} else {
		bpi.currKey, bpi.currValue = bpi.cursor.First()
	}
}

func (bpi *bptreeIterator) Seek(key []byte) {
	bpi.currKey, bpi.currValue = bpi.cursor.Seek(key)
}

func (bpi *bptreeIterator) Next() {
	if bpi.reverse {
		bpi.currKey, bpi.currValue = bpi.cursor.Prev()
	} else {
		bpi.currKey, bpi.currValue = bpi.cursor.Next()
	}
}

func (bpi *bptreeIterator) Valid() bool {
	return len(bpi.currKey) != 0
}

func (bpi *bptreeIterator) Key() []byte {
	return bpi.currKey
}

func (bpi *bptreeIterator) Value() *model.LogRecordPos {
	return model.DecodeLogRecordPos(bpi.currValue)
}

func (bpi *bptreeIterator) Close() {
	_ = bpi.tx.Rollback()
}
