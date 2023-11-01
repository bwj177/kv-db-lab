package index

import (
	"github.com/stretchr/testify/assert"
	"kv-db-lab/model"
	"testing"
)

func TestRadixTree_Get(t *testing.T) {
	rTree := NewRadixTree()
	assert.NotNil(t, rTree)

	rTree.Put([]byte("key1"), &model.LogRecordPos{
		FileID: 1,
		Offset: 1,
	})

	pos := rTree.Get([]byte("key1"))
	assert.NotNil(t, pos)

	pos2 := rTree.Get([]byte("key2"))
	assert.Nil(t, pos2)

}
