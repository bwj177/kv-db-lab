package storage

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"kv-db-lab/model"
	"testing"
)

func TestIterate(t *testing.T) {
	opts := model.DefaultOptions

	db, err := OpenWithOptions(opts)
	assert.Nil(t, err)

	iter := db.NewIterate(model.DefaultIteratorOptions)
	assert.NotNil(t, iter)
	assert.Equal(t, false, iter.Valid())

	err2 := db.Put([]byte("key55"), []byte("value55"))
	assert.Nil(t, err2)

	iter2 := db.NewIterate(model.DefaultIteratorOptions)
	assert.NotNil(t, iter2)
	assert.Equal(t, true, iter2.Valid())

	v, err := db.Get([]byte("key55"))
	assert.Nil(t, err)
	fmt.Println(string(v))

}
