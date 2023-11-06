package storage

import (
	"github.com/stretchr/testify/assert"
	"kv-db-lab/model"
	"strconv"
	"testing"
)

func TestWriteBatch(t *testing.T) {
	db, err := OpenWithOptions(model.DefaultOptions)
	assert.Nil(t, err)

	wb := db.NewWriteBatch(model.DefaultWriteBatchOptions)
	err = wb.Put([]byte("k11"), []byte("v22"))
	assert.Nil(t, err)
	err = wb.Delete([]byte("k11"))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	key, err := db.Get([]byte("k11"))
	assert.NotNil(t, err)
	assert.Nil(t, key)
}

func TestWriteBatch_Commit(t *testing.T) {
	db, err := OpenWithOptions(model.DefaultOptions)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb := db.NewWriteBatch(model.DefaultWriteBatchOptions)
	for i := 0; i < 100; i++ {
		err = wb.Put([]byte("255344"+strconv.Itoa(i)), []byte(strconv.Itoa(i)))
		assert.Nil(t, err)
	}
	err = wb.Commit()
	assert.Nil(t, err)

	assert.Nil(t, err)

	err4 := db.Merge()
	assert.Nil(t, err4)
	err11 := db.Close()
	assert.Nil(t, err11)
	t.Logf("%#v", db.Stat())

	db, err = OpenWithOptions(model.DefaultOptions)

	assert.Nil(t, err)
	assert.NotNil(t, db)
}
