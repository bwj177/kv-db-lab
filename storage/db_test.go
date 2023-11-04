package storage

import (
	"github.com/stretchr/testify/assert"
	"kv-db-lab/model"
	"testing"
)

func TestEngine_BackUp(t *testing.T) {
	db, _ := OpenWithOptions(model.DefaultOptions)
	err := db.BackUp("./../backFile")
	assert.Nil(t, err)
}
