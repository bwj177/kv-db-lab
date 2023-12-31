package model

import (
	"github.com/stretchr/testify/assert"
	"kv-db-lab/fileIO"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dataFile, err := OpenDataFile("./../test_file", 1, fileIO.StandardFileIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

}

func TestDataFile_Write(t *testing.T) {
	dataFile, err := OpenDataFile("./../test_file", 2, fileIO.StandardFileIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
	err = dataFile.Write([]byte("5184814471你好你好"))
	assert.Nil(t, err)
}
