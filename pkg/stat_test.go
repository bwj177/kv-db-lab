package pkg

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_getDiskFreeSpace(t *testing.T) {
	size, err := getDiskFreeSpace("/")

	assert.Nil(t, err)
	t.Log(size)
}
