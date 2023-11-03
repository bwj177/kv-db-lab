package fileIO

import (
	"errors"
	"golang.org/x/exp/mmap"
	"kv-db-lab/constant"
	"os"
)

// MMap mmap文件IO
type MMap struct {
	readerAt *mmap.ReaderAt
}

func NewMMapIOManager(fileName string) (*MMap, error) {
	// 如果该文件不存在则要创建
	_, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, constant.DefaultFileMode)
	if err != nil {
		return nil, err
	}

	readAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readAt}, nil
}

func (m *MMap) Read(bytes []byte, i int64) (int, error) {
	return m.readerAt.ReadAt(bytes, i)
}

func (m *MMap) Write(bytes []byte) (int, error) {
	// mmap并不支持write
	return 0, errors.New("mmap cant supposed read")
}

func (m *MMap) Sync() error {
	return nil
}

func (m *MMap) Close() error {
	return m.readerAt.Close()
}

func (m *MMap) Size() (int64, error) {
	return int64(m.readerAt.Len()), nil
}
