package fileIO

import (
	"errors"
	"kv-db-lab/model"
)

// IOManager
//
//	IOManager
//	@Description: 文件io公用接口
type IOManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)

	// Sync : 将内存缓冲区的数据持久化到磁盘中
	Sync() error

	Close() error

	// Size 获取到文件大小
	Size() (int64, error)
}

func NewIOManager(filePath string, ioType model.IOType) (IOManager, error) {
	switch ioType {
	case model.StandardFileIO:
		return NewFileIO(filePath)
	case model.MMapFileIO:
		return NewMMapIOManager(filePath)
	default:
		return nil, errors.New("无效的fileIO类型")
	}
}
