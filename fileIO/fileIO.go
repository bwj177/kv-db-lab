package fileIO

import "os"

type fileIO struct {
	fd *os.File
}

// Read
//
//	@Description: read API 从指定位置读取文件中数据
//	@receiver f
//	@param bytes
//	@param i :offset
//	@return int
//	@return error
func (f fileIO) Read(bytes []byte, i int64) (int, error) {
	return f.fd.ReadAt(bytes, i)
}

// Write
//
//	@Description: 追加写入数据到文件
//	@receiver f
//	@param bytes
//	@return int
//	@return error
func (f fileIO) Write(bytes []byte) (int, error) {
	return f.fd.Write(bytes)
}

func (f fileIO) Sync() error {
	return f.fd.Sync()
}

func (f fileIO) Close() error {
	return f.fd.Close()
}

func (f fileIO) Size() (int64, error) {
	stat, err := f.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
func NewFileIO(fileName string) (*fileIO, error) {
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &fileIO{fd: fd}, nil
}
