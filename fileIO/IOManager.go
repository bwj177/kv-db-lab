package fileIO

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
}