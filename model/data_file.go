package model

import "kv-db-lab/fileIO"

type DataFile struct {
	FilePos   *LogRecordPos    // 文件数据位置信息
	IOManager fileIO.IOManager // 文件IO的能力接入
}

func OpenDataFile(path string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) Write(b []byte) error {
	return nil
}

func (df *DataFile) ReadLogRecordByOffset(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}
