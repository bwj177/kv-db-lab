package model

import (
	"fmt"
	"io"
	"kv-db-lab/constant"
	"kv-db-lab/fileIO"
	"path/filepath"
)

type DataFile struct {
	FilePos   *LogRecordPos    // 文件数据位置信息
	IOManager fileIO.IOManager // 文件IO的能力接入
}

func OpenDataFile(path string, fileId uint32, fileIOType fileIO.IOType) (*DataFile, error) {
	// 组装filePath
	fileName := filepath.Join(path, fmt.Sprintf("%09d", fileId)+constant.DataFileSuffix)

	// 初始化fileIO
	ioManager, err := fileIO.NewIOManager(fileName, fileIOType)
	if err != nil {
		return nil, err
	}

	dataFile := &DataFile{
		FilePos: &LogRecordPos{
			FileID: uint(fileId),
			Offset: 0,
		},
		IOManager: ioManager,
	}

	return dataFile, nil
}

func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, constant.HintFileName)

	// 初始化fileIO
	ioManager, err := fileIO.NewIOManager(fileName, fileIO.StandardFileIO)
	if err != nil {
		return nil, err
	}

	dataFile := &DataFile{
		FilePos: &LogRecordPos{
			FileID: 0,
			Offset: 0,
		},
		IOManager: ioManager,
	}

	return dataFile, nil
}

func OpenTxIDFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, constant.NowTxIDFileName)

	// 初始化fileIO
	ioManager, err := fileIO.NewIOManager(fileName, fileIO.StandardFileIO)
	if err != nil {
		return nil, err
	}

	dataFile := &DataFile{
		FilePos: &LogRecordPos{
			FileID: 0,
			Offset: 0,
		},
		IOManager: ioManager,
	}

	return dataFile, nil
}

func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, constant.MergeFinishedName)

	// 初始化fileIO
	ioManager, err := fileIO.NewIOManager(fileName, fileIO.StandardFileIO)
	if err != nil {
		return nil, err
	}

	dataFile := &DataFile{
		FilePos: &LogRecordPos{
			FileID: 0,
			Offset: 0,
		},
		IOManager: ioManager,
	}

	return dataFile, nil
}

func (df *DataFile) Write(b []byte) error {
	n, err := df.IOManager.Write(b)

	// 更新写入偏移量
	df.FilePos.Offset += int64(n)
	return err
}

func (df *DataFile) ReadLogRecordByOffset(offset int64) (*LogRecord, int64, error) {
	// 获取当前文件大小
	size, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}

	var headerBytes int64 = constant.MaxLogRecordHeaderSize
	// 如果读到最后，但是最后一个数据的大小并没有MaxLogRecordHeaderSize这么大，如何再读取这么大的buf就会报错
	if offset+constant.MaxLogRecordHeaderSize > size {
		headerBytes = size - offset
	}

	// 读取header信息
	headerBuf, err := df.read_N_Bytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	// 解码获取header结构体与header的大小
	header, headerSize := decodeLogRecordHeader(headerBuf)

	//如果读完了
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	keySize, valueSize := int64(header.keySize), int64(header.valueSize)

	// 通过头部信息读取实际存储key、value数据
	recordByte, err := df.read_N_Bytes(keySize+valueSize, offset+headerSize)
	if err != nil {
		return nil, 0, err
	}

	// 解析读出的byte数组
	key := recordByte[:keySize]
	value := recordByte[keySize:]

	// 拼接数据格式返回
	logRecord := &LogRecord{
		Key:    key,
		Value:  value,
		Status: header.recordType,
	}

	// 通过crc校验数据的有效性
	// 注意header可能并没有headerbuf这么长(key-value Size为边长)
	crc := getLogRecordCRC(logRecord, headerBuf[:headerSize])
	if crc != header.crc {
		return nil, 0, constant.ErrInvalidCRC
	}

	return logRecord, headerSize + keySize + valueSize, nil
}

// read_N_Bytes
//
//	@Description: 读取文件中长度为N的字节数组
//	@receiver df
//	@param n  长度
//	@param offset  指定位置开始读
//	@return []byte
//	@return error
func (df *DataFile) read_N_Bytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)

	_, err := df.IOManager.Read(b, offset)

	if err != nil {
		return nil, err
	}

	return b, nil
}

func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IOManager.Sync()
}

// WriteHintRecord 写入索引信息到Hint文件中
func (df *DataFile) WriteHintRecord(key []byte, recordPos *LogRecordPos) error {
	logRecord := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(recordPos),
	}

	encByte, _ := EncodeLogRecord(logRecord)
	err := df.Write(encByte)
	if err != nil {
		return err
	}

	return nil
}

// SetIOManager
//
//	@Description: 重新设置dateFile中文件io类型
//	@receiver df
//	@param dirPath
//	@param ioType
//	@return error
func (df *DataFile) SetIOManager(dirPath string, ioType fileIO.IOType) error {
	// 关闭旧的ioManager
	if err := df.IOManager.Close(); err != nil {
		return err
	}

	// 构造新的IOManager
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", df.FilePos.FileID)+constant.DataFileSuffix)
	ioManager, err := fileIO.NewIOManager(fileName, ioType)
	if err != nil {
		return err
	}

	df.IOManager = ioManager
	return nil
}
