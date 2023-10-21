package dao

import (
	"github.com/sirupsen/logrus"
	"kv-db-lab/constant"
	"kv-db-lab/index"
	"kv-db-lab/model"
	"sync"
)

// DB 存储引擎实例
type DB struct {
	option     *model.Options
	lock       *sync.RWMutex
	activeFile *model.DataFile
	oldFile    map[uint]*model.DataFile
	index      index.Indexer
}

// Put
//
//	@Description: 将数据写入文件
//	@receiver d
//	@param key
//	@param value
//	@return error
func (d *DB) Put(key []byte, value []byte) error {
	// 参数校验
	if len(key) == 0 {
		return constant.ErrEmptyParam
	}

	// 构造要存入数据的格式
	logRecord := &model.LogRecord{
		Key:    key,
		Value:  value,
		Status: constant.LogRecordNormal,
	}

	// 追加写入活跃文件中
	pos, err := d.appendLogRecord(logRecord)
	if err != nil {
		logrus.Error("数据追加写入失败，err:", err.Error())
		return err
	}

	// 更新内存索引
	if ok := d.index.Put(key, pos); !ok {
		logrus.Error("update index failed,err")
		return constant.ErrUpdateIndex
	}

	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	// 参数校验
	if len(key) == 0 {
		return nil, constant.ErrEmptyParam
	}

	db.lock.RLock()
	defer db.lock.RUnlock()
	// 从内存中获取索信息
	logRecordPos := db.index.Get(key)

	//索引信息不存在
	if logRecordPos == nil {
		return nil, constant.ErrNotExist
	}

	// 根据文件ID找到对应数据文件  可能在活跃文件也可能在old文件
	var dataFile *model.DataFile
	if db.activeFile.FilePos.FileID == logRecordPos.FileID {
		dataFile = db.activeFile
	} else {
		dataFile = db.oldFile[logRecordPos.FileID]
	}

	// 未找到该文件
	if dataFile == nil {
		return nil, constant.ErrNotExist
	}

	// 根据偏移量去读取数据
	logRecord, err := dataFile.ReadLogRecordByOffset(dataFile.FilePos.Offset)
	if err != nil {
		logrus.Error("根据偏移量读取数据失败，err:", err.Error())
		return nil, err
	}

	if logRecord.Status == constant.LogRecordDelete {
		logrus.Info("根据偏移量读取数据的状态是已删除")
		return nil, constant.ErrNotExist
	}

	return logRecord.Value, nil
}

// appendLogRecord
//
//	@Description: 追加写入数据到活跃文件中
//	@receiver db
//	@param logRecord  // 写入数据
//	@return *model.LogRecordPos  // 写入后返回该数据的索引信息
//	@return error
func (db *DB) appendLogRecord(logRecord *model.LogRecord) (*model.LogRecordPos, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// 判断当前活跃数据文件是否存在，若不存在需要自己生成
	if db.activeFile == nil {
		err := db.setActiveFile()
		if err != nil {
			return nil, err
		}
	}

	// 对要写入record进行编码
	Record, size := model.EncodeLogRecord(logRecord)

	// 如果写入数据加上这一段数据>该活跃文件数据量阈值  ----> 关闭当前活跃文件，打开新的文件
	if db.activeFile.FilePos.Offset+size > db.option.DataFileSize {
		// 先持久化数据文件，保证原有数据落盘
		if err := db.activeFile.Sync(); err != nil {
			logrus.Info("活跃数据落盘错误,err:=", err.Error())
			return nil, err
		}

		// 将当前活跃文件转为旧的数据文件
		db.oldFile[db.activeFile.FilePos.FileID] = db.activeFile

		//打开新的数据文件
		if err := db.setActiveFile(); err != nil {
			logrus.Error("打开新的数据文件failed，err:", err.Error())
			return nil, err
		}
	}

	// 正式写入文件

	// 维护写入时的offset，后续构建索引信息
	writeOffset := db.activeFile.FilePos.Offset
	err := db.activeFile.Write(Record)
	if err != nil {
		logrus.Error("activeFile：数据写入失败,err:", err.Error())
		return nil, err
	}
	db.activeFile.FilePos.Offset += size //更新活跃文件写入偏移

	// 根据用户配置决定是否需要持久化
	if db.option.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			logrus.Error("activeFile数据持久化失败,err:", err.Error())
			return nil, err
		}
	}

	// 构造内存索引信息
	pos := &model.LogRecordPos{
		FileID: db.activeFile.FilePos.FileID,
		Offset: int64(writeOffset),
	}
	return pos, nil
}

// setActiveFile
//
//	@Description: 设置当前活跃文件(该共享数据结构的修改非线程安全，加锁访问）
//	@receiver db
//	@return error
func (db *DB) setActiveFile() error {
	var initialFileId uint32
	if db.activeFile != nil {
		initialFileId = uint32(db.activeFile.FilePos.FileID + 1)
	}

	// 打开新的数据文件
	dataFile, err := model.OpenDataFile(db.option.DirPath, initialFileId)
	if err != nil {
		logrus.Info("db:open new file failed,err:", err.Error())
		return err
	}

	db.activeFile = dataFile
	return nil
}
