package storage

import (
	"errors"
	"github.com/sirupsen/logrus"
	"io"
	"kv-db-lab/constant"
	"kv-db-lab/index"
	"kv-db-lab/model"
	"kv-db-lab/pkg"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Engine 存储引擎实例
type Engine struct {
	option     *model.Options
	lock       *sync.RWMutex
	activeFile *model.DataFile
	oldFile    map[uint]*model.DataFile
	index      index.Indexer

	fileIds []int // 仅用于加载索引
}

// Put
//
//	@Description: 将数据写入文件
//	@receiver db
//	@param key
//	@param value
//	@return error
func (db *Engine) Put(key []byte, value []byte) error {
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
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		logrus.Error("数据追加写入失败，err:", err.Error())
		return err
	}

	// 更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		logrus.Error("update index failed,err")
		return constant.ErrUpdateIndex
	}

	return nil
}

func (db *Engine) Get(key []byte) ([]byte, error) {
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
	logRecord, _, err := dataFile.ReadLogRecordByOffset(logRecordPos.Offset)
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

func (db *Engine) GetByRecordPos(logRecordPos *model.LogRecordPos) ([]byte, error) {
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
	logRecord, _, err := dataFile.ReadLogRecordByOffset(logRecordPos.Offset)
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
func (db *Engine) appendLogRecord(logRecord *model.LogRecord) (*model.LogRecordPos, error) {
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
		Offset: writeOffset,
	}
	return pos, nil
}

func (db *Engine) Delete(key []byte) error {
	// 校验入参
	if len(key) == 0 {
		return constant.ErrEmptyParam
	}

	// 检验key是否在Btree索引中是否存在，若不存在则没有继续的必要
	recordPos := db.index.Get(key)
	if recordPos == nil {
		return nil
	}

	// 后续步骤与put一致,写入状态为delete的数据信息
	logRecord := &model.LogRecord{
		Key:    key,
		Value:  nil,
		Status: constant.LogRecordDelete,
	}
	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 删除索引文件
	ok := db.index.Delete(key)
	if !ok {
		return errors.New("删除索引失败")
	}

	return nil
}

// setActiveFile
//
//	@Description: 设置当前活跃文件(该共享数据结构的修改非线程安全，加锁访问）
//	@receiver db
//	@return error
func (db *Engine) setActiveFile() error {
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

// 从磁盘中加载数据文件
func (db *Engine) loadDateFile() error {
	dirEnties, err := os.ReadDir(db.option.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// fileName ex:001.data 002.data 001 to fileIds
	// 遍历文件找到符合数据文件的后缀
	for _, dirEntry := range dirEnties {
		if strings.HasSuffix(dirEntry.Name(), constant.DataFileSuffix) {
			prefix := strings.Split(dirEntry.Name(), ".")[0]
			fileID, err := strconv.Atoi(prefix)
			if err != nil {
				return errors.New("文件前缀非数字")
			}
			fileIds = append(fileIds, fileID)
		}
	}

	// 将文件ID进行排序
	sort.Ints(fileIds)

	// 维护有序的fileIds便于后续加载索引信息->BTree
	db.fileIds = fileIds

	// 打开文件并加载到引擎的数据文件中
	for i, fileId := range fileIds {
		dataFile, err := model.OpenDataFile(db.option.DirPath, uint32(fileId))
		if err != nil {
			return err
		}
		// 默认让最大id的文件作为activeFile
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.oldFile[uint(fileId)] = dataFile
		}
	}
	return nil
}

// 从数据文件中加载索引
// 遍历所有数据记录并将key,fileId,offset记录到索引中
func (db *Engine) loadIndexFromDateFiles() error {
	// 无文件加载
	if len(db.fileIds) == 0 {
		return nil
	}

	for i, fileID := range db.fileIds {
		fid := uint(fileID)
		var dateFile *model.DataFile
		if fid == db.activeFile.FilePos.FileID {
			dateFile = db.activeFile
		} else {
			dateFile = db.oldFile[fid]
		}

		//循环读取file中数据
		var offset int64 = 0
		for {
			logRecord, size, err := dateFile.ReadLogRecordByOffset(offset)
			if err == io.EOF { // 已读完
				break
			}
			if err != nil {
				return err
			}
			// 拿到数据记录将其构造出内存索引存入内存存储中
			logRecordPos := model.LogRecordPos{
				FileID: fid,
				Offset: offset,
			}
			// 如果记录为已删除状态
			if logRecord.Status == constant.LogRecordDelete {
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, &logRecordPos)
			}

			offset += size
		}
		// 如果加载的是当前活跃文件，那么更新文件的writeOff
		if i == len(db.fileIds)-1 {
			db.activeFile.FilePos.Offset = int64(i)
		}
	}
	return nil
}

// OpenWithOptions
//
//	@Description: engine启动入口，用户需传入自己需要的配置项，加载datafile，索引信息，再返回engine使用
//	@param options
//	@return *Engine
//	@return error
func OpenWithOptions(options *model.Options) (*Engine, error) {
	// 校验传入的配置项
	if err := pkg.CheckOptions(options); err != nil {
		logrus.Error("db:open failed,err:", err.Error())
		return nil, err
	}

	// 判断数据目录是否存在，如果不存在则创建这个目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		// 不存在，自行创建目录
		if err := os.Mkdir(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化engine结构体
	db := &Engine{
		option:  options,
		lock:    &sync.RWMutex{},
		oldFile: make(map[uint]*model.DataFile),
		index:   index.NewIndexer(options.Index),
	}

	// 加载数据文件
	if err := db.loadDateFile(); err != nil {
		logrus.Error("初始化时加载数据文件失败")
		return nil, err
	}

	// 从数据文件中加载索引
	if err := db.loadIndexFromDateFiles(); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *Engine) NewIterate(opts *model.IteratorOptions) *Iterate {
	indexIter := db.index.Iterator(opts.Reverse)

	return &Iterate{
		indexIter: indexIter,
		engine:    db,
		options:   opts,
	}
}

// GetAllKeys : 获取数据库中所有key
func (db *Engine) GetAllKeys() [][]byte {
	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	idx := 0

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx += 1
	}
	return keys
}

// Fold
//
//	@Description: 获取数据库所有key，value对并执行指定fn逻辑
//	@receiver db
//	@param fn
//	@return error
func (db *Engine) Fold(fn func(key []byte, value []byte) bool) error {
	iter := db.index.Iterator(false)

	//从文件中读加读锁
	db.lock.RLock()
	defer db.lock.RUnlock()

	// 使用迭代器获得pos->value
	for iter.Rewind(); iter.Valid(); iter.Next() {
		value, err := db.GetByRecordPos(iter.Value())
		if err != nil {
			return err
		}
		ok := fn(iter.Key(), value)
		if !ok {
			break
		}
	}
	return nil
}

// Close 关闭Engine，将文件中数据进行持久化
func (db *Engine) Close() error {
	if db.activeFile == nil {
		return nil
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	// 关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// 关闭旧的活跃文件
	for _, file := range db.oldFile {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Sync 将DB当前活跃数据持久化
func (db *Engine) Sync() error {
	if db.activeFile == nil {
		return nil
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	return db.activeFile.Sync()
}
