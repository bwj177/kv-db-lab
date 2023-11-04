package storage

import (
	"errors"
	"kv-db-lab/constant"
	"kv-db-lab/model"
	"kv-db-lab/pkg"
	"sync"
	"sync/atomic"
)

// WriteBatch 批量写数据保证原子性
type WriteBatch struct {
	lock          *sync.RWMutex
	engine        *Engine
	pendingWrites map[string]*model.LogRecord
	options       *model.WriteBatchOptions
}

// Put
//
//	@Description: 批量写入，先暂存入pendingWrites
//	@receiver w
//	@param key
//	@param value
//	@return error
func (w *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return constant.ErrEmptyParam
	}

	w.lock.Lock()
	defer w.lock.Unlock()

	// 先将写入数据暂存而非直接写入磁盘，保证原子性
	logRecord := &model.LogRecord{
		Key:    key,
		Value:  value,
		Status: constant.LogRecordNormal,
	}
	w.pendingWrites[string(key)] = logRecord

	return nil
}

// Delete
//
//	@Description: 批量写的delete API
//	@receiver w
//	@param key
//	@return error
func (w *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return constant.ErrEmptyParam
	}

	w.lock.Lock()
	defer w.lock.Unlock()

	// 先check key是否存在,若不存在该数据则没必要将其暂存去进行系统调用，内存也无暂存数据，则返回数据不存在，若内存中有先前写入的数据，则进行删除
	lrPos := w.engine.index.Get(key)
	if lrPos == nil {
		// 如果暂存map中不存在数据，那么删除数据本不存在，返回
		if _, ok := w.pendingWrites[string(key)]; !ok {
			return constant.ErrNotExist
		} else {
			// 如果暂存map中存在数据，那么将其数据删除
			delete(w.pendingWrites, string(key))
			return nil
		}
	}

	//与Put一致先暂存在内存中
	logRecord := &model.LogRecord{
		Key:    key,
		Value:  nil,
		Status: constant.LogRecordDelete,
	}
	w.pendingWrites[string(key)] = logRecord
	return nil
}

// Commit
//
//	@Description: 事务提交，将预写的批量数据持久化到磁盘
//	@receiver w
//	@return error
func (w *WriteBatch) Commit() error {
	if len(w.pendingWrites) == 0 {
		return nil
	}

	// 如果要批量写入的数据超出的设定阈值
	if uint(len(w.pendingWrites)) > w.options.MaxBatchSize {
		return errors.New("batch write over max num")
	}

	// 获取当前事务最新的事务ID
	transID := atomic.AddUint64(&w.engine.transID, 1)

	// map读安全
	w.lock.RLock()
	defer w.lock.RUnlock()

	// 将Btree索引信息先维护在内存中，后续将索引信息批量写入
	logRecordPoses := make(map[string]*model.LogRecordPos)

	// 依次进行批量写入
	for _, record := range w.pendingWrites {
		encLogRecord := &model.LogRecord{
			Key:    pkg.LogRecordKeySeq(record.Key, transID),
			Value:  record.Value,
			Status: record.Status,
		}
		logRecordPos, err := w.engine.appendLogRecord(encLogRecord)
		if err != nil {
			return err
		}

		logRecordPoses[string(record.Key)] = logRecordPos
	}

	// 写一条标识事务已提交的数据 标识是否全部成功写入
	finishedRecord := &model.LogRecord{
		Key:    pkg.LogRecordKeySeq(constant.TxFinKey, transID),
		Value:  nil,
		Status: constant.LogRecordNormal,
	}
	if _, err := w.engine.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	// 根据配置决定是否持久化
	if w.options.SyncWrite {
		if err := w.engine.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, record := range w.pendingWrites {
		key := record.Key
		pos := logRecordPoses[string(key)]
		var oldPos *model.LogRecordPos

		if record.Status == constant.LogRecordNormal {
			oldPos = w.engine.index.Put(key, pos)
		}
		if record.Status == constant.LogRecordDelete {
			oldPos = w.engine.index.Delete(key)
			// 更新无效数据大小
			w.engine.reclaimSize += pos.Size
		}

		if oldPos != nil {
			// 更新无效数据大小
			w.engine.reclaimSize += oldPos.Size
		}
	}

	// 清空暂存数据
	w.pendingWrites = make(map[string]*model.LogRecord)
	return nil
}
