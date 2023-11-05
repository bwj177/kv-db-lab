package storage

import (
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"kv-db-lab/constant"
	"kv-db-lab/model"
	"kv-db-lab/pkg"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

// Merge
//
//	@Description: 清理无效数据并生成对应Hint文件
//	@receiver db
//	@return error
func (db *Engine) Merge() error {
	if db.activeFile == nil {
		return nil
	}

	// 不允许同时执行merge
	if db.isMerging {
		return errors.New("engine is merging")
	}

	// 未达到设定无效数据阈值不可以merge
	// mergeRatio : 无效数据与总key数量的比值
	stat := db.Stat()

	reclaimSize, diskSize := stat.ReclaimableSize, stat.DiskSize
	fmt.Println(reclaimSize)
	fmt.Println(diskSize)
	mergeRatio := float32(reclaimSize) / float32(diskSize)
	logrus.Info(reclaimSize, diskSize, mergeRatio)
	if mergeRatio < db.option.DateFileMergeRatio {
		return errors.New("can`t frequently merge it")
	}

	// 锁定资源
	db.isMerging = true
	db.lock.Lock()
	defer func() {
		db.isMerging = false
	}()

	// ====================预处理结束================================================================================

	// 持久化当前活跃数据
	if err := db.activeFile.Sync(); err != nil {
		db.lock.Unlock()
		return err
	}

	// 将当前活跃文件转为旧的数据文件
	db.oldFile[db.activeFile.FilePos.FileID] = db.activeFile

	// 打开新的数据文件作为活跃文件，让在merge时，使用者写入的数据写入此文件
	if err := db.setActiveFile(); err != nil {
		db.lock.Unlock()
		return err
	}

	// 记录没有参加merge的文件ID，如新创建的活跃文件,比这个文件ID小的文件都是merge完成的文件
	nonMergeFileID := db.activeFile.FilePos.FileID

	// 取出所有的旧文件进行merge，避免与使用者读数据去竞争锁，影响使用者使用体验
	var mergeFile []*model.DataFile
	for _, oldFile := range db.oldFile {
		mergeFile = append(mergeFile, oldFile)
	}
	db.lock.Unlock()

	// 将merge文件小->大排序，依次merge
	sort.Slice(mergeFile, func(i, j int) bool {
		return mergeFile[i].FilePos.FileID > mergeFile[j].FilePos.FileID
	})

	mergePath := db.GetMergePath()
	// 如果目录存在，说明发生过 merge，将其删除掉
	if _, err := os.Stat(mergePath); err == nil {
		log.Println("删除旧的merge文件")
		if err := os.RemoveAll(mergePath); err != nil {
			return errors.New("删除merge目录失败")
		}
	}
	// 新建一个 merge path 的目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	// 打开一个新的临时bitcask引擎用于merge操作
	mergeOptions := db.option
	mergeOptions.SyncWrites = true
	mergeOptions.DirPath = mergePath
	mergeEngine, err := OpenWithOptions(mergeOptions)
	if err != nil {
		return err
	}

	// 打开Hint文件去存储数据的索引
	hintFile, err := model.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	// 遍历取出的旧文件依次进行merge
	for _, dateFile := range mergeFile {
		var offset int64
		for {
			logRecord, size, err := dateFile.ReadLogRecordByOffset(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			realKey, _ := pkg.PraseKey(logRecord.Key)

			// 从db中内存索引的key(最新的) 拿到位置信息与merge中比对，若此数据为最新的数据那么有效，进行重写
			logRecordPos := db.index.Get(realKey)
			if logRecordPos != nil && logRecordPos.FileID == dateFile.FilePos.FileID && logRecordPos.Offset == offset {
				// 有效，写入
				logRecord.Key = pkg.LogRecordKeySeq(logRecord.Key, constant.NoneTransactionID)
				pos, err := mergeEngine.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				// 将当前索引写入到Hint文件当中
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}

			// 写入后更新offset继续读取写入
			offset += size
		}
	}

	// 全部写完后将hint文件、merge文件持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeEngine.Sync(); err != nil {
		return err
	}

	// 标识 merge完成 的文件
	mergeFinishedFile, err := model.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}

	// 构造finished的数据
	mergeFinRecord := &model.LogRecord{
		Key:   []byte(constant.MergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileID))),
	}

	encRecord, _ := model.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}

	// 持久化mergeFinishFile
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}

	err = mergeFinishedFile.Close()
	if err != nil {
		return err
	}

	return nil
}

// GetMergePath 获得merge文件的目录，与数据文件目录同级，如 /test_file   /test_file-merge
func (db *Engine) GetMergePath() string {
	// dir：数据目录父目录  base:目录名称
	dir := path.Dir(path.Clean(db.option.DirPath))
	base := path.Base(db.option.DirPath)

	// 拼接返回
	return filepath.Join(dir, base+constant.MergeSuffix)
}

// 加载merge 数据目录
func (db *Engine) loadMergeFile() error {
	mergePath := db.GetMergePath()

	// 该目录不存在则直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}

	// 删除merge文件
	defer func() {
		fmt.Println(mergePath)
		err := os.RemoveAll(mergePath)
		if err != nil {
			panic("remove mergeDir failed")
		}
	}()

	// 读取目录所有文件
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 查找标识merge完成的文件，判断merge是否处理完成
	var isMergeFin bool

	// 在扫描整个merge目录的文件时记录所有merge的文件名方便迁移
	mergeFileNames := make([]string, 0)
	for _, entry := range dirEntries {
		if entry.Name() == constant.MergeFinishedName {
			isMergeFin = true
		}

		// 不需要将存储事务ID的文件迁移进行merge
		if entry.Name() == constant.NowTxIDFileName {
			continue
		}

		//锁文件也不需要
		if entry.Name() == constant.FileLockName {
			continue
		}

		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	// 如果没完成则直接返回
	if !isMergeFin {
		return nil
	}

	// 拿到未进行merge的最小文件ID
	nonMergeFileID, err := db.getNonMergeFileID(mergePath)
	if err != nil {
		return err
	}

	// 删除旧的数据文件(已经merge的数据文件)
	var fileID uint32
	for ; fileID < nonMergeFileID; fileID++ {
		// 按照规则拼接文件名
		fileName := filepath.Join(db.option.DirPath, fmt.Sprintf("%09d", fileID)+constant.DataFileSuffix)

		// 若该文件存在则删除掉
		if _, err := os.Stat(fileName); err == nil {
			err := os.Remove(fileName)
			if err != nil {
				return err
			}
		}
	}

	// 将merge后的文件移到数据目录当中
	for _, mergeFileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, mergeFileName)
		destPath := filepath.Join(db.option.DirPath, mergeFileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}

	return nil
}

// 拿到未进行merge的最小文件ID，也就是mergeFinFile文件中存储的record的value
func (db *Engine) getNonMergeFileID(dirPath string) (uint32, error) {
	mergeFinishedFile, err := model.OpenMergeFinishedFile(dirPath)

	// 文件中仅存储了merge完成的标识的数据，所以offset为0就是需要的数据
	record, _, err := mergeFinishedFile.ReadLogRecordByOffset(0)
	if err != nil {
		return 0, err
	}

	recordFileID, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}

	err = mergeFinishedFile.Close()
	if err != nil {
		return 0, err
	}

	return uint32(recordFileID), err
}

func (db *Engine) loadIndexFromHintFile() error {
	// 查看hint文件是否存在,不存在则直接返回
	filePath := path.Join(db.option.DirPath, constant.HintFileName)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil
	}

	// 打开hint索引文件
	hintFile, err := model.OpenHintFile(db.option.DirPath)
	if err != nil {
		return err
	}

	// 循环读取hintFile中索引信息存储
	var offset int64
	for {
		logRecord, size, err := hintFile.ReadLogRecordByOffset(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// 对存储的value解码成pos
		recordPos := model.DecodeLogRecordPos(logRecord.Value)

		// 存储索引
		db.index.Put(logRecord.Key, recordPos)

		// 更新偏移
		offset += size
	}

	return nil
}
