package pkg

import (
	"errors"
	"github.com/sirupsen/logrus"
	"kv-db-lab/constant"
	"kv-db-lab/model"
)

func CheckOptions(options *model.Options) error {
	if options.DirPath == "" {
		logrus.Error("open engine failed,empty dir path")
		return constant.ErrEmptyParam
	}
	if options.DataFileSize <= 0 {
		options.DataFileSize = constant.DefaultFileSize
		logrus.Warn("初始化存储引擎未指定存储文件的存储阈值，将使用默认阈值")
	}

	if options.DateFileMergeRatio < 0 || options.DateFileMergeRatio > 1 {
		return errors.New("merge ratio must be 0~1")
	}
	return nil
}
