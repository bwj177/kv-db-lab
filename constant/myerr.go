package constant

type Err string

func (e Err) Error() string {
	return string(e)
}

const (
	ErrEmptyParam  = Err("参数为空")
	ErrUpdateIndex = Err("索引更新失败")
	ErrNotExist    = Err("数据不存在")
	ErrInvalidCRC  = Err("无效的CRC")
	ErrWrongTypeOp = Err("此数据类型不支持该操作")
	ErrExpireTime  = Err("此数据已经过期")
)
