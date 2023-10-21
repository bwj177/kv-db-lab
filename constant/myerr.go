package constant

type Err string

func (e Err) Error() string {
	return string(e)
}

const (
	ErrEmptyParam  = Err("参数为空")
	ErrUpdateIndex = Err("索引更新失败")
	ErrNotExist    = Err("数据不存在")
)
