package redis

import (
	"kv-db-lab/index"
)

// 需求： 设计一个针对zset数据结构的迭代器，由于kvDB存在磁盘中的数据是无序的，但是存在btree索引的key是天然有序的，zset的数据存储有两部分
// 数据格式分两部分存储：1：(key)key |version | score | member |member size =>(value)NULL
//                   2:(key)key | version | member => (value) score
// 因为天然有序，btree库支持遍历数据，因此遍历到的internalKey是有序的，score越大，internalKey越大
// 与之前设计的迭代器有所去别的是我需要添加进迭代器时进行一个校验，是否满足key|version这样一个前缀，若满足还需判断key长度是否为数据格式1的数据，再取出member，score

// 通过迭代器实现ZRange ZPopmax ZPopmin

// todo: 实现zset的迭代器去进行pop，range相关操作
type zsetIterator struct {
	index.Iterator

	// 用于过滤的前缀信息
	suffix string
}
