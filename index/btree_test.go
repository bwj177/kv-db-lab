package index

import (
	c "github.com/smartystreets/goconvey/convey"
	"kv-db-lab/model"
	"testing"
)

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree(32)

	bt.Put([]byte("key1"), &model.LogRecordPos{
		FileID: 1,
		Offset: 0,
	})

	c.Convey("删除api测试", t, func() {
		tt := []struct {
			name   string
			key    []byte
			expect bool
		}{
			{"test1", []byte("sdsd"), false},
			{"test2", []byte("sdsv"), false},
			{"test3", []byte("key1"), true},
		}
		for _, tc := range tt {
			c.Convey(tc.name, func() { // 嵌套调用Convey
				got := bt.Delete(tc.key)
				c.So(got, c.ShouldResemble, tc.expect)
			})
		}
	})
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree(32)
	bt.Put([]byte("key1"), &model.LogRecordPos{
		FileID: 1,
		Offset: 0,
	})

	c.Convey("get-api-test", t, func() {
		tt := []struct {
			name   string
			key    []byte
			expect *model.LogRecordPos
		}{
			{"test1", []byte("sdsd"), nil},
			{"test2", []byte("key1"), &model.LogRecordPos{
				FileID: 1,
				Offset: 0,
			}},
		}
		for _, tc := range tt {
			c.Convey(tc.name, func() { // 嵌套调用Convey
				got := bt.Get(tc.key)
				c.So(got, c.ShouldResemble, tc.expect)
			})
		}
	})
}

func TestBTree_Put(t *testing.T) {
	bt := NewBTree(32)
	c.Convey("get-api-test", t, func() {
		pos1 := &model.LogRecordPos{
			FileID: 1,
			Offset: 0,
		}
		pos2 := &model.LogRecordPos{
			FileID: 1,
			Offset: 3,
		}
		tt := []struct {
			name   string
			key    []byte
			pos    *model.LogRecordPos
			expect bool
		}{
			{"test1", []byte("key1"), pos1, true},
			{"test2", []byte("key2"), pos2, true},
		}
		for _, tc := range tt {
			c.Convey(tc.name, func() { // 嵌套调用Convey
				got := bt.Put(tc.key, tc.pos)
				c.So(got, c.ShouldResemble, tc.expect)
			})
		}
	})
}
