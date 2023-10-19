package fileIO

import (
	"github.com/sirupsen/logrus"
	c "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

func TestFileIO_Write(t *testing.T) {
	filePath := "./test1.txt"
	fd, err := NewFileIO(filePath)
	if err != nil {
		t.Error("生成文件err")
		return
	}
	c.Convey("fileIO-wirte-test", t, func() {
		tt := []struct {
			name   string
			bytes  []byte
			expect int
		}{
			{"test1", []byte("你好你好"), 12},
			{"test2", []byte("不好不好"), 12},
		}
		for _, tc := range tt {
			c.Convey(tc.name, func() { // 嵌套调用Convey
				n, err := fd.Write(tc.bytes)
				t.Logf("写入数据:%v", string(tc.bytes))
				c.So(n, c.ShouldResemble, tc.expect)
				c.So(err, c.ShouldResemble, nil)
			})
		}
	})
	Flush(filePath)
}

func TestFileIO_Read(t *testing.T) {
	filePath := "./test1.txt"
	fd, err := NewFileIO(filePath)
	defer Flush(filePath)
	if err != nil {
		t.Error("生成文件err")
		return
	}
	c.Convey("fileIO-read-test", t, func() {
		b1 := make([]byte, 3)
		b2 := make([]byte, 4)
		b3 := make([]byte, 6)
		tt := []struct {
			name   string
			bytes  []byte
			start  int64
			expect string
		}{
			{"test1", b1, 1, "bcd"},
			{"test2", b2, 3, "defg"},
			{"test3", b3, 11, "你好"},
		}
		for _, tc := range tt {
			c.Convey(tc.name, func() { // 嵌套调用Convey
				n, err := fd.Read(tc.bytes, tc.start)
				t.Logf("读取数据:%v", string(tc.bytes))
				c.So(string(tc.bytes), c.ShouldResemble, tc.expect)
				c.So(n, c.ShouldResemble, len(tc.bytes))
				c.So(err, c.ShouldResemble, nil)
			})
		}
	})
	Flush(filePath)
}

func Flush(filePath string) {
	err := os.RemoveAll(filePath)
	if err != nil {
		logrus.Error("删除文件失败")
		return
	}
	return
}
