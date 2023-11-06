package benchmark

import (
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"kv-db-lab/constant"
	"kv-db-lab/model"
	"kv-db-lab/pkg"
	"kv-db-lab/storage"
	"math/rand"
	"testing"
	"time"
)

var db *storage.Engine

func init() {
	var err error
	db, err = storage.OpenWithOptions(model.DefaultOptions)
	if err != nil {
		panic(err)
	}

}
func Benchmark_Put(b *testing.B) {
	// 重置计时器
	b.ResetTimer()

	// 打印每个方法内存使用情况
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(pkg.GetTestKey(i), pkg.RandomValue(8))
		if err != nil {
			logrus.Error("put failed，err:", err.Error())
		}
	}
}

func Benchmark_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(pkg.GetTestKey(i), pkg.RandomValue(1024))
		assert.Nil(b, err)
	}

	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(pkg.GetTestKey(rand.Int()))
		if err != nil && err != constant.ErrNotExist {
			b.Fatal(err)
		}
	}
}
func Benchmark_Delete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		err := db.Delete(pkg.GetTestKey(rand.Int()))
		assert.Nil(b, err)
	}
}
