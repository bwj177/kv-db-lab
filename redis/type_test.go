package redis

import (
	"github.com/stretchr/testify/assert"
	"kv-db-lab/constant"
	"kv-db-lab/model"
	"kv-db-lab/pkg"
	"kv-db-lab/storage"
	"testing"
)

func TestRedisDataStructure_Get(t *testing.T) {
	opts := model.DefaultOptions
	dir := "./../redis_test_file"
	opts.DirPath = dir
	db, err := storage.OpenWithOptions(opts)
	rds, err := NewRedisDateStructure(db)
	assert.Nil(t, err)

	err = rds.Set(pkg.GetTestKey(1), pkg.RandomValue(100), 0)
	assert.Nil(t, err)

	val1, err := rds.Get(pkg.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	_, err = rds.Get(pkg.GetTestKey(33))
	assert.Equal(t, constant.ErrNotExist, err)
}

func TestRedisDataStructure_Del_Type(t *testing.T) {
	opts := model.DefaultOptions
	dir := "./../redis_test_file"
	opts.DirPath = dir
	db, err := storage.OpenWithOptions(opts)
	rds, err := NewRedisDateStructure(db)
	assert.Nil(t, err)

	// del
	_ = rds.Del(pkg.GetTestKey(11))
	assert.Nil(t, err)

	err = rds.Set(pkg.GetTestKey(1), pkg.RandomValue(100), 0)
	assert.Nil(t, err)

	// type
	typ, err := rds.Type(pkg.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, String, typ)

	err = rds.Del(pkg.GetTestKey(1))
	assert.Nil(t, err)

	_, err = rds.Get(pkg.GetTestKey(1))
	assert.Equal(t, constant.ErrNotExist, err)
}

func TestRedisDateStructure_HSet(t *testing.T) {
	opts := model.DefaultOptions
	dir := "./../redis_test_file"
	opts.DirPath = dir

	db, err := storage.OpenWithOptions(opts)
	assert.Nil(t, err)

	rds, err := NewRedisDateStructure(db)
	assert.Nil(t, err)

	ok, err := rds.HSet([]byte("晚餐q"), []byte("水果1"), []byte("香蕉"))
	t.Log(ok)
	assert.Nil(t, err)

	ok, err = rds.HSet([]byte("晚餐q"), []byte("蔬菜1"), []byte("西贡市"))
	t.Log(ok)
	assert.Nil(t, err)
}

func TestRedisDateStructure_Get(t *testing.T) {
	opts := model.DefaultOptions
	dir := "./../redis_test_file"
	opts.DirPath = dir

	db, err := storage.OpenWithOptions(opts)
	assert.Nil(t, err)

	rds, err := NewRedisDateStructure(db)
	assert.Nil(t, err)

	value, err := rds.HGet([]byte("晚餐"), []byte("水果"))
	assert.Nil(t, err)
	t.Log(string(value))

	value, err = rds.HGet([]byte("晚餐"), []byte("主食"))
	assert.NotNil(t, err)
	t.Log(string(value))
}

func TestRedisDataStructure_HDel(t *testing.T) {
	opts := model.DefaultOptions
	dir := "./../redis_test_file"
	opts.DirPath = dir

	db, err := storage.OpenWithOptions(opts)
	assert.Nil(t, err)

	rds, err := NewRedisDateStructure(db)
	assert.Nil(t, err)

	del1, err := rds.HDel(pkg.GetTestKey(200), nil)
	assert.Nil(t, err)
	assert.False(t, del1)

	ok1, err := rds.HSet(pkg.GetTestKey(1), []byte("field1"), pkg.RandomValue(100))
	assert.Nil(t, err)
	assert.True(t, ok1)

	v1 := pkg.RandomValue(100)
	ok2, err := rds.HSet(pkg.GetTestKey(1), []byte("field1"), v1)
	assert.Nil(t, err)
	assert.False(t, ok2)

	v2 := pkg.RandomValue(100)
	ok3, err := rds.HSet(pkg.GetTestKey(1), []byte("field2"), v2)
	assert.Nil(t, err)
	assert.True(t, ok3)

	del2, err := rds.HDel(pkg.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.True(t, del2)
}

func TestRedisDataStructure_SIsMember(t *testing.T) {
	opts := model.DefaultOptions
	dir := "./../redis_test_file"
	opts.DirPath = dir

	db, err := storage.OpenWithOptions(opts)
	assert.Nil(t, err)

	rds, err := NewRedisDateStructure(db)
	assert.Nil(t, err)

	ok, err := rds.SAdd(pkg.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SAdd(pkg.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SAdd(pkg.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(pkg.GetTestKey(2), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SIsMember(pkg.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SIsMember(pkg.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SIsMember(pkg.GetTestKey(1), []byte("val-not-exist"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestRedisDataStructure_SRem(t *testing.T) {
	opts := model.DefaultOptions
	dir := "./../redis_test_file"
	opts.DirPath = dir

	db, err := storage.OpenWithOptions(opts)
	assert.Nil(t, err)

	rds, err := NewRedisDateStructure(db)
	assert.Nil(t, err)

	ok, err := rds.SAdd(pkg.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SAdd(pkg.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SAdd(pkg.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SRem(pkg.GetTestKey(2), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SRem(pkg.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(pkg.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestRedisDataStructure_LPop(t *testing.T) {
	opts := model.DefaultOptions
	dir := "./../redis_test_file"
	opts.DirPath = dir

	db, err := storage.OpenWithOptions(opts)
	assert.Nil(t, err)

	rds, err := NewRedisDateStructure(db)
	assert.Nil(t, err)

	res, err := rds.LPush(pkg.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), res)
	res, err = rds.LPush(pkg.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), res)
	res, err = rds.LPush(pkg.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), res)

	val, err := rds.LPop(pkg.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = rds.LPop(pkg.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = rds.LPop(pkg.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
}

func TestRedisDataStructure_RPop(t *testing.T) {
	opts := model.DefaultOptions
	dir := "./../redis_test_file"
	opts.DirPath = dir

	db, err := storage.OpenWithOptions(opts)
	assert.Nil(t, err)

	rds, err := NewRedisDateStructure(db)
	assert.Nil(t, err)

	res, err := rds.RPush(pkg.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), res)
	res, err = rds.RPush(pkg.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), res)
	res, err = rds.RPush(pkg.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), res)

	val, err := rds.RPop(pkg.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = rds.RPop(pkg.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = rds.RPop(pkg.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
}
