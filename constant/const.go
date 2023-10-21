package constant

type LogRecordStatus byte

const (
	LogRecordNormal LogRecordStatus = 100 + iota
	LogRecordDelete
)
