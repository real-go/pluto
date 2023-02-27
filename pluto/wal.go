package db

type WriteAheadLog struct{}

type WriteAheadLogEntry struct{}

type WriteAheadLogInterface interface {
	Append(entry interface{}) error
	Truncate() error
}
