package db

import (
	"errors"
	"io"
	"log"
	"os"
	"sync"
)

type DB struct {
	Dir      string
	WriteLog *WriteLog
	Table    *Table
}

type Table struct {
	Map map[string][]byte
	mu  sync.Mutex
}

func (t *Table) Put(key []byte, val []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Map[string(key)] = val
	return nil
}

func (t *Table) Get(key []byte) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	val, ok := t.Map[string(key)]
	if !ok {
		return nil, ErrorKeyNotFound
	}
	return val, nil
}

func (t *Table) Delete(key []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.Map, string(key))
	return nil
}

type WriteLog struct {
	Records []LogRecord
	Dir     string
	mu      sync.Mutex

	f *os.File
}

func (r *LogRecord) String() string {
	return "##" + r.Action.String() + "#" + string(r.Key) + "#" + string(r.Val) + "##"
}

func (r *LogRecord) Bytes() []byte {
	return []byte(r.String())
}

func (w *WriteLog) Append(record LogRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.Records = append(w.Records, record)
	return w.flush()
}

func parseLogRecord(data []byte) ([]LogRecord, error) {
	records := make([]LogRecord, 0)
	for i := 0; i < len(data); {
		if data[i] != '#' {
			return nil, errors.New("invalid log record")
		}
		i += 2
		var action Action
		switch data[i] {
		case 'p':
			action = ActionPut
		case 'g':
			action = ActionGet
		case 'd':
			action = ActionDelete
		default:
			return nil, errors.New("invalid log record")
		}
		i += 2
		var key []byte
		for ; i < len(data); i++ {
			if data[i] == '#' {
				break
			}
			key = append(key, data[i])
		}
		i++
		var val []byte
		for ; i < len(data); i++ {
			if data[i] == '#' {
				break
			}
			val = append(val, data[i])
		}
		i += 2
		records = append(records, LogRecord{
			Key:    key,
			Val:    val,
			Action: action,
		})
	}
	return records, nil
}

func (w *WriteLog) flush() error {
	n, err := w.f.Write(w.last().Bytes())
	if err != nil {
		log.Printf("write log error: %v, n: %d", err, n)
		return err
	}
	return nil
}

func newWriteLog(dir string) *WriteLog {
	f, err := os.OpenFile(dir+"/write.log", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		log.Fatal(err)
	}
	records, err := parseLogRecord(data)
	if err != nil {
		log.Fatal(err)
	}
	// seek to end, so we can append
	if _, err := f.Seek(0, 2); err != nil {
		log.Fatal(err)
	}
	return &WriteLog{
		Records: records,
		Dir:     dir,
		f:       f,
	}
}

type LogRecord struct {
	Key    []byte
	Val    []byte
	Action Action
}

type Action int

const (
	ActionPut Action = iota
	ActionGet
	ActionDelete

	LOGLIMIT = 4 * 1024
)

var (
	ErrorKeyNotFound = errors.New("key not found")
)

func (a Action) String() string {
	switch a {
	case ActionPut:
		return "p"
	case ActionGet:
		return "g"
	case ActionDelete:
		return "d"
	default:
		return "u"
	}
}

func newDefaultDB() *DB {
	return &DB{
		Dir:      ".",
		WriteLog: newWriteLog("."),
		Table:    &Table{Map: make(map[string][]byte)},
	}
}

type Option func(*DB) error

func Open(options []Option) (*DB, error) {
	db := newDefaultDB()
	for _, option := range options {
		option(db)
	}
	return db, nil
}

func OptionDir(dir string) Option {
	return func(db *DB) error {
		db.Dir = dir
		return nil
	}
}

func (db *DB) Put(key []byte, val []byte) error {
	return db.append(key, val, ActionPut)
}

func (db *DB) Delete(key []byte) error {
	return db.append(key, nil, ActionDelete)
}

func (db *DB) Get(key []byte) ([]byte, error) {
	return db.search(key)
}

func (db *DB) append(key []byte, val []byte, action Action) error {
	if db.WriteLog.len() > LOGLIMIT {
		if err := db.compact(); err != nil {
			log.Fatal(err)
		}
	}
	if err := db.WriteLog.Append(LogRecord{
		Key:    key,
		Val:    val,
		Action: action,
	}); err != nil {
		return err
	}

	return db.sync()
}

func (db *DB) search(key []byte) ([]byte, error) {
	if val, err := db.Table.Get(key); err == nil {
		return val, nil
	}
	return nil, nil
}

func (db *DB) Close() error {
	defer db.WriteLog.close()
	return nil
}

// sync is used to sync the write log to table
func (db *DB) sync() error {
	record := db.WriteLog.last()
	switch record.Action {
	case ActionPut:
		if err := db.Table.Put(record.Key, record.Val); err != nil {
			return err
		}
	case ActionDelete:
		if err := db.Table.Delete(record.Key); err != nil {
			return err
		}
	}

	return nil
}

func (w *WriteLog) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	defer w.f.Close()
	return nil
}

func (w *WriteLog) len() int {
	return len(w.Records)
}

func (w *WriteLog) last() *LogRecord {
	return &w.Records[len(w.Records)-1]
}

func (db *DB) compact() error {
	// TODO
	return nil
}
