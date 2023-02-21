package db

import (
	"errors"
	"log"
	"sync"
)

type Action int

const (
	ActionPut Action = iota
	ActionGet
	ActionDelete

	LogLimit = 4 * 1024
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

func transAction(a byte) Action {
	switch a {
	case 'p':
		return ActionPut
	case 'g':
		return ActionGet
	case 'd':
		return ActionDelete
	default:
		return ActionPut
	}
}

var (
	ErrorKeyNotFound = errors.New("key not found")
)

type DB struct {
	Dir   string
	WAL   *WAL
	Table *Table
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

func newDefaultDB() *DB {
	return &DB{
		Dir:   ".",
		WAL:   newWAL("."),
		Table: &Table{Map: make(map[string][]byte)},
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
	if db.WAL.len() > LogLimit {
		if err := db.compact(); err != nil {
			log.Fatal(err)
		}
	}
	if err := db.WAL.Append(Record{
		Len:    len(key) + len(val) + 1 + 1,
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
	defer db.WAL.close()
	return nil
}

func (db *DB) sync() error {
	record := db.WAL.last()
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

func (db *DB) compact() error {
	// TODO
	return nil
}
