package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
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
		return -1
	}
}

var (
	ErrorKeyNotFound = errors.New("key not found")
)

type DB struct {
	WAL   *WAL
	Table *Table
}

type Table struct {
	Dir       string
	MemTable  map[string][]byte
	Immutable map[string][]byte

	level int
	mu    sync.Mutex
}

type KV struct {
	Key []byte `json:"key"`
	Val []byte `json:"val"`
}

type KVs []KV

func (kvs KVs) Len() int {
	return len(kvs)
}
func (kvs KVs) Less(i, j int) bool {
	return kvs[i].less(kvs[j])
}
func (kvs KVs) Swap(i, j int) {
	kvs[i], kvs[j] = kvs[j], kvs[i]
}

func (kv *KV) less(other KV) bool {
	return string(kv.Key) < string(other.Key)
}

func (t *Table) Put(key []byte, val []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.MemTable[string(key)] = val
	return nil
}

func (t *Table) Get(key []byte) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	val, ok := t.MemTable[string(key)]
	if !ok {
		val, ok = t.Immutable[string(key)]
		if !ok {
			return nil, ErrorKeyNotFound
		}
		return val, nil
	}
	return val, nil
}

func (t *Table) Delete(key []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.MemTable, string(key))
	return nil
}

func (t *Table) writeSSTable() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	data := make([]KV, 0, len(t.Immutable))
	for k, v := range t.Immutable {
		data = append(data, KV{
			Key: []byte(k),
			Val: v,
		})
	}
	sort.Sort(KVs(data))
	f, err := os.Create(fmt.Sprintf("%s/%d.sst", t.Dir, t.level))
	if err != nil {
		return err
	}
	defer f.Close()

	m := make(map[string]string)
	for _, kv := range data {
		m[string(kv.Key)] = string(kv.Val)
	}
	mData, _ := json.Marshal(m)
	_, err = f.Write(mData)
	if err != nil {
		return err
	}
	t.level++
	return nil
}

func newDefaultDB() *DB {
	return &DB{
		Table: &Table{
			Dir:      ".",
			MemTable: make(map[string][]byte),
			level:    0,
		},
	}
}

func OptionDir(dir string) Option {
	return func(db *DB) error {
		db.Table.Dir = dir
		return nil
	}
}

type Option func(*DB) error

func Open(options ...Option) (*DB, error) {
	db := newDefaultDB()
	for _, option := range options {
		_ = option(db)
	}

	db.WAL = newWAL(db.Table.Dir)
	return db, nil
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
	val, err := db.Table.Get(key)
	if err != nil {
		return nil, err
	}
	return val, nil
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
	if err := db.WAL.compact(); err != nil {
		return err
	}

	if len(db.Table.Immutable) > 0 {
		if err := db.writeSSTable(); err != nil {
			return err
		}
	}
	db.Table.Immutable = db.Table.MemTable
	db.Table.MemTable = make(map[string][]byte)
	return nil
}

func (db *DB) writeSSTable() error {
	if err := db.Table.writeSSTable(); err != nil {
		return err
	}
	return nil
}
