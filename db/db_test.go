package db

import (
	"fmt"
	"log"
	"os"
	"testing"
)

func TestDB_Get_Put_Delete(t *testing.T) {
	db, err := Open(nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Put([]byte("hello"), []byte("world"))
	if err != nil {
		log.Fatal(err)
	}
	err = db.Put([]byte("hello1"), []byte("world1"))
	if err != nil {
		log.Fatal(err)
	}
	val, err := db.Get([]byte("hello"))
	if err != nil {
		log.Fatal(err)
	}
	if string(val) != "world" {
		t.Errorf("expected: %s, got: %s", "world", string(val))
	}

	err = db.Delete([]byte("hello"))
	if err != nil {
		log.Fatal(err)
	}
	val, err = db.Get([]byte("hello"))
	if err != ErrorKeyNotFound {
		t.Errorf("expected: %s, got: %s", "nil", string(val))
		log.Fatal(err)
	}
}

func BenchmarkDB_Get(b *testing.B) {
	dir, _ := os.MkdirTemp("/tmp/pluto", "db")
	db, err := Open(OptionDir(dir))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Put([]byte("hello"), []byte("world"))
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		_, _ = db.Get([]byte(fmt.Sprintf("%s-%d", "key", i)))
	}
}

func BenchmarkDB_Put(b *testing.B) {
	dir, _ := os.MkdirTemp("/tmp/pluto", "db")
	db, err := Open(OptionDir(dir))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < b.N; i++ {
		_ = db.Put([]byte(fmt.Sprintf("%s-%d", "key", i)), []byte(fmt.Sprintf("%s-%d", "val", i)))
	}
}

func BenchmarkDB_Delete(b *testing.B) {
	dir, _ := os.MkdirTemp("/tmp/pluto", "db")
	db, err := Open(OptionDir(dir))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < b.N; i++ {
		_ = db.Delete([]byte(fmt.Sprintf("%s-%d", "key", i)))
	}
}
