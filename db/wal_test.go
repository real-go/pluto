package db

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestWAL(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	wal := newWAL(dir)

	// Test Append method
	record1 := Record{
		Len:    12,
		Key:    []byte("hello"),
		Val:    []byte("world"),
		Action: ActionPut,
	}
	err = wal.Append(record1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	record2 := Record{
		Len:    12,
		Key:    []byte("hello"),
		Val:    []byte("world"),
		Action: ActionDelete,
	}
	err = wal.Append(record2)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Test parseRecord function
	data, err := os.ReadFile(filepath.Join(dir, ".wal"))
	if err != nil {
		t.Fatalf("failed to read WAL file: %v", err)
	}
	records, err := parseRecord(data)
	if err != nil {
		t.Fatalf("parseRecord failed: %v", err)
	}
	expectedRecords := []Record{record1, record2}
	if !reflect.DeepEqual(records, expectedRecords) {
		t.Fatalf("parseRecord returned incorrect result, expected: %v, got: %v", expectedRecords, records)
	}

	// Test compact method
	err = wal.compact()
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}
	data, err = os.ReadFile(filepath.Join(dir, ".wal"))
	if err != nil {
		t.Fatalf("failed to read WAL file after compact: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("WAL file should be empty after compact, got length: %d", len(data))
	}
}
