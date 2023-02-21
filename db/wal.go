package db

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

type WAL struct {
	Records []Record
	Dir     string
	mu      sync.Mutex

	f *os.File
}

type Record struct {
	Len    int
	Key    []byte
	Val    []byte
	Action Action
}

func (r *Record) String() string {
	return fmt.Sprintf("%d%s%s|%s", r.Len, r.Action, r.Key, r.Val)
}

func parseRecord(data []byte) ([]Record, error) {
	records := make([]Record, 0)
	for i := 0; i < len(data); {
		dataLen, ll := 0, 0
		for ; data[i] >= '0' && data[i] <= '9'; i++ {
			dataLen = dataLen*10 + int(data[i]-'0')
			ll++
		}
		action := transAction(data[i])
		i++
		key := make([]byte, 0)
		for data[i] != '|' {
			key = append(key, data[i])
			i++
		}
		i++
		val := make([]byte, 0)
		for j := 0; j < dataLen-len(key)-2; j++ {
			val = append(val, data[i])
			i++
		}
		records = append(records, Record{
			Len:    dataLen,
			Key:    key,
			Val:    val,
			Action: action,
		})
	}
	return records, nil
}

func (r *Record) Bytes() []byte {
	return []byte(r.String())
}

func (w *WAL) Append(record Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.Records = append(w.Records, record)
	return w.flush()
}

func (w *WAL) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	defer w.f.Close()
	return nil
}

func (w *WAL) len() int {
	return len(w.Records)
}

func (w *WAL) last() *Record {
	return &w.Records[len(w.Records)-1]
}

func (w *WAL) flush() error {
	n, err := w.f.Write(w.last().Bytes())
	if err != nil {
		log.Printf("write log error: %v, n: %d", err, n)
		return err
	}
	return nil
}

func (w *WAL) compact() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.Records = make([]Record, 0)
	w.f.Seek(0, 0)
	w.f.Truncate(0)
	return nil
}

func newWAL(dir string) *WAL {
	f, err := os.OpenFile(dir+"/wal.txt", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		log.Fatal(err)
	}
	records, err := parseRecord(data)
	if err != nil {
		log.Fatal(err)
	}
	// seek to end, so we can append
	if _, err := f.Seek(0, 2); err != nil {
		log.Fatal(err)
	}
	return &WAL{
		Records: records,
		Dir:     dir,
		f:       f,
	}
}
