package main

import (
	"log"
	"pluto/db"
)

func main() {
	db, err := db.Open(nil)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		_ = db.Put([]byte("k1"), []byte("v1"))
		_ = db.Put([]byte("k2"), []byte("v2"))
		_ = db.Put([]byte("k3"), []byte("v3"))

		// get
		val, _ := db.Get([]byte("k1"))
		log.Printf("get k1: %s", val)

		// delete
		_ = db.Delete([]byte("k1"))
		log.Printf("delete k1")

		_, err = db.Get([]byte("k1"))
		if err != nil {
			log.Printf("get k1: %s", err)
		}
	}
	db.Close()
}
