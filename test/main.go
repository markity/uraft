package main

import (
	"github.com/tidwall/wal"
)

func Persist(newInfo string) {
	log, err := wal.Open("raft_state.log", nil)
	if err != nil {
		panic(err)
	}

	idx, err := log.LastIndex()
	if err != nil {
		panic(err)
	}

	log.Write(idx+1, []byte(newInfo))
	log.TruncateFront(idx + 1)
}

func GetInfo() string {
	log, err := wal.Open("raft_state.log", nil)
	if err != nil {
		panic(err)
	}

	batch := &wal.Batch{}
	batch.Write(1, []byte("hello"))
	batch.Write(2, []byte("world"))

	log.WriteBatch(batch)

	return ""
}

func main() {
	println(GetInfo())
}
