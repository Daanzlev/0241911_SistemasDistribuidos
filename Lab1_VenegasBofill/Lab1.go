package main

import (
	"sync"
)

type
// User struct
User struct {
}

type Log struct {
	mu     sync.Mutex
	record []Record
}

type Record struct {
	value  []byte `json:"value"`
	offset int64  `json:"offset"`
}
