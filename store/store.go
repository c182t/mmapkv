package store

import (
	"fmt"

	"os"

	"golang.org/x/sys/unix"
)

type OffsetMap map[string]int

type Store[T any] struct {
	data      []byte
	offsetMap OffsetMap
}

func NewStore[T any]() (*Store[T], error) {
	sysPageSize := os.Getpagesize()
	dbLogFileSize := 1 << 30

	fmt.Printf("Page size=%v\n", sysPageSize)

	var dbLogFileName = "/tmp/mmapkv.db.log"
	f, err := os.OpenFile(dbLogFileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create mmapkv db log file [%s]: %v", dbLogFileName, err)
	}
	defer f.Close()

	if err := f.Truncate(int64(dbLogFileSize)); err != nil {
		return nil, fmt.Errorf("failed to resize mmapkv db log file [%s] to [%d]: %v", dbLogFileName, dbLogFileSize, err)
	}

	data, err := unix.Mmap(int(f.Fd()),
		0,
		int(dbLogFileSize),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED)

	if err != nil {
		return nil, fmt.Errorf("failed to mmap db log file [%s]: %v", dbLogFileName, err)
	}

	return &Store[T]{data, OffsetMap{}}, nil
}

func (s *Store[T]) Set(key string, value T) error {
	return nil
}

func (s *Store[T]) Get(key string) (T, error) {
	var nothing T
	return nothing, nil
}

func (s *Store[T]) Delete(key string) error {
	return nil
}
