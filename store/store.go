package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"

	"os"

	"golang.org/x/sys/unix"
)

type OffsetMap map[string]int

type Store[T any] struct {
	data       []byte
	offsetMap  OffsetMap
	lastOffset uint32
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

	return &Store[T]{data, OffsetMap{}, 0}, nil
}

func (s *Store[T]) Set(key string, value T) error {
	var keyBytes []byte
	var keyBytesLen int

	keyBytes = []byte(key)
	keyBytesLen = len(keyBytes)

	fmt.Println(keyBytesLen)

	var valueBytes []byte
	switch v := any(value).(type) {
	case int:
		valueBytes = IntToBytes(v)
	default:
		return fmt.Errorf("Unsupported value type: %s", v)
	}

	nextValueOffset := len(keyBytes)
	nextKeyOffset := len(valueBytes)

	appendBytesSize := int(unsafe.Sizeof(nextValueOffset)) +
		len(keyBytes) +
		int(unsafe.Sizeof(nextKeyOffset)) +
		len(valueBytes)

	//valuePairBytes := make([]byte, appendBytesSize)
	valueOffsetBytes := IntToBytes(nextValueOffset)
	keyOffsetBytes := IntToBytes(nextKeyOffset)

	valuePairBytes := append(append(append(valueOffsetBytes, keyBytes...),
		keyOffsetBytes...), valueBytes...)

	copy(s.data[s.lastOffset:appendBytesSize], valuePairBytes)

	err := unix.Msync(s.data, unix.MS_SYNC)
	if err != nil {
		return fmt.Errorf("failed to sync data to db log: %v", err)
	}

	return nil
}

func IntToBytes(intValue int) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int64(intValue))
	return buf.Bytes()
}

func (s *Store[T]) Get(key string) (T, error) {
	var nothing T
	return nothing, nil
}

func (s *Store[T]) Delete(key string) error {
	return nil
}
