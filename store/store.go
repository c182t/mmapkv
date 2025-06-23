package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"reflect"
	"unsafe"

	"golang.org/x/sys/unix"
)

type OffsetMap map[string]int

type Header struct {
	version    uint8
	lastOffset uint32
}

var headerSize uint16 = 5

type Store[T any] struct {
	header     Header
	headerSize uint16
	data       []byte
	offsetMap  OffsetMap
}

func NewStore[T any]() (*Store[T], error) {
	sysPageSize := os.Getpagesize()
	dbLogFileSize := 1 << 30

	fmt.Printf("Page size=%v\n", sysPageSize)

	var dbLogFileName = "/tmp/mmapkv.db.bin"
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

	header := Header{1, uint32(headerSize)}
	writeHeader(&header, data)

	return &Store[T]{header, headerSize, data, OffsetMap{}}, nil
}

func writeHeader(header *Header, data []byte) {
	headerBytes := append(ToBytes(header.version), ToBytes(header.lastOffset)...)
	if len(headerBytes) != int(headerSize) {
		panic(fmt.Sprintf("len(headerBytes) != 16; len(headerBytes) = %d", len(headerBytes)))
	}
	copy(data[:16], headerBytes)
}

func (s *Store[T]) Set(key string, value T) error {
	var fixedSizeValue any
	var keyBytes []byte
	var keyBytesLen int

	keyBytes = []byte(key)
	keyBytesLen = len(keyBytes)

	fmt.Println(keyBytesLen)

	var valueBytes []byte
	var err error

	switch v := any(value).(type) {
	case int:
		fixedSizeValue, err = ToFixedSize(v)
		if err != nil {
			return fmt.Errorf("unable to convert value to fixed-size type: %v", err)
		}
		valueBytes = ToBytes(fixedSizeValue)
	default:
		return fmt.Errorf("unsupported value type: %s", v)
	}

	nextValueOffset := uint16(len(keyBytes))
	nextKeyOffset := uint16(len(valueBytes))
	fmt.Println("nextValueOffset=", nextValueOffset)

	appendBytesSize := uint32(unsafe.Sizeof(nextValueOffset)) +
		uint32(len(keyBytes)) +
		uint32(unsafe.Sizeof(nextKeyOffset)) +
		uint32(len(valueBytes))

	valueOffsetBytes := ToBytes(nextValueOffset)
	fmt.Println("valueOffsetBytes=", valueOffsetBytes)
	keyOffsetBytes := ToBytes(nextKeyOffset)
	fmt.Println("keyOffsetBytes=", keyOffsetBytes)

	valuePairBytes := append(append(append(valueOffsetBytes, keyBytes...),
		keyOffsetBytes...), valueBytes...)

	copy(s.data[s.header.lastOffset:s.header.lastOffset+uint32(appendBytesSize)], valuePairBytes)
	s.header.lastOffset += appendBytesSize
	writeHeader(&s.header, s.data)

	err = unix.Msync(s.data, unix.MS_SYNC)
	if err != nil {
		return fmt.Errorf("failed to sync data to db log: %v", err)
	}

	s.header.lastOffset = s.header.lastOffset + appendBytesSize

	return nil
}

func ToFixedSize[T any](value T) (any, error) {
	switch v := any(value).(type) {
	case int:
		return int64(v), nil
	case uint:
		return uint64(v), nil
	case int8, int16, int32, int64,
		uint8, uint16, uint32, uint64,
		float32, float64:
		return v, nil
	default:
		return nil, fmt.Errorf("unsupported type: %T", v)
	}
}

func ToBytes[T any](value T) []byte {
	buf := new(bytes.Buffer)
	fmt.Println("Tobytes: ", value)
	err := binary.Write(buf, binary.LittleEndian, value)
	if err != nil {
		valueType := reflect.TypeOf(value)
		panic(fmt.Sprintf("cannot convert type [%s] to bytes: %v", valueType, err))
	}

	fmt.Println("buf: ", buf.Bytes())
	return buf.Bytes()
}

func (s *Store[T]) Get(key string) (T, error) {
	var nothing T
	return nothing, nil
}

func (s *Store[T]) Delete(key string) error {
	return nil
}

func (s *Store[T]) Close() error {
	return unix.Munmap(s.data)
}
