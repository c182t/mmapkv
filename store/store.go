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

type OffsetMap map[string]uint32
type KeyByteSize uint16
type ValueByteSize uint16
type Tombstone struct{}

type Header struct {
	version    uint8
	lastOffset uint32
	valueType  uint8
}

var headerSize uint16 = 6

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

	header := Header{1, uint32(headerSize), 1}
	writeHeader(&header, data)
	store := Store[T]{header, headerSize, data, OffsetMap{}}

	store.syncData()

	return &store, nil
}

func writeHeader(header *Header, data []byte) {
	headerBytes := append(append(ToBytes(header.version), ToBytes(header.lastOffset)...), ToBytes(header.valueType)...)
	if len(headerBytes) != int(headerSize) {
		panic(fmt.Sprintf("len(headerBytes) != int(headerSize); len(headerBytes)=%d; int(headerSize)=%d", int(headerSize), len(headerBytes)))
	}
	copy(data[:16], headerBytes)
}

func (s *Store[T]) setValue(key string, value any) error {
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
	case Tombstone:
		valueBytes = nil
	default:
		return fmt.Errorf("unsupported value type: %s", v)
	}

	keyByteSize := KeyByteSize(len(keyBytes))
	valueByteSize := ValueByteSize(len(valueBytes))

	keyByteSizeBytes := ToBytes(keyByteSize)
	valueByteSizeBytes := ToBytes(valueByteSize)

	// Copy key bytes length
	startOffset := s.header.lastOffset
	endOffset := startOffset + uint32(len(keyByteSizeBytes))
	copy(s.data[startOffset:endOffset], keyByteSizeBytes)

	// Copy key bytes
	startOffset = endOffset
	endOffset += uint32(len(keyBytes))
	copy(s.data[startOffset:endOffset], keyBytes)

	// Copy value bytes length
	startOffset = endOffset
	endOffset += uint32(len(valueByteSizeBytes))
	copy(s.data[startOffset:endOffset], valueByteSizeBytes)

	// Copy value bytes
	startOffset = endOffset
	endOffset += uint32(len(valueBytes))
	copy(s.data[startOffset:endOffset], valueBytes)

	s.header.lastOffset = endOffset

	writeHeader(&s.header, s.data)
	s.syncData()

	s.offsetMap[key] = s.header.lastOffset - uint32(len(valueBytes)) - uint32(len(valueByteSizeBytes))

	return nil
}
func (s *Store[T]) Set(key string, value T) error {
	return s.setValue(key, value)
}

func (s *Store[T]) syncData() {
	err := unix.Msync(s.data, unix.MS_SYNC)
	if err != nil {
		panic(fmt.Errorf("failed to sync data to db log: %v", err))
	}
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

func FromBytes[T any](data []byte) (T, error) {
	var value T
	switch any(value).(type) {
	case uint16:
		return any(uint16(binary.LittleEndian.Uint16(data))).(T), nil
	case int:
		return any(int(int64(binary.LittleEndian.Uint64(data)))).(T), nil
	default:
		return value, fmt.Errorf("unsupported type in FromBytes")
	}

}

func ToBytes[T any](value T) []byte {
	buf := new(bytes.Buffer)
	//fmt.Println("Tobytes: ", value)
	err := binary.Write(buf, binary.LittleEndian, value)
	if err != nil {
		valueType := reflect.TypeOf(value)
		panic(fmt.Sprintf("cannot convert type [%s] to bytes: %v", valueType, err))
	}

	//fmt.Println("buf: ", buf.Bytes())
	return buf.Bytes()
}

func (s *Store[T]) Get(key string) (T, error) {
	var nothing T

	valueSizeOffset, ok := s.offsetMap[key]
	//valueSizeOffset = 0x26
	if !ok {
		return nothing, fmt.Errorf("key [%s] not found", key)
	}
	//fmt.Printf("valueSizeOffset=%d (%X)\n", valueSizeOffset, valueSizeOffset)

	// Copy value length
	var valueBytesSize ValueByteSize
	valueBytesSize = ValueByteSize(unsafe.Sizeof(valueBytesSize))
	valueByteSizeBytes := make([]byte, valueBytesSize)
	copy(valueByteSizeBytes, s.data[valueSizeOffset:valueSizeOffset+uint32(valueBytesSize)])
	//fmt.Println("valueByteSizeBytes=", hex.EncodeToString(valueByteSizeBytes))
	valueSize, err := FromBytes[uint16](valueByteSizeBytes)
	if err != nil {
		return nothing, fmt.Errorf("unable to read value bytes size from binary")
	}

	if valueSize == 0 {
		return nothing, fmt.Errorf("key [%s] doesn't exist", key)
	}

	// Copy value
	valueSizeOffset += uint32(valueBytesSize)
	valueBytes := make([]byte, valueSize)
	copy(valueBytes, s.data[valueSizeOffset:valueSizeOffset+uint32(valueSize)])
	//fmt.Println("valueBytes=", valueSize)
	value, err := FromBytes[int](valueBytes)
	if err != nil {
		return nothing, fmt.Errorf("unable to read value bytes from binary")
	}
	//fmt.Println("value=", value)
	return any(value).(T), nil
}

func (s *Store[T]) Delete(key string) error {
	return s.setValue(key, Tombstone{})
}

func (s *Store[T]) Close() error {
	return unix.Munmap(s.data)
}
