package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"reflect"
	"sync"
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
	valueType  string
}

type Store[T any] struct {
	header       Header
	data         []byte
	offsetMap    OffsetMap
	dbName       string
	mu           *sync.Mutex
	syncStrategy StoreSyncStrategy
}

type StoreTx[T any] struct {
	store      *Store[T]
	lastOffset uint32
	buffer     map[string]*T
}

func (store *Store[T]) Transaction(fn func(tx *StoreTx[T])) error {
	store.mu.Lock()
	tx := StoreTx[T]{store, store.header.lastOffset, make(map[string]*T)}
	store.mu.Unlock()

	fn(&tx)

	store.mu.Lock()
	if store.header.lastOffset != tx.lastOffset {
		return fmt.Errorf("unable to commit transaction; store was modified before transaction could be committed.")
	}

	for k, v := range tx.buffer {
		if v == nil {
			store.Delete(k)
		} else {
			store.Set(k, *v)
		}
	}

	store.mu.Unlock()

	return nil
}

func (tx *StoreTx[T]) Get(key string) (T, error) {
	if val, ok := tx.buffer[key]; ok {
		return *val, nil
	}

	tx.store.mu.Lock()
	val, err := tx.store.Get(key)
	tx.store.mu.Unlock()

	return val, err
}

func (tx *StoreTx[T]) Set(key string, value T) {
	v := value
	tx.buffer[key] = &v
}

func (tx *StoreTx[T]) Delete(key string) {
	tx.buffer[key] = nil
}

func DropStore(dbName string) error {
	var dbLogFileName = fmt.Sprintf("/tmp/mmapkv.%s.db.bin", dbName)
	err := os.Remove(dbLogFileName)
	if err != nil {
		return fmt.Errorf("unable to remove file: %s", dbLogFileName)
	}
	return nil
}

func NewStore[T any](dbName string, syncStrategy StoreSyncStrategy) (*Store[T], error) {
	sysPageSize := os.Getpagesize()
	dbLogFileSize := 1 << 30

	fmt.Printf("Page size=%v\n", sysPageSize)

	var dbLogFileName = fmt.Sprintf("/tmp/mmapkv.%s.db.bin", dbName)
	f, err := os.OpenFile(dbLogFileName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create mmapkv db log file [%s]: %v",
			dbLogFileName, err)
	}
	defer f.Close()

	if err := f.Truncate(int64(dbLogFileSize)); err != nil {
		return nil, fmt.Errorf("failed to resize mmapkv db log file [%s] to [%d]: %v",
			dbLogFileName, dbLogFileSize, err)
	}

	data, err := unix.Mmap(int(f.Fd()),
		0,
		int(dbLogFileSize),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED)

	if err != nil {
		return nil, fmt.Errorf("failed to mmap db log file [%s]: %v",
			dbLogFileName, err)
	}

	var nothing T
	header := Header{1, 0, reflect.TypeOf(nothing).String()}

	store := Store[T]{header, data, OffsetMap{}, dbName, &sync.Mutex{}, syncStrategy}

	headerSize := store.writeHeader()
	store.header.lastOffset = headerSize
	store.Sync()

	syncStrategy.OnStoreOpened(&store)

	return &store, nil
}

func (s *Store[T]) Get(key string) (T, error) {
	var nothing T

	valueSizeOffset, ok := s.offsetMap[key]
	if !ok {
		return nothing, fmt.Errorf("key [%s] not found", key)
	}

	// Copy value length
	var valueBytesSize ValueByteSize
	valueBytesSize = ValueByteSize(unsafe.Sizeof(valueBytesSize))
	valueByteSizeBytes := make([]byte, valueBytesSize)

	s.mu.Lock()
	copy(valueByteSizeBytes, s.data[valueSizeOffset:valueSizeOffset+uint32(valueBytesSize)])
	s.mu.Unlock()

	valueSize, err := FromBytes[uint16](valueByteSizeBytes)
	if err != nil {
		return nothing, fmt.Errorf("unable to read value bytes size from binary: %v", err)
	}

	if valueSize == 0 {
		return nothing, fmt.Errorf("key [%s] doesn't exist", key)
	}

	// Copy value
	valueSizeOffset += uint32(valueBytesSize)
	valueBytes := make([]byte, valueSize)

	s.mu.Lock()
	copy(valueBytes, s.data[valueSizeOffset:valueSizeOffset+uint32(valueSize)])
	s.mu.Unlock()

	value, err := FromBytes[T](valueBytes)
	if err != nil {
		return nothing, fmt.Errorf("unable to read value bytes from binary: %v", err)
	}

	return any(value).(T), nil
}

func (s *Store[T]) Set(key string, value T) error {
	return s.setValue(key, value)
}

func (s *Store[T]) Delete(key string) error {
	return s.setValue(key, Tombstone{})
}

func (s *Store[T]) Close() error {
	s.syncStrategy.OnCloseStore(s)
	return unix.Munmap(s.data)
}

func (s *Store[T]) Drop() error {
	if err := s.Close(); err != nil {
		return err
	}

	if err := DropStore(s.dbName); err != nil {
		return err
	}

	s.offsetMap = nil
	s.header = Header{}
	s.data = nil

	return nil
}

func (s *Store[T]) writeHeader() uint32 {
	headerBytes := append(append(ToBytes(s.header.version),
		ToBytes(s.header.lastOffset)...),
		[]byte(s.header.valueType)...)

	s.copyData(0, uint32(len(headerBytes)), headerBytes)
	return uint32(len(headerBytes))
}

func (s *Store[T]) setValue(key string, value any) error {
	var fixedSizeValue any
	keyBytes := []byte(key)

	var valueBytes []byte
	var err error

	switch v := any(value).(type) {
	case int:
		fixedSizeValue, err = ToFixedSize(v)
		if err != nil {
			return fmt.Errorf("unable to convert value to fixed-size type: %v", err)
		}
		valueBytes = ToBytes(fixedSizeValue)
	case float32, float64:
		valueBytes = ToBytes(v)
	case string:
		valueBytes = []byte(v)
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
	s.copyData(startOffset, endOffset, keyByteSizeBytes)

	// Copy key bytes
	startOffset = endOffset
	endOffset += uint32(len(keyBytes))
	s.copyData(startOffset, endOffset, keyBytes)

	// Copy value bytes length
	startOffset = endOffset
	endOffset += uint32(len(valueByteSizeBytes))
	s.copyData(startOffset, endOffset, valueByteSizeBytes)

	// Copy value bytes
	startOffset = endOffset
	endOffset += uint32(len(valueBytes))
	s.copyData(startOffset, endOffset, valueBytes)
	s.header.lastOffset = endOffset

	s.writeHeader()

	s.offsetMap[key] = s.header.lastOffset - uint32(len(valueBytes)) -
		uint32(len(valueByteSizeBytes))

	return nil
}

func (s *Store[T]) copyData(startOffset uint32, endOffset uint32, src []byte) {
	s.mu.Lock()
	copy(s.data[startOffset:endOffset], src)
	s.mu.Unlock()
	s.syncStrategy.OnDataCopyFinished(s)
}

func (s *Store[T]) Sync() {
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
	case float32:
		float32Bits := binary.LittleEndian.Uint32(data)
		return any(math.Float32frombits(float32Bits)).(T), nil
	case float64:
		float64Bits := binary.LittleEndian.Uint64(data)
		return any(math.Float64frombits(float64Bits)).(T), nil
	case string:
		return any(string(data)).(T), nil
	default:
		return value, fmt.Errorf("unsupported type in FromBytes")
	}

}

func ToBytes[T any](value T) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, value)
	if err != nil {
		valueType := reflect.TypeOf(value)
		panic(fmt.Sprintf("cannot convert type [%s] to bytes: %v", valueType, err))
	}
	return buf.Bytes()
}
