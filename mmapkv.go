package mmapkv

type KVStore[T any] interface {
	Set(key string, value T) error
	Get(key string) error
	Delete(key string) error
}
