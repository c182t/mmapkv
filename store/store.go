package store

type Store[T any] struct{}

func (s *Store[T]) Initialize() error {
	//func Mmap(fd int, offset int64, length int, prot int, flags int) (data []byte, err error)
	return nil
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
