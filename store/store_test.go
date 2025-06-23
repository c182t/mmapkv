package store

import (
	"testing"
)

func TestSetGet(t *testing.T) {
	tests := []struct {
		key      string
		value    int
		expected int
	}{
		{"key1", 1, 1},
		{"key2", 2, 2},
		{"key3", 3, 3},
	}

	store, err := NewStore[int]()
	if err != nil {
		panic(err)
	}
	defer store.Close()

	for _, test := range tests {
		err := store.Set(test.key, test.value)
		if err != nil {
			t.Errorf("store.Set(%s, %d) failed: %v", test.key, test.value, err)
		}
		result, err := store.Get(test.key)
		if err != nil {
			t.Errorf("store.Get(%s) failed: %v", test.key, err)
		}
		if result != test.expected {
			t.Errorf("store.Get(%s) failed: %d != %d", test.key, result, test.expected)
		}
	}
}

func TestDelete(t *testing.T) {
	tests := []struct {
		key      string
		value    int
		expected int
	}{
		{"key1", 1, 1},
		{"key2", 2, 2},
		{"key3", 3, 3},
	}

	store, err := NewStore[int]()
	if err != nil {
		panic(err)
	}
	defer store.Close()

	for _, test := range tests {
		err := store.Set(test.key, test.value)
		if err != nil {
			t.Errorf("store.Set(%s, %d) failed: %v", test.key, test.value, err)
		}
		result, err := store.Get(test.key)
		if err != nil {
			t.Errorf("store.Get(%s) failed: %v", test.key, err)
		}
		if result != test.expected {
			t.Errorf("store.Get(%s) failed: %d != %d", test.key, result, test.expected)
		}
	}

	for _, test := range tests {
		err := store.Delete(test.key)
		if err != nil {
			t.Errorf("store.Set(%s, %d) failed: %v", test.key, test.value, err)
		}
		result, err := store.Get(test.key)
		if err == nil {
			t.Errorf("store.Delete(%s) failed: %v != nill; result=%+v", test.key, err, result)
		}
	}
}
