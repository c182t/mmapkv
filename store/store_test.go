package store

import (
	"fmt"
	"testing"
	"time"
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

	DropStore("TestSetGet")

	store, err := NewStore[int]("TestSetGet")
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

func TestSetGetFloat32(t *testing.T) {
	tests := []struct {
		key      string
		value    float32
		expected float32
	}{
		{"key1", 1.111, 1.111},
		{"key2", 2.222, 2.222},
		{"key3", 3.333, 3.333},
	}

	DropStore("TestSetGetFloat32")

	store, err := NewStore[float32]("TestSetGetFloat32")
	if err != nil {
		panic(err)
	}
	defer store.Close()

	for _, test := range tests {
		err := store.Set(test.key, test.value)
		if err != nil {
			t.Errorf("store.Set(%s, %.2f) failed: %v", test.key, test.value, err)
		}
		result, err := store.Get(test.key)
		if err != nil {
			t.Errorf("store.Get(%s) failed: %v", test.key, err)
		}
		if result != test.expected {
			t.Errorf("store.Get(%s) failed: %.2f != %.2f", test.key, result, test.expected)
		}
	}
}

func TestSetGetFloat64(t *testing.T) {
	tests := []struct {
		key      string
		value    float64
		expected float64
	}{
		{"key1", (1 << 24) + 1.000001, (1 << 24) + 1.000001},
		{"key2", (1 << 24) + 2.000002, (1 << 24) + 2.000002},
		{"key3", (1 << 24) + 3.000003, (1 << 24) + 3.000003},
	}

	DropStore("TestSetGetFloat64")

	store, err := NewStore[float64]("TestSetGetFloat64")
	if err != nil {
		panic(err)
	}
	defer store.Close()

	for _, test := range tests {
		err := store.Set(test.key, test.value)
		if err != nil {
			t.Errorf("store.Set(%s, %f) failed: %v", test.key, test.value, err)
		}
		result, err := store.Get(test.key)
		if err != nil {
			t.Errorf("store.Get(%s) failed: %v", test.key, err)
		}
		if result != test.expected {
			t.Errorf("store.Get(%s) failed: %f != %f", test.key, result, test.expected)
		}
	}
}

func TestSetGetString(t *testing.T) {
	tests := []struct {
		key      string
		value    string
		expected string
	}{
		{"Images", "Imágenes", "Imágenes"},
		{"and", "y", "y"},
		{"Words", "Palabras", "Palabras"},
		{"Pull", "引っ張る", "引っ張る"},
		{"Me", "私", "私"},
		{"Under", "下", "下"},
	}

	DropStore("TestSetGetString")

	store, err := NewStore[string]("TestSetGetString")
	if err != nil {
		panic(err)
	}
	defer store.Close()

	for _, test := range tests {
		err := store.Set(test.key, test.value)
		if err != nil {
			t.Errorf("store.Set(%s, %s) failed: %v", test.key, test.value, err)
		}
		result, err := store.Get(test.key)
		if err != nil {
			t.Errorf("store.Get(%s) failed: %v", test.key, err)
		}
		if result != test.expected {
			t.Errorf("store.Get(%s) failed: %s != %s", test.key, result, test.expected)
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

	DropStore("TestDelete")

	store, err := NewStore[int]("TestDelete")
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

func TestLargeSetGet(t *testing.T) {
	tests := []struct {
		keyPrefix string
		maxValue  int
	}{{"key", 100000}}

	DropStore("TestLargeSetGet")

	store, err := NewStore[int]("TestLargeSetGet")
	if err != nil {
		panic(err)
	}
	defer store.Close()

	startTime := time.Now()
	for _, test := range tests {
		for i := 0; i < test.maxValue; i++ {
			err := store.Set(fmt.Sprintf("%s%d", test.keyPrefix, i), i)
			if err != nil {
				t.Errorf("store.Set(%s, %d) failed: %v", test.keyPrefix, i, err)
			}
		}
	}
	duration := time.Since(startTime)
	fmt.Printf("TestLargeSetGet Duration: %v", duration)
}
