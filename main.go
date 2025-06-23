package main

import (
	"fmt"
	mmapkv "mmapkv/store"
)

func main() {
	var err error
	var val int

	var store *mmapkv.Store[int]

	store, err = mmapkv.NewStore[int]()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Set [key1=%d] \n", 1)
	err = store.Set("key1", 1)

	fmt.Printf("Set [key2=%d]\n", 2)
	err = store.Set("key2", 2)

	fmt.Printf("Set [key3=%d]\n", 3)
	err = store.Set("key3", 3)

	val, err = store.Get("key1")
	fmt.Printf("Get [key1=%d]\n", val)

	val, err = store.Get("key2")
	fmt.Printf("Get [key2=%d]\n", val)

	val, err = store.Get("key3")
	fmt.Printf("Get [key3=%d]\n", val)

	err = store.Delete("key1")
	if err != nil {
		fmt.Printf("Delete [key1] error: %v\n", err)
	}
	fmt.Printf("Delete [key1]\n")

	val, err = store.Get("key1")
	if err != nil {
		fmt.Printf("Get [key1] error: %v\n", err)
	} else {
		fmt.Printf("Get [key1] still exists, but should be deleted (!) [%d]\n", val)
	}

	store.Close()
}
