package main

import (
	"fmt"
	mmapkv "mmapkv/store"
)

func main() {
	store := mmapkv.Store[int]{}
	var err error
	var val int

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
	fmt.Printf("Delete [key1]\n")

	val, err = store.Get("key1")
	if err != nil {
		fmt.Printf("Get [key1Value] error: %v\n", err)
	}

}
