package main

import (
	"fmt"
	"kv_projects/conf"
	"kv_projects/db"
)

func main() {
	opts := conf.DefaultOptions
	opts.DirPath = "./temp"
	initDB, err := db.Open(opts)
	if err != nil {
		panic(err)
	}

	//err = initDB.Put([]byte("name"), []byte("lll"))
	//if err != nil {
	//	panic(err)
	//}
	val, err := initDB.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("val = %s", string(val))

	//err = initDB.Delete([]byte("name"))
	//if err != nil {
	//	panic(err)
	//}

}
