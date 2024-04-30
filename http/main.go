package main

import (
	"encoding/json"
	"fmt"
	"kv_projects/conf"
	"kv_projects/db"
	"log"
	"net/http"
	"path/filepath"
)

var Db *db.DB

func init() {
	var err error
	opts := conf.DefaultOptions
	dir := filepath.Join("./temp", "bitcask-go-http")
	opts.DataFileSize = 32 * 1024 * 1024
	opts.DirPath = dir
	Db, err = db.Open(opts)
	if err != nil {
		panic("db init failed")
	}
	fmt.Println("db init success")
}

func handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not be allowed", http.StatusMethodNotAllowed)
		return
	}
	var data map[string]string
	err := json.NewDecoder(r.Body).Decode(&data)
	//fmt.Println(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	for key, value := range data {
		err := Db.Put([]byte(key), []byte(value))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to put value in db , err:%v\n", err)
			return
		}
	}
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not be allowed", http.StatusMethodNotAllowed)
		return
	}
	key := r.URL.Query().Get("key")
	value, err := Db.Get([]byte(key))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get value from db , err:%v\n", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(string(value))
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not be allowed", http.StatusMethodNotAllowed)
		return
	}
	key := r.URL.Query().Get("key")
	err := Db.Delete([]byte(key))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to delete value from db , err:%v\n", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode("OK")
}

func handleListKeys(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not be allowed", http.StatusMethodNotAllowed)
		return
	}
	listKeys := Db.ListKeys()
	w.Header().Set("Content-Type", "application/json")
	var result []string
	for _, v := range listKeys {
		result = append(result, string(v))
	}
	_ = json.NewEncoder(w).Encode(result)
}

func handleStat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not be allowed", http.StatusMethodNotAllowed)
		return
	}
	stat := Db.Stat()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stat)
}

func main() {
	http.HandleFunc("/bitcask/put", handlePut)
	http.HandleFunc("/bitcask/get", handleGet)
	http.HandleFunc("/bitcask/delete", handleDelete)
	http.HandleFunc("/bitcask/listkeys", handleListKeys)
	http.HandleFunc("/bitcask/stat", handleStat)
	_ = http.ListenAndServe("localhost:8080", nil)
}
