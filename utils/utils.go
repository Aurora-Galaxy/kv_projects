package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	// 随机源
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")
)

// 获取测试使用的key
func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("bitcask-go-key-%09d", i))
}

func GetTestValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	return []byte("bitcask-go-value-" + string(b))
}
