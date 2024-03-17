package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetTestKey(t *testing.T) {
	testKey1 := GetTestKey(1)
	assert.Equal(t, []byte("bitcask-go-key-000000001"), testKey1)

	testKey2 := GetTestKey(0)
	assert.Equal(t, []byte("bitcask-go-key-000000000"), testKey2)

	testKey3 := GetTestKey(1111)
	assert.Equal(t, []byte("bitcask-go-key-000001111"), testKey3)
}

func TestGetTestValue(t *testing.T) {
	//testValue1 := GetTestValue(5)
	for i := 0; i < 10; i++ {
		assert.NotNil(t, GetTestValue(5))
		t.Log(string(GetTestValue(5)))
	}

}
