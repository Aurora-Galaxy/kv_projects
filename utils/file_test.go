package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAvailableDiskSize(t *testing.T) {
	size, err := AvailableDiskSize()
	assert.Nil(t, err)
	t.Log(size)
}
