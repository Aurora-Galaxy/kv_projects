package errs

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpdateFailed      = errors.New("failed to update Index")
	ErrKeyNotFound            = errors.New("key is not found in database")
	ErrDataFileNotFound       = errors.New("data file is not found")
	ErrDataAlreadyDeleted     = errors.New("data already deleted")
	ErrDataDirectoryCorrupted = errors.New("the database directory maybe corrupted")
	ErrInvalidCRC             = errors.New("invalid crc value, logRecord maybe corrupted")
)
