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
	ErrExceedMaxBatchNum      = errors.New("exceed the max batch num")
	ErrMergeIsProgress        = errors.New("merge is in progress, try again later")
	ErrDatabaseIsUsing        = errors.New("the database directory is used by another process")
	ErrMergeRatioUnreached    = errors.New("the merge ratio do not reach option ratio")
	ErrNotEnoughSpaceForMerge = errors.New("no enough disk space for merge")
)
