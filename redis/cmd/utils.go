package main

import "fmt"

func newWrongNumberOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}
