package main

import (
	"errors"
	"fmt"
)

func E(e error, msg string) error {
	return errors.New(fmt.Sprintf("%s - %s", msg, e.Error()))
}
