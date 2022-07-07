package connector

import (
	"errors"
	"fmt"
)

var (
	ErrNoRows = errors.New("no rows affected")
)

func ErrDbNotSupported(dbType string) error {
	return fmt.Errorf("connector '%s' is not supported yet", dbType)
}
