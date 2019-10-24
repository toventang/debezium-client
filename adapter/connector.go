package adapter

import (
	"github.com/toventang/debezium-client/schema"
)

type ConnectorType int8

const (
	MYSQL    ConnectorType = 1
	POSTGRES               = 2
	MSSQL                  = 3
	ELASTIC                = 4
)

type Connector interface {
	Write(row schema.Row) error
	Delete(row schema.Row) error
	Close() error
}
