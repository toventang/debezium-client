package adapter

import (
	"github.com/toventang/debezium-client/schema"
)

type ConnectorType int8

const (
	MYSQL    ConnectorType = 0
	POSTGRES               = 1
	MSSQL                  = 2
	ELASTIC                = 3
)

func ParseConnectorType(t string) ConnectorType {
	switch t {
	case "mysql":
		return MYSQL
	case "postgres":
		return POSTGRES
	case "mssql":
		return MSSQL
	default:
		return ELASTIC
	}
}

type Connector interface {
	Init() error
	Create(row schema.Row) error
	Update(row schema.Row) error
	Delete(row schema.Row) error
	Exists(row schema.Row) bool
	Close() error
}
