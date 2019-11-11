package mssql

import (
	"github.com/toventang/debezium-client/adapter"
	"github.com/toventang/debezium-client/schema"
)

type Mssql struct{}

func NewMssql() (adapter.Connector, error) {
	sql := Mssql{}

	return sql, nil
}

func (sql Mssql) Init() error {
	return nil
}

func (sql Mssql) Create(row schema.Row) error {
	return nil
}

func (sql Mssql) Update(row schema.Row) error {
	return nil
}

func (sql Mssql) Delete(row schema.Row) error {
	return nil
}

func (sql Mssql) Exists(row schema.Row) bool {
	return false
}

func (sql Mssql) Close() error {
	return nil
}
