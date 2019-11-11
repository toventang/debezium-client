package mysql

import (
	"github.com/toventang/debezium-client/adapter"
	"github.com/toventang/debezium-client/schema"
)

type Mysql struct{}

func NewMysql() (adapter.Connector, error) {
	mysql := Mysql{}

	return mysql, nil
}

func (sql Mysql) Init() error {
	return nil
}

func (sql Mysql) Create(row schema.Row) error {
	return nil
}

func (sql Mysql) Update(row schema.Row) error {
	return nil
}

func (sql Mysql) Delete(row schema.Row) error {
	return nil
}

func (sql Mysql) Exists(row schema.Row) bool {
	return false
}

func (sql Mysql) Close() error {
	return nil
}
