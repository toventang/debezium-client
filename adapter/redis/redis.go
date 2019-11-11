package redis

import (
	"github.com/toventang/debezium-client/adapter"
	"github.com/toventang/debezium-client/schema"
)

type Redis struct{}

func NewRedis() (adapter.Connector, error) {
	redis := Redis{}

	return redis, nil
}

func (r Redis) Init() error {
	return nil
}

func (r Redis) Create(row schema.Row) error {
	return nil
}

func (r Redis) Update(row schema.Row) error {
	return nil
}

func (r Redis) Delete(row schema.Row) error {
	return nil
}

func (r Redis) Exists(row schema.Row) bool {
	return false
}

func (r Redis) Close() error {
	return nil
}
