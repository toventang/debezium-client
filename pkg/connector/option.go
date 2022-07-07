package connector

import (
	"time"

	"github.com/toventang/debezium-client/pkg/schema"
)

type Options struct {
	Timeout            time.Duration
	Database           string
	Username, Password string
	Tables             []schema.Table
}

type Option func(*Options)

func NewOptions(opts ...Option) Options {
	opt := Options{}
	for _, o := range opts {
		o(&opt)
	}
	return opt
}

func WithDatabase(dbName string) Option {
	return func(opt *Options) {
		opt.Database = dbName
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(opt *Options) {
		opt.Timeout = timeout
	}
}

func WithTable(tables ...schema.Table) Option {
	return func(opt *Options) {
		opt.Tables = tables
	}
}

func WithAuth(username, password string) Option {
	return func(opt *Options) {
		opt.Username = username
		opt.Password = password
	}
}
