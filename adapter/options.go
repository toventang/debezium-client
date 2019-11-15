package adapter

import (
	"time"
)

type Options struct {
	ConnectorType      ConnectorType
	Addresses          []string
	Timeout            time.Duration
	Database           string
	Username, Password string
	Tables             []string

	FieldMapping string
}

type Option func(*Options)

func NewOptions(opts ...Option) Options {
	opt := Options{}
	for _, o := range opts {
		o(&opt)
	}
	return opt
}

func WithConnectorType(t ConnectorType) Option {
	return func(opt *Options) {
		opt.ConnectorType = t
	}
}

func WithAddresses(addresses []string) Option {
	return func(opt *Options) {
		opt.Addresses = addresses
	}
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

func WithTable(tableName ...string) Option {
	return func(opt *Options) {
		opt.Tables = tableName
	}
}

func WithAuth(username, password string) Option {
	return func(opt *Options) {
		opt.Username = username
		opt.Password = password
	}
}
