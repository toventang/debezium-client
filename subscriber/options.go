package subscriber

import "github.com/toventang/debezium-client/adapter"

type Options struct {
	Addresses []string
	GroupID   string
	Topics    []string

	connector adapter.Connector
}

type Option func(*Options)

func NewOptions(opt ...Option) Options {
	opts := Options{}
	for _, o := range opt {
		o(&opts)
	}
	return opts
}

func NewOption(addresses []string, groupID string, topics []string) Option {
	return func(opts *Options) {
		opts.Addresses = addresses
		opts.GroupID = groupID
		opts.Topics = topics
	}
}

func WithConnector(conn adapter.Connector) Option {
	return func(opts *Options) {
		opts.connector = conn
	}
}
