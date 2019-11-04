package client

import (
	"github.com/toventang/debezium-client/adapter"
	"github.com/toventang/debezium-client/subscriber"
)

type Options struct {
	SubscriberOptions subscriber.Options
	AdapterOptions    adapter.Options
}
