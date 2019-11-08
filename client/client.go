package client

import (
	"context"
	"fmt"
	"log"

	"github.com/toventang/debezium-client/adapter"
	"github.com/toventang/debezium-client/adapter/elasticsearch"
	sub "github.com/toventang/debezium-client/subscriber"
)

type Client struct {
	subscriber sub.Subscriber
	connector  adapter.Connector
}

func NewClient(opts Options) (*Client, error) {
	var c adapter.Connector
	var err error
	switch opts.AdapterOptions.ConnectorType {
	case adapter.ELASTIC:
		c, err = elasticsearch.NewElasticSearch(opts.AdapterOptions)
		if err != nil {
			panic(err)
		}
	default:
		panic(fmt.Sprintf(`the connector "%v" is not supported now`, opts.AdapterOptions.ConnectorType))
	}
	err = c.Init()
	if err != nil {
		panic(err)
	}

	subOpt := sub.NewOption(opts.SubscriberOptions.Addresses, opts.SubscriberOptions.GroupID, opts.SubscriberOptions.Topics)
	s, err := sub.NewSubscriber(subOpt, sub.WithConnector(c))
	if err != nil {
		return nil, err
	}

	adp := &Client{
		subscriber: s,
		connector:  c,
	}

	return adp, nil
}

func (d *Client) Start(ctx context.Context) error {
	log.Println("debezium client was started")
	for {
		err := make(chan error, 1)
		go func() {
			err <- d.subscriber.Subscribe(ctx)
		}()

		select {
		case <-ctx.Done():
			d.subscriber.Close()
			return ctx.Err()
		case e := <-err:
			if e != nil {
				return e
			}
		}
	}
}

func (d *Client) Close() error {
	if err := d.subscriber.Close(); err != nil {
		return err
	}
	return d.connector.Close()
}
