package client

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/toventang/debezium-client/adapter"
	"github.com/toventang/debezium-client/adapter/elasticsearch"
)

type Client struct {
	topics []string

	consumer  sarama.ConsumerGroup
	connector adapter.Connector
}

func NewClient(ko KafkaOptions, bo adapter.Options) (*Client, error) {
	conf := sarama.NewConfig()
	conf.Version = sarama.V0_11_0_0
	consumer, err := sarama.NewConsumerGroup(ko.Addresses, ko.GroupID, conf)
	if err != nil {
		return nil, err
	}

	var connector adapter.Connector
	switch bo.ConnectorType {
	case adapter.ELASTIC:
		connector, err = elasticsearch.NewElasticSearch(adapter.WithAddresses(bo.Addresses))
		if err != nil {
			panic(err)
		}
	default:
		panic(`"ConnectorType" must be specified`)
	}
	defer func() {
		if r := recover(); r != nil {
			consumer.Close()
		}
	}()

	adp := &Client{
		topics:    ko.Topics,
		consumer:  consumer,
		connector: connector,
	}

	return adp, nil
}

func (d *Client) Start(ctx context.Context) error {
	for {
		err := make(chan error, 1)
		go func() {
			err <- d.consumer.Consume(ctx, d.topics, &Consumer{d.connector})
		}()

		select {
		case <-ctx.Done():
			d.consumer.Close()
			return ctx.Err()
		case e := <-err:
			if e != nil {
				return e
			}
		}
	}
}

func (d *Client) Close() error {
	if err := d.consumer.Close(); err != nil {
		return err
	}
	return d.connector.Close()
}
