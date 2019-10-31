package client

import (
	"github.com/Shopify/sarama"
	"github.com/toventang/debezium-client/adapter"
	"github.com/toventang/debezium-client/schema"
)

type Consumer struct {
	connector adapter.Connector
}

func (c Consumer) handle(msg *sarama.ConsumerMessage) error {
	keyFields, err := schema.ParsePrimaryKeys(msg.Key)
	if err != nil {
		return err
	}

	if len(msg.Value) == 0 {
		return nil
	}
	s, err := schema.ParseValues(msg.Value)
	if err != nil {
		return err
	}

	row := schema.Row{
		Schema:     s.Payload.Source.Schema,
		TableName:  s.Payload.Source.Table,
		FieldItems: schema.GetFieldValues(keyFields, s),
	}
	switch s.Payload.Op {
	case schema.CREATE, schema.UPDATE:
		return c.connector.Write(row)
	case schema.DELETE:
		return c.connector.Delete(row)
	}

	return nil
}

func (c Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if err := c.handle(msg); err != nil {
			break
		} else {
			sess.MarkMessage(msg, "")
		}
	}
	return nil
}

func (Consumer) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (Consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}
