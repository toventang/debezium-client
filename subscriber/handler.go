package subscriber

import (
	"github.com/Shopify/sarama"
	"github.com/toventang/debezium-client/adapter"
	"github.com/toventang/debezium-client/schema"
)

type handler struct {
	connector adapter.Connector
}

func (h handler) process(msg *sarama.ConsumerMessage) error {
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
		Schema:    s.Payload.Source.Schema,
		TableName: s.Payload.Source.Table,
	}
	switch s.Payload.Op {
	case schema.CREATE, schema.UPDATE:
		// Update it when this row exists, otherwise Add it as a new record
		exists := false
		if s.Payload.Op == schema.UPDATE {
			row.FieldItems = schema.GetPKValues(keyFields, s)
			exists = h.connector.Exists(row)
		}
		if exists {
			row.FieldItems = schema.GetFieldValues(keyFields, s, true)
			return h.connector.Update(row)
		}
		row.FieldItems = schema.GetFieldValues(keyFields, s, false)
		return h.connector.Create(row)
	case schema.DELETE:
		row.FieldItems = schema.GetFieldValues(keyFields, s, false)
		return h.connector.Delete(row)
	}

	return nil
}
