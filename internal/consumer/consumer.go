package consumer

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/segmentio/kafka-go"
	"github.com/toventang/debezium-client/internal/config"
	"github.com/toventang/debezium-client/pkg/connector"
	"github.com/toventang/debezium-client/pkg/connector/elasticsearch"
	"github.com/toventang/debezium-client/pkg/connector/mysql"
	"github.com/toventang/debezium-client/pkg/connector/postgres"
	"github.com/toventang/debezium-client/pkg/schema"
)

type Consumer struct {
	logger     zerolog.Logger
	ctx        context.Context
	kafka      *kafka.Reader
	wg         sync.WaitGroup
	connectors []connector.Connector
	timeout    int64
}

func NewConsumer(ctx context.Context, logger zerolog.Logger, c *config.Config) *Consumer {
	conn := newConnectors(c.Connectors, logger)
	return &Consumer{
		logger:     logger,
		ctx:        ctx,
		kafka:      newKafkaReader(c),
		timeout:    c.Timeout,
		connectors: conn,
	}
}

func (c *Consumer) Start() {
	c.wg.Add(1)

	go func() {
		defer c.wg.Done()

		for {
			msg, err := c.kafka.FetchMessage(c.ctx)
			if err == io.EOF || err == io.ErrClosedPipe {
				return
			}
			if err != nil {
				c.logger.Error().Msg(err.Error())
				continue
			}

			if err = c.consume(msg); err == nil {
				c.kafka.CommitMessages(c.ctx, msg)
			}
		}
	}()
}

func (c *Consumer) Stop() error {
	return c.kafka.Close()
}

func (c *Consumer) consume(msg kafka.Message) error {
	// decode
	evt, err := schema.NewChangedEvent(msg.Value)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(c.timeout))
	defer cancel()

	// consume
	switch evt.Payload.Op {
	case schema.CREATE:
		for i := range c.connectors {
			row, err := c.connectors[i].GetRowsFromEvent(evt)
			if err != nil {
				c.logger.Error().Msg(err.Error())
				continue
			}

			err = c.connectors[i].Insert(ctx, row)
			if err != nil {
				c.logger.Error().Msg(err.Error())
			}
		}
	case schema.UPDATE:
		for i := range c.connectors {
			row, err := c.connectors[i].GetRowsFromEvent(evt)
			if err != nil {
				c.logger.Error().Msg(err.Error())
				continue
			}

			err = c.connectors[i].Update(ctx, row)
			if err != nil {
				c.logger.Error().Msg(err.Error())
			}
		}
	case schema.DELETE:
		for i := range c.connectors {
			row, err := c.connectors[i].GetRowsFromEvent(evt)
			if err != nil {
				c.logger.Error().Msg(err.Error())
				continue
			}

			c.connectors[i].Delete(ctx, row)
		}
	}

	return nil
}

func newConnectors(conf []config.ConnectorConf, logger zerolog.Logger) []connector.Connector {
	connectors := make([]connector.Connector, len(conf))
	for i, n := range conf {
		conn, err := newConnector(n, logger)
		if err != nil {
			panic(err)
		}
		connectors[i] = conn
	}

	return connectors
}

func newConnector(c config.ConnectorConf, logger zerolog.Logger) (connector.Connector, error) {
	tables := make([]schema.Table, len(c.Tables))
	for i, tc := range c.Tables {
		fm, err := schema.ParseFieldMaps(tc.FieldMapping)
		if err != nil {
			continue
		}

		tables[i] = schema.Table{
			Name:            tc.Name,
			PrimaryKey:      tc.PrimaryKey,
			FieldMappingMap: fm,
		}
	}

	switch connector.DbType(c.Type) {
	case connector.Mysql:
		return mysql.NewMysql(c.DataSource, logger, connector.WithTable(tables...))
	case connector.Postgres:
		return postgres.NewPostgres(c.DataSource, logger, connector.WithTable(tables...))
	case connector.Elasticsearch:
		return elasticsearch.NewElasticSearch(c.DataSource, logger, connector.WithTable(tables...))
	}

	return nil, connector.ErrDbNotSupported(c.Type)
}

func newKafkaReader(c *config.Config) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     c.Kafka.Brokers,
		GroupID:     c.Kafka.Group,
		GroupTopics: c.Kafka.Topics,
		MinBytes:    c.Kafka.MinBytes,
		MaxBytes:    c.Kafka.MaxBytes,
	})
}
