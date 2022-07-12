package consumer

import (
	"context"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/segmentio/kafka-go"
	"github.com/toventang/debezium-client/internal/config"
	"github.com/toventang/debezium-client/pkg/connector"
	"github.com/toventang/debezium-client/pkg/connector/elasticsearch"
	"github.com/toventang/debezium-client/pkg/connector/mysql"
	"github.com/toventang/debezium-client/pkg/connector/postgres"
	"github.com/toventang/debezium-client/pkg/schema"
)

var (
	processTime = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "dbzc_process_duration_seconds",
			Help: "Histogram of handling latency of the message consumer that handled by the server.",
		},
		[]string{"schema", "table", "action"})

	processTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dbzc_process_msg_total",
			Help: "All messages processed total.",
		},
		[]string{"schema", "table", "action", "status"})
)

type Consumer struct {
	Config     *config.Config
	logger     zerolog.Logger
	ctx        context.Context
	kafka      *kafka.Reader
	wg         sync.WaitGroup
	connectors []connector.Connector
	timeout    int64
}

type actionFunc func(context.Context, *schema.Row) error

func NewConsumer(ctx context.Context, logger zerolog.Logger, c *config.Config) *Consumer {
	conn := newConnectors(c.Connectors, logger)
	return &Consumer{
		Config:     c,
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

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		c.startPrometheusAgent()
	}()
}

func (c *Consumer) Stop() error {
	return c.kafka.Close()
}

func (c *Consumer) startPrometheusAgent() {
	if len(c.Config.PrometheusConf.Path) == 0 || len(c.Config.PrometheusConf.Addr) == 0 {
		return
	}

	http.Handle(c.Config.PrometheusConf.Path, promhttp.Handler())
	c.logger.Info().Msgf("Starting prometheus agent at %s", c.Config.PrometheusConf.Addr)
	if err := http.ListenAndServe(c.Config.PrometheusConf.Addr, nil); err != nil {
		c.logger.Fatal().Msg(err.Error())
		os.Exit(1)
	}
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
			if err := c.handle(ctx, c.connectors[i], "insert", evt, func(ctx context.Context, row *schema.Row) error {
				return c.connectors[i].Insert(ctx, row)
			}); err != nil {
				c.logger.Error().Msg(err.Error())
			}
		}
	case schema.UPDATE:
		for i := range c.connectors {
			if err := c.handle(ctx, c.connectors[i], "update", evt, func(ctx context.Context, row *schema.Row) error {
				return c.connectors[i].Update(ctx, row)
			}); err != nil {
				c.logger.Error().Msg(err.Error())
			}
		}
	case schema.DELETE:
		for i := range c.connectors {
			if err := c.handle(ctx, c.connectors[i], "delete", evt, func(ctx context.Context, row *schema.Row) error {
				return c.connectors[i].Delete(ctx, row)
			}); err != nil {
				c.logger.Error().Msg(err.Error())
			}
		}
	}

	return nil
}

func (c *Consumer) handle(ctx context.Context, connector connector.Connector, action string, evt *schema.ChangedEvent, fn actionFunc) error {
	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		processTime.WithLabelValues(evt.Payload.Source.DB, evt.Payload.Source.Table, action).Observe(v)
	}))
	defer timer.ObserveDuration()

	row, err := connector.GetRowsFromEvent(evt)
	if err != nil {
		return err
	}

	err = fn(ctx, row)
	if err != nil {
		processTotal.WithLabelValues(evt.Payload.Source.DB, evt.Payload.Source.Table, action, "fail").Inc()
		return err
	}
	processTotal.WithLabelValues(evt.Payload.Source.DB, evt.Payload.Source.Table, action, "ok").Inc()

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
