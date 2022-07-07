package elasticsearch

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/rs/zerolog"
	"github.com/toventang/debezium-client/pkg/connector"
	"github.com/toventang/debezium-client/pkg/schema"
)

type elasticSearch struct {
	client  *elasticsearch.Client
	logger  zerolog.Logger
	options connector.Options
}

func NewElasticSearch(address string, logger zerolog.Logger, opts ...connector.Option) (connector.Connector, error) {
	opt := connector.NewOptions(opts...)
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: strings.Split(address, ","),
		Username:  opt.Username,
		Password:  opt.Password,
	})
	if err != nil {
		return nil, err
	}
	if _, err = client.Ping(); err != nil {
		return nil, err
	}

	return &elasticSearch{
		client:  client,
		logger:  logger,
		options: opt,
	}, nil
}

func (es *elasticSearch) Insert(ctx context.Context, row *schema.Row) error {
	return es.Update(ctx, row)
}

func (es *elasticSearch) Update(ctx context.Context, row *schema.Row) error {
	script, err := BuildUpsertScript(row)
	if err != nil {
		return nil
	}
	es.logger.Debug().Msg(script.query)

	rsp, err := es.client.Update(script.index, script.docId, strings.NewReader(script.query), es.client.Update.WithTimeout(time.Millisecond*es.options.Timeout))
	if err != nil {
		return err
	}
	defer rsp.Body.Close()

	if rsp.IsError() {
		return errors.New(rsp.String())
	}

	return nil
}

func (es *elasticSearch) Delete(ctx context.Context, row *schema.Row) error {
	var docID string
	indexName := getIndexName(row)
	for _, f := range row.FieldItems {
		if f.PrimaryKey && len(docID) == 0 {
			docID = fmt.Sprint(f.Value)
			break
		}
	}
	if len(docID) == 0 {
		return fmt.Errorf("index '%s' has no primary key", indexName)
	}

	rsp, err := es.client.Delete(indexName, docID, es.client.Delete.WithTimeout(es.options.Timeout))
	if err != nil {
		return err
	}
	defer rsp.Body.Close()

	if rsp.IsError() && rsp.StatusCode != http.StatusNotFound {
		return errors.New(rsp.String())
	}

	return nil
}

func (es *elasticSearch) Close(ctx context.Context) error {
	return nil
}

func (db *elasticSearch) GetRowsFromEvent(evt *schema.ChangedEvent) (*schema.Row, error) {
	return connector.GetFieldsWithMapping(db, evt, evt.GetFieldMappingWithTable(db.options.Tables))
}

func (db *elasticSearch) GetPrimaryKey(tableName string) (string, error) {
	return connector.GetPrimaryKey(db.options.Tables, tableName)
}
