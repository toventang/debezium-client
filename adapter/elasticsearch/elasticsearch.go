package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	elastic "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/toventang/debezium-client/adapter"
	"github.com/toventang/debezium-client/schema"
)

type elasticSearch struct {
	client *elastic.Client
}

func NewElasticSearch(opts ...adapter.Option) (adapter.Connector, error) {
	options := &adapter.Options{}
	for _, o := range opts {
		o(options)
	}
	client, err := elastic.NewClient(elastic.Config{
		Addresses: options.Addresses,
		Username:  options.Username,
		Password:  options.Password,
	})
	if err != nil {
		return nil, err
	}
	if _, err = client.Ping(); err != nil {
		return nil, err
	}
	return elasticSearch{client}, nil
}

func (es elasticSearch) Write(row schema.Row) error {
	var builder strings.Builder
	var docID string
	indexName := fmt.Sprintf("%s.%s", row.Schema, row.TableName)
	length := len(row.FieldItems)
	builder.WriteString(`{`)
	for i, f := range row.FieldItems {
		b, err := json.Marshal(f.Value)
		if err != nil {
			return err
		}
		builder.WriteString(`"`)
		builder.WriteString(f.Field)
		builder.WriteString(`":`)
		builder.WriteString(string(b))
		if i < length-1 {
			builder.WriteString(",")
		}

		if f.PrimaryKey && docID == "" {
			docID = fmt.Sprint(f.Value)
		}
	}
	builder.WriteString(`}`)

	req := esapi.IndexRequest{
		Index:      indexName,
		DocumentID: docID,
		Body:       strings.NewReader(builder.String()),
	}

	res, err := req.Do(context.Background(), es.client)
	if err != nil {
		return fmt.Errorf("has error occured when Write: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf(res.String())
	}

	return nil
}

func (es elasticSearch) Delete(row schema.Row) error {
	var docID string
	indexName := fmt.Sprintf("%s.%s", row.Schema, row.TableName)
	for _, f := range row.FieldItems {
		if f.PrimaryKey && docID == "" {
			docID = fmt.Sprint(f.Value)
		}
	}
	req := esapi.DeleteRequest{
		Index:      indexName,
		DocumentID: docID,
	}

	res, err := req.Do(context.Background(), es.client)
	if err != nil {
		return fmt.Errorf("has error occured when Delete: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf(res.String())
	}

	return nil
}

func (es elasticSearch) Close() error {
	return nil
}
