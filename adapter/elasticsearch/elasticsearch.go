package elasticsearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"

	elastic "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/toventang/debezium-client/adapter"
	"github.com/toventang/debezium-client/schema"
)

type elasticSearch struct {
	client *elastic.Client

	options adapter.Options
}

func NewElasticSearch(opts adapter.Options) (adapter.Connector, error) {
	client, err := elastic.NewClient(elastic.Config{
		Addresses: opts.Addresses,
		Username:  opts.Username,
		Password:  opts.Password,
	})
	if err != nil {
		return nil, err
	}
	if _, err = client.Ping(); err != nil {
		return nil, err
	}

	return elasticSearch{client, opts}, nil
}

func (es elasticSearch) Init() error {
	var wg sync.WaitGroup
	for _, t := range es.options.Tables {
		wg.Add(1)

		go func(t string) {
			exists, err := es.tableExists(t)
			if err != nil {
				es.Close()
				panic(err)
			}
			if !exists {
				if err := es.createTable(t); err != nil {
					es.Close()
					panic(err)
				}
			}
			wg.Done()
		}(t)
	}
	wg.Wait()

	return nil
}

func (es elasticSearch) tableExists(name string) (bool, error) {
	rsp, err := es.client.Indices.Exists([]string{name})
	if err != nil {
		return false, err
	}
	defer rsp.Body.Close()

	return rsp.StatusCode == http.StatusOK, nil
}

func (es elasticSearch) createTable(name string) error {
	rsp, err := es.client.Indices.Create(name)
	if err != nil {
		return err
	}
	defer rsp.Body.Close()

	if rsp.IsError() {
		return errors.New(rsp.String())
	}
	if rsp.StatusCode != http.StatusOK {
		return fmt.Errorf("create index error, index: %s", name)
	}
	return nil
}

func (es elasticSearch) Create(row schema.Row) error {
	indexName, docID, body, err := getRequestParams(row)
	if err != nil {
		return nil
	}

	req := esapi.CreateRequest{
		Index:      indexName,
		DocumentID: docID,
		Body:       body,
		Timeout:    es.options.Timeout,
	}

	ctx, cancel := adapter.Context(es.options.Timeout)
	defer cancel()

	rsp, err := req.Do(ctx, es.client)
	if err != nil {
		return fmt.Errorf("has error occured when Write: %s", err)
	}
	defer rsp.Body.Close()

	if rsp.IsError() {
		return errors.New(rsp.String())
	}

	return nil
}

func (es elasticSearch) Update(row schema.Row) error {
	indexName, docID, body, err := getRequestParams(row)
	if err != nil {
		return nil
	}

	req := esapi.UpdateRequest{
		Index:      indexName,
		DocumentID: docID,
		Body:       body,
		Timeout:    es.options.Timeout,
	}

	ctx, cancel := adapter.Context(es.options.Timeout)
	defer cancel()

	rsp, err := req.Do(ctx, es.client)
	if err != nil {
		return fmt.Errorf("has error occured when Write: %s", err)
	}
	defer rsp.Body.Close()

	if rsp.IsError() {
		log.Println("Elasticsearch error: ", rsp.String())
		return errors.New(rsp.String())
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
		Timeout:    es.options.Timeout,
	}

	ctx, cancel := adapter.Context(es.options.Timeout)
	defer cancel()

	rsp, err := req.Do(ctx, es.client)
	if err != nil {
		return fmt.Errorf("has error occured when Delete: %s", err)
	}
	defer rsp.Body.Close()

	if rsp.IsError() && rsp.StatusCode != http.StatusNotFound {
		return errors.New(rsp.String())
	}

	return nil
}

func (es elasticSearch) Exists(row schema.Row) bool {
	var docID string
	for _, f := range row.FieldItems {
		if f.PrimaryKey {
			docID = fmt.Sprint(f.Value)
			break
		}
	}
	indexName := fmt.Sprintf("%s.%s", row.Schema, row.TableName)

	rsp, err := es.client.Exists(indexName, docID)
	if err != nil {
		return false
	}
	defer rsp.Body.Close()

	return !rsp.IsError() && rsp.StatusCode == http.StatusOK
}

func (es elasticSearch) Close() error {
	return nil
}

func getValue(f schema.FieldItem) string {
	b, err := json.Marshal(f.Value)
	if err != nil {
		return ""
	}

	switch f.Type {
	case "int64":
		return fmt.Sprintf(`"%s"`, string(b))
	}
	return string(b)
}

func getRequestParams(row schema.Row) (string, string, io.Reader, error) {
	var builder strings.Builder
	var docID string
	length := len(row.FieldItems)
	if length == 0 {
		return "", "", nil, NoFieldEffect
	}

	indexName := fmt.Sprintf("%s.%s", row.Schema, row.TableName)
	builder.WriteString(`{"doc":{`)
	for i, f := range row.FieldItems {
		if f.PrimaryKey && docID == "" {
			docID = fmt.Sprint(f.Value)
		}
		v := getValue(f)
		builder.Grow(len(v) + len(f.Field) + 4)
		builder.WriteString(`"`)
		builder.WriteString(f.Field)
		builder.WriteString(`":`)
		builder.WriteString(v)
		if i < length-1 {
			builder.WriteString(",")
		}
	}
	builder.WriteString(`}}`)

	return indexName, docID, strings.NewReader(builder.String()), nil
}
