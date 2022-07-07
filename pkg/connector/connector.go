package connector

import (
	"context"
	"fmt"

	"github.com/toventang/debezium-client/pkg/schema"
)

type DbType string

const (
	Mysql         DbType = "mysql"
	SqlServer     DbType = "sqlserver"
	Postgres      DbType = "postgres"
	Oracle        DbType = "oracle"
	Cassandra     DbType = "cassandra"
	ClickHouse    DbType = "clickhouse"
	MongoDB       DbType = "mongo"
	Elasticsearch DbType = "elasticsearch"
	// Redis         DbType = "redis"
)

type Connector interface {
	Insert(context.Context, *schema.Row) error
	Update(context.Context, *schema.Row) error
	Delete(context.Context, *schema.Row) error
	Close(context.Context) error
	GetRowsFromEvent(evt *schema.ChangedEvent) (*schema.Row, error)
	GetPrimaryKey(tableName string) (string, error)
}

func GetPrimaryKey(tables []schema.Table, tableName string) (string, error) {
	for _, t := range tables {
		if t.Name == tableName {
			return t.PrimaryKey, nil
		}
	}
	return "", fmt.Errorf("table '%s' has not config", tableName)
}

func GetFieldsWithMapping(db Connector, evt *schema.ChangedEvent, fieldMapping []*schema.FieldMap) (*schema.Row, error) {
	pk, err := db.GetPrimaryKey(evt.Payload.Source.Table)
	if err != nil {
		return nil, err
	}

	fields := evt.ChangedFieldValues(pk)
	for _, f := range fields {
		for _, m := range fieldMapping {
			if f.Field == m.Source {
				f.Field = m.Target
			}
		}
	}

	return &schema.Row{
		Schema:     evt.Payload.Source.DB,
		TableName:  evt.Payload.Source.Table,
		Query:      evt.Payload.Source.Query,
		DDL:        evt.Payload.DDL,
		FieldItems: fields,
	}, nil
}
