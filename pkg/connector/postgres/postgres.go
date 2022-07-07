package postgres

import (
	"context"
	"database/sql"

	_ "github.com/lib/pq"
	"github.com/rs/zerolog"

	"github.com/toventang/debezium-client/pkg/connector"
	"github.com/toventang/debezium-client/pkg/schema"
)

type postgres struct {
	client  *sql.DB
	logger  zerolog.Logger
	options connector.Options
}

func NewPostgres(datasource string, logger zerolog.Logger, opts ...connector.Option) (connector.Connector, error) {
	opt := connector.NewOptions(opts...)
	db, err := sql.Open("postgre", datasource)
	if err != nil {
		return nil, err
	}
	return &postgres{client: db, logger: logger, options: opt}, nil
}

func (db *postgres) Insert(ctx context.Context, row *schema.Row) error {
	return connector.InsertContext(ctx, db.logger, connector.Postgres, db.client, row)
}

func (db *postgres) Update(ctx context.Context, row *schema.Row) error {
	return connector.UpdateContext(ctx, db.logger, connector.Postgres, db.client, row)
}

func (db *postgres) Delete(ctx context.Context, row *schema.Row) error {
	return connector.DeleteContext(ctx, db.logger, connector.Postgres, db.client, row)
}

func (db *postgres) Close(ctx context.Context) error {
	return db.client.Close()
}

func (db *postgres) GetRowsFromEvent(evt *schema.ChangedEvent) (*schema.Row, error) {
	return connector.GetFieldsWithMapping(db, evt, evt.GetFieldMappingWithTable(db.options.Tables))
}

func (db *postgres) GetPrimaryKey(tableName string) (string, error) {
	return connector.GetPrimaryKey(db.options.Tables, tableName)
}
