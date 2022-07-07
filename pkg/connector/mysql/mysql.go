package mysql

import (
	"context"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog"

	"github.com/toventang/debezium-client/pkg/connector"
	"github.com/toventang/debezium-client/pkg/schema"
)

type mysql struct {
	client  *sql.DB
	logger  zerolog.Logger
	options connector.Options
}

func NewMysql(datasource string, logger zerolog.Logger, opts ...connector.Option) (connector.Connector, error) {
	opt := connector.NewOptions(opts...)
	db, err := sql.Open("mysql", datasource)
	if err != nil {
		return nil, err
	}
	return &mysql{client: db, logger: logger, options: opt}, nil
}

func (db *mysql) Insert(ctx context.Context, row *schema.Row) error {
	return connector.InsertContext(ctx, db.logger, connector.Mysql, db.client, row)
}

func (db *mysql) Update(ctx context.Context, row *schema.Row) error {
	return connector.UpdateContext(ctx, db.logger, connector.Mysql, db.client, row)
}

func (db *mysql) Delete(ctx context.Context, row *schema.Row) error {
	return connector.DeleteContext(ctx, db.logger, connector.Mysql, db.client, row)
}

func (db *mysql) Close(ctx context.Context) error {
	return db.client.Close()
}

func (db *mysql) GetRowsFromEvent(evt *schema.ChangedEvent) (*schema.Row, error) {
	return connector.GetFieldsWithMapping(db, evt, evt.GetFieldMappingWithTable(db.options.Tables))
}

func (db *mysql) GetPrimaryKey(tableName string) (string, error) {
	return connector.GetPrimaryKey(db.options.Tables, tableName)
}
