package connector

import (
	"context"
	"database/sql"

	"github.com/huandu/go-sqlbuilder"
	"github.com/rs/zerolog"
	"github.com/toventang/debezium-client/pkg/schema"
)

type parseSqlFunc func() (string, error)

func InsertContext(ctx context.Context, logger zerolog.Logger, dbType DbType, db *sql.DB, row *schema.Row) error {
	return exec(ctx, logger, db, func() (string, error) {
		return ParseInsertSQL(dbType, row.TableName, row.FieldItems)
	})
}

func UpdateContext(ctx context.Context, logger zerolog.Logger, dbType DbType, db *sql.DB, row *schema.Row) error {
	return exec(ctx, logger, db, func() (string, error) {
		return ParseUpdateSQL(dbType, row.TableName, row.FieldItems)
	})
}

func DeleteContext(ctx context.Context, logger zerolog.Logger, dbType DbType, db *sql.DB, row *schema.Row) error {
	return exec(ctx, logger, db, func() (string, error) {
		return ParseDeleteSQL(dbType, row.TableName, row.FieldItems)
	})
}

func ParseInsertSQL(dbType DbType, table string, fields []*schema.Field) (string, error) {
	var (
		cols    []string
		values  []interface{}
		builder = sqlbuilder.NewInsertBuilder()
	)
	for _, f := range fields {
		cols = append(cols, quote(f.Field))
		values = append(values, f.Value)
	}
	sql, args := builder.InsertInto(quote(table)).Cols(cols...).Values(values...).Build()
	return prepareSQL(dbType, sql, args)
}

func ParseUpdateSQL(dbType DbType, table string, fields []*schema.Field) (string, error) {
	builder := sqlbuilder.NewUpdateBuilder()
	for _, f := range fields {
		qf := quote(f.Field)
		if f.PrimaryKey {
			builder.Where(builder.Equal(qf, f.Value))
			continue
		}
		builder.SetMore(builder.Assign(qf, f.Value))
	}
	sql, args := builder.Update(quote(table)).Build()
	return prepareSQL(dbType, sql, args)
}

func ParseDeleteSQL(dbType DbType, table string, fields []*schema.Field) (string, error) {
	builder := sqlbuilder.NewDeleteBuilder()
	for _, f := range fields {
		if f.PrimaryKey {
			builder.Where(builder.Equal(quote(f.Field), f.Value))
			continue
		}
	}
	sql, args := builder.DeleteFrom(quote(table)).Build()
	return prepareSQL(dbType, sql, args)
}

func prepareSQL(dbType DbType, sql string, args []interface{}) (string, error) {
	switch dbType {
	case Mysql:
		return sqlbuilder.MySQL.Interpolate(sql, args)
	case Postgres:
		return sqlbuilder.PostgreSQL.Interpolate(sql, args)
	case SqlServer:
		return sqlbuilder.SQLServer.Interpolate(sql, args)
	}
	return "", ErrDbNotSupported(string(dbType))
}

func exec(ctx context.Context, logger zerolog.Logger, db *sql.DB, fn parseSqlFunc) error {
	query, err := fn()
	if err != nil {
		return err
	}
	logger.Debug().Msg(query)
	_, err = db.ExecContext(ctx, query)
	return err
}

func quote(name string) string {
	var (
		b []byte
		q = []byte("`")
	)
	b = append(q, name...)
	b = append(b, q...)
	return string(b)
}
