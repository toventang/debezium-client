package postgres

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/lib/pq"

	"github.com/toventang/debezium-client/adapter"
	"github.com/toventang/debezium-client/schema"
)

const violatesUniqueErrCode = "23505"

type Postgres struct {
	db *sql.DB

	options       adapter.Options
	createdTables []string
}

func NewPostgres(opts adapter.Options) (adapter.Connector, error) {
	pg := Postgres{options: opts}
	s := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		pg.options.Username, pg.options.Password, pg.options.Addresses[0], pg.options.Database)
	db, err := sql.Open("postgres", s)
	if err != nil {
		return pg, err
	}
	pg.db = db

	return pg, nil
}

func (pg Postgres) Init() error {
	return nil
}

func (pg Postgres) Create(row schema.Row) error {
	sql := prepareUpsertSQL(row)
	return pg.exec(sql)
}

func (pg Postgres) Update(row schema.Row) error {
	sql := prepareUpsertSQL(row)
	return pg.exec(sql)
}

func (pg Postgres) Delete(row schema.Row) error {
	var sql strings.Builder
	tn := getTableName(row)
	pk, v := adapter.GetPKFieldValue(row)

	sql.Grow(len(tn) + len(pk) + 22)
	sql.WriteString("DELETE FROM ")
	sql.WriteString(tn)
	sql.WriteString(` WHERE "`)
	sql.WriteString(pk)
	sql.WriteString(`"=`)
	sql.WriteString(v)

	s := sql.String()

	log.Println("delete sql: ", s)
	return pg.exec(s)
}

func (pg Postgres) Exists(row schema.Row) bool {
	// return false to use upsert strategy
	return false
}

func (pg Postgres) Close() error {
	if pg.db != nil {
		return pg.db.Close()
	}
	return nil
}

func (pg Postgres) exec(sql string) error {
	ctx, cancel := adapter.Context(pg.options.Timeout)
	defer cancel()

	r, err := pg.db.ExecContext(ctx, sql)
	if err != nil {
		if e, ok := err.(*pq.Error); !ok {
			return e
		} else if e.Code == violatesUniqueErrCode {
			return nil
		}
	}

	rows, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if rows <= 0 {
		return adapter.ErrNoRows
	}
	return nil
}
