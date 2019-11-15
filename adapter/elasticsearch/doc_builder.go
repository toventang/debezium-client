package elasticsearch

import (
	"fmt"
	"strings"

	"github.com/toventang/debezium-client/adapter"
	"github.com/toventang/debezium-client/schema"
	"github.com/toventang/debezium-client/schema/mapping"
)

type DocBuilder struct {
	fieldMapper mapping.Mapper
}

func NewDocBuilder(fm string) DocBuilder {
	mapper := mapping.NewFieldMapper([]byte(fm))
	return DocBuilder{mapper}
}

func (db DocBuilder) BuildUpsertScript(row schema.Row) (string, string, string, error) {
	var builder strings.Builder
	var doc strings.Builder
	l := len(row.FieldItems)
	if l == 0 {
		return "", "", "", adapter.ErrNoRows
	}

	var docID string
	indexName := getIndexName(row)
	for i, f := range row.FieldItems {
		if f.PrimaryKey && docID == "" {
			docID = fmt.Sprint(f.Value)
		}

		fn := db.fieldMapper.GetFieldName(indexName, f.Field)
		v := adapter.GetDocValue(f)

		doc.Grow(4 + len(fn) + len(v))
		doc.WriteString(`"`)
		doc.WriteString(fn)
		doc.WriteString(`":`)
		doc.WriteString(v)

		if i < l-1 {
			doc.WriteString(",")
		}
	}

	d := doc.String()
	doc.Reset()

	builder.Grow(31 + len(d))
	builder.WriteString("{")
	builder.WriteString(`"doc":{`)
	builder.WriteString(d)
	builder.WriteString(`},"doc_as_upsert":true}`)

	return indexName, docID, builder.String(), nil
}

func getIndexName(row schema.Row) string {
	return fmt.Sprintf(`%s.%s`, row.Schema, row.TableName)
}
