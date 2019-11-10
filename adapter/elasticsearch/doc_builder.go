package elasticsearch

import (
	"fmt"
	"strings"

	"github.com/toventang/debezium-client/adapter"
	"github.com/toventang/debezium-client/schema"
)

func buildUpsertScript(row schema.Row) (string, string, string, error) {
	var builder strings.Builder
	var source strings.Builder
	var doc strings.Builder
	l := len(row.FieldItems)
	if l == 0 {
		return "", "", "", adapter.ErrNoRows
	}

	var docID string
	indexName := fmt.Sprintf("%s.%s", row.Schema, row.TableName)
	for i, f := range row.FieldItems {
		if f.PrimaryKey && docID == "" {
			docID = fmt.Sprint(f.Value)
		}

		v := getValue(f)
		sv := adapter.ToSQLValue(v)
		lf := len(f.Field)
		source.Grow(14 + lf + len(sv))
		source.WriteString("ctx._source.")
		source.WriteString(f.Field)
		source.WriteString("=")
		source.WriteString(sv)

		doc.Grow(4 + lf + len(v))
		doc.WriteString(`"`)
		doc.WriteString(f.Field)
		doc.WriteString(`":`)
		doc.WriteString(v)

		if i < l-1 {
			source.WriteString(";")
			doc.WriteString(",")
		}
	}

	s := source.String()
	d := doc.String()
	builder.Grow(45 + len(s) + len(d))
	builder.WriteString(`{"script":{`)
	builder.WriteString(`"source":"`)
	builder.WriteString(s)
	builder.WriteString(`",`)
	builder.WriteString(`"lang": "painless"`)
	builder.WriteString("},")
	builder.WriteString(`"upsert":{`)
	builder.WriteString(d)
	builder.WriteString("}}")

	return indexName, docID, builder.String(), nil
}

func buildRequestParams(row schema.Row) (string, string, string, error) {
	var builder strings.Builder
	var docID string
	length := len(row.FieldItems)
	if length == 0 {
		return "", "", "", adapter.ErrNoRows
	}

	indexName := fmt.Sprintf("%s.%s", row.Schema, row.TableName)
	builder.WriteString(`{`)
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
	builder.WriteString(`}`)

	return indexName, docID, builder.String(), nil
}
