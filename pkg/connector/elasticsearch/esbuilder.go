package elasticsearch

import (
	"fmt"
	"strings"

	"github.com/spf13/cast"
	"github.com/toventang/debezium-client/pkg/connector"
	"github.com/toventang/debezium-client/pkg/schema"
)

var (
	docScriptPrefix = []byte(`{"doc":{`)
	docScriptSuffix = []byte(`},"doc_as_upsert":true}`)
)

type Script struct {
	index string
	docId string
	query string
}

func BuildUpsertScript(row *schema.Row) (*Script, error) {
	var (
		builder    strings.Builder
		doc        strings.Builder
		docID      string
		fieldCount int = len(row.FieldItems)
	)

	if fieldCount == 0 {
		return nil, connector.ErrNoRows
	}

	indexName := getIndexName(row)
	for i, f := range row.FieldItems {
		if f.PrimaryKey && len(docID) == 0 {
			s, err := cast.ToStringE(f.Value)
			if err != nil {
				return nil, err
			}
			docID = s
		}

		v := parseValue(f)
		kv := fmt.Sprintf(`"%s":%s`, f.Field, v)
		if i < fieldCount-1 {
			kv += ","
		}
		doc.Grow(len(kv))
		doc.WriteString(kv)
	}

	s := doc.String()
	doc.Reset()

	builder.Grow(len(docScriptPrefix) + len(docScriptSuffix) + len(s))
	builder.Write(docScriptPrefix)
	builder.WriteString(s)
	builder.Write(docScriptSuffix)

	return &Script{
		index: indexName,
		docId: docID,
		query: builder.String(),
	}, nil
}

// returns the field value, if the field is a string type, the field value will be quoted with double quotes
func parseValue(f *schema.Field) string {
	v := cast.ToString(f.Value)
	if strings.EqualFold(f.Type, "string") {
		return fmt.Sprintf(`"%s"`, v)
	}
	return v
}

func getIndexName(row *schema.Row) string {
	return fmt.Sprintf(`%s_%s`, row.Schema, row.TableName)
}
