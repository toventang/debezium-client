package adapter

import (
	"encoding/json"
	"log"
	"strings"

	"github.com/toventang/debezium-client/schema"
)

// EscapeQuotes double quotes value escape to single quotes
func EscapeQuotes(v string) string {
	dqv := v
	if dqv[:1] == `"` {
		dqv = "'" + dqv[1:]
	}
	if dqv[len(dqv)-1:] == `"` {
		dqv = dqv[:len(dqv)-1] + "'"
	}

	return dqv
}

// ToSQLValue convert string values to sql values
func ToSQLValue(v string) string {
	l := len(v)
	if l == 0 {
		return ""
	}

	if l >= 8 && v[:4] == `"{\"` && v[len(v)-4:] == `\"}"` {
		// replace json escape character
		v = strings.ReplaceAll(v, `\"`, `"`)
	}

	return EscapeQuotes(v)
}

// GetSQLValue returns a field string value
func GetSQLValue(f schema.FieldItem) string {
	b, err := json.Marshal(f.Value)
	if err != nil {
		return ""
	}

	v := ToSQLValue(string(b))
	log.Println(f.Field, f.Type, f.Value, v)
	return v
}

// GetPKFieldValue returns a primary key and values
func GetPKFieldValue(row schema.Row) (string, string) {
	var pk, val string
	for _, f := range row.FieldItems {
		if f.PrimaryKey && len(pk) == 0 {
			pk = f.Field
			val = GetSQLValue(f)
			break
		}
	}
	return pk, val
}
