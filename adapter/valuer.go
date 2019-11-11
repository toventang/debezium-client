package adapter

import (
	"encoding/json"
	"log"
	"strings"

	"github.com/toventang/debezium-client/schema"
)

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

	// double quotes value escape to single quotes
	if v[:1] == `"` {
		v = "'" + v[1:]
	}
	if v[len(v)-1:] == `"` {
		v = v[:len(v)-1] + "'"
	}

	return v
}

// GetValue returns a field string value
func GetValue(f schema.FieldItem) string {
	b, err := json.Marshal(f.Value)
	if err != nil {
		return ""
	}

	v := ToSQLValue(string(b))
	log.Println(f.Field, f.Type, f.Value, v)
	return v
}

// GetPKField returns a primary key and values
func GetPKFieldValue(row schema.Row) (string, string) {
	var pk, val string
	for _, f := range row.FieldItems {
		if f.PrimaryKey && len(pk) == 0 {
			pk = f.Field
			val = GetValue(f)
			break
		}
	}
	return pk, val
}
