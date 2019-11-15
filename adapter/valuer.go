package adapter

import (
	"encoding/json"
	"strings"

	"github.com/toventang/debezium-client/schema"
)

func GetDocValue(f schema.FieldItem) string {
	b, err := json.Marshal(f.Value)
	if err != nil {
		return ""
	}

	v := string(b)
	if isJSONObject(v) {
		v = strings.ReplaceAll(v, `\"`, `"`)
		v = v[1 : len(v)-1]
	}

	return v
}

// GetSQLValue returns a field string value
func GetSQLValue(f schema.FieldItem) string {
	b, err := json.Marshal(f.Value)
	if err != nil {
		return ""
	}

	v := ToSQLValue(string(b))
	return v
}

// EscapeQuotes double quotes value escape to single quotes
func EscapeQuotes(v string) string {
	return ReplaceQuotes(v, "'")
}

func ReplaceQuotes(value string, quote string) string {
	dqv := value
	if dqv[:1] == `"` {
		dqv = quote + dqv[1:]
	}
	l := len(dqv)
	if dqv[l-1:] == `"` {
		dqv = dqv[:l-1] + quote
	}

	return dqv
}

// ToSQLValue convert string values to sql values
func ToSQLValue(v string) string {
	if isJSONObject(v) {
		// replace json escape character
		v = strings.ReplaceAll(v, `\"`, `"`)
	}

	return EscapeQuotes(v)
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

func isJSONObject(v string) bool {
	l := len(v)
	if l == 0 {
		return false
	}

	if l >= 8 {
		start := v[:4]
		end := v[l-4:]
		if (start == `"{\"` && end == `\"}"`) ||
			(start == `"[\"` && end == `\"]"`) {
			return true
		}
	}

	return false
}
