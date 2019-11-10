package adapter

import (
	"encoding/json"
	"log"
	"strings"

	"github.com/toventang/debezium-client/schema"
)

func ToSQLValue(v string) string {
	l := len(v)
	if l == 0 {
		return ""
	}

	if l >= 8 && v[:4] == `"{\"` && v[len(v)-4:] == `\"}"` {
		// replace json escape character
		v = strings.ReplaceAll(v, `\"`, `"`)
	}
	if v[:1] == `"` {
		v = "'" + v[1:]
	}
	if v[len(v)-1:] == `"` {
		v = v[:len(v)-1] + "'"
	}

	return v
}

func GetValue(f schema.FieldItem) string {
	b, err := json.Marshal(f.Value)
	if err != nil {
		return ""
	}

	v := ToSQLValue(string(b))
	log.Println(f.Field, f.Type, f.Value, v)
	return v
}
