package elasticsearch

import (
	"encoding/json"

	"github.com/toventang/debezium-client/schema"
)

func getValue(f schema.FieldItem) string {
	b, err := json.Marshal(f.Value)
	if err != nil {
		return ""
	}

	return string(b)
}
