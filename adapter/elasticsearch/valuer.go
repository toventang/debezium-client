package elasticsearch

import (
	"encoding/json"
	"fmt"

	"github.com/toventang/debezium-client/schema"
)

func getValue(f schema.FieldItem) string {
	b, err := json.Marshal(f.Value)
	if err != nil {
		return ""
	}

	switch f.Type {
	case "int64":
		return fmt.Sprintf(`"%s"`, string(b))
	}
	return string(b)
}
