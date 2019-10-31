package schema

import (
	"bytes"
	"encoding/json"
	"log"
)

type ValueMapping struct {
	Schema  ValueSchema
	Payload ValuePayload
}

type ValueSchema struct {
	Type     string
	Optional bool
	Fields   []PayloadItem
	Name     string
}

type ValuePayload struct {
	Before map[string]interface{}
	After  map[string]interface{}
	Source Source
	Op     ChangeEvent
	TsMs   int
}

func ParseValues(b []byte) (ValueMapping, error) {
	var m ValueMapping

	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	err := decoder.Decode(&m)
	if err != nil {
		log.Println("json decode error: ", string(b))
		return m, err
	}

	return m, nil
}
