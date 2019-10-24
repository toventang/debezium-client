package schema

import "encoding/json"

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

func ParseValues(bytes []byte) (values ValueMapping, err error) {
	err = json.Unmarshal(bytes, &values)
	if err != nil {
		return
	}
	return
}
