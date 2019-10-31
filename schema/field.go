package schema

import (
	"encoding/json"
	"errors"
)

var (
	UNKNOW_OPERATION = errors.New("unknown operation")
)

type Row struct {
	Schema, TableName string
	FieldItems        FieldItems
}

type PayloadItem struct {
	Type     string
	Optional bool
	Fields   []FieldItem
	Name     string
	Field    string
}

type FieldItem struct {
	Type     string
	Optional bool
	Field    string

	PrimaryKey bool
	Value      interface{}
}

type FieldItems []FieldItem

func (fi FieldItems) ContainsKey(key string) bool {
	for _, i := range fi {
		if i.Field == key {
			return true
		}
	}
	return false
}

func ParsePrimaryKeys(bytes []byte) (FieldItems, error) {
	m := new(KeyMapping)
	err := json.Unmarshal(bytes, m)
	if err != nil {
		return nil, err
	}
	var fields []FieldItem
	for _, f := range m.Schema.Fields {
		fields = append(fields, f)
	}
	return fields, nil
}

// GetFieldValues 获取所有字段及最新的值
func GetFieldValues(keys FieldItems, m ValueMapping) FieldItems {
	var fieldItems FieldItems
	var pos string

	fields := make(map[string]interface{})
	switch m.Payload.Op {
	case CREATE, UPDATE:
		fields = m.Payload.After
		pos = "after"
	case DELETE:
		fields = m.Payload.Before
		pos = "before"
	}

	if len(m.Schema.Fields) > 0 {
		for _, s := range m.Schema.Fields {
			if s.Field == pos {
				for _, f := range s.Fields {
					f.Value = fields[f.Field]
					f.PrimaryKey = keys.ContainsKey(f.Field)

					fieldItems = append(fieldItems, f)
				}
			}
		}
	}

	return fieldItems
}
