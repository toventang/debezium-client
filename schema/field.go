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

func GetFieldValues(keyFields FieldItems, fields map[string]interface{}) FieldItems {
	var fieldItems FieldItems
	for k, v := range fields {
		f := FieldItem{
			Field:      k,
			Value:      v,
			PrimaryKey: keyFields.ContainsKey(k),
		}
		fieldItems = append(fieldItems, f)
	}
	return fieldItems
}
