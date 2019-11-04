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

// ParsePrimaryKeys returns primary key, but not set values
func ParsePrimaryKeys(bytes []byte) (FieldItems, error) {
	m := new(KeyMapping)
	err := json.Unmarshal(bytes, m)
	if err != nil {
		return nil, err
	}
	var fields []FieldItem
	for _, f := range m.Schema.Fields {
		f.PrimaryKey = true
		fields = append(fields, f)
	}
	return fields, nil
}

func GetPKValues(pk FieldItems, m ValueMapping) FieldItems {
	var fieldItems FieldItems
	for _, s := range m.Schema.Fields {
		if s.Field == "before" {
			for _, f := range s.Fields {
				f.Value = m.Payload.Before[f.Field]
				f.PrimaryKey = pk.ContainsKey(f.Field)

				fieldItems = append(fieldItems, f)
			}
			break
		}
	}
	return fieldItems
}

// GetFieldValues returns fields and the new values
func GetFieldValues(pk FieldItems, m ValueMapping, onlyChangedFields bool) FieldItems {
	var fieldItems FieldItems

	if len(m.Schema.Fields) > 0 {
		switch m.Payload.Op {
		case CREATE, UPDATE:
			if onlyChangedFields {
				fieldItems = GetUpdateEventValues(pk, m)
			} else {
				fieldItems = GetCreateEventValues(pk, m)
			}
		case DELETE:
			fieldItems = GetPKValues(pk, m)
		}
	}

	return fieldItems
}
