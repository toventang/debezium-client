package mapping

import (
	"encoding/json"
	"errors"
)

var (
	ErrNoTable  = errors.New("no tables mapping")
	ErrNoFields = errors.New("no fields mapping")
)

type Mapper struct {
	tables map[string]FieldMapping
}

type FieldMapping map[string]string

func NewFieldMapper(cfg []byte) Mapper {
	var tables map[string]FieldMapping
	json.Unmarshal(cfg, &tables)

	return Mapper{tables}
}

func (m Mapper) GetFieldName(table, field string) string {
	t, err := m.GetTable(table)
	if err != nil {
		return field
	}

	f := t.GetFieldName(field)
	return f
}

func (m Mapper) GetTable(table string) (FieldMapping, error) {
	var fm FieldMapping
	if len(table) == 0 {
		return fm, ErrNoTable
	}

	fm = m.tables[table]
	if len(fm) == 0 {
		return fm, ErrNoFields
	}
	return fm, nil
}

func (fm FieldMapping) GetFieldName(field string) string {
	f := fm[field]
	if len(f) == 0 {
		return field
	}
	return f
}
