package schema

type Row struct {
	Schema, TableName string
	DDL, Query        string
	PrimaryKey        string
	FieldItems        []*Field
}

type FieldPayload struct {
	Type     string
	Optional bool
	Fields   []*Field
	Name     string
	Field    string
}

type Field struct {
	Name       string
	Type       string
	Optional   bool
	Field      string
	PrimaryKey bool
	Value      interface{}
}
