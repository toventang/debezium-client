package schema

type Table struct {
	Name            string
	PrimaryKey      string
	FieldMappingMap []*FieldMap
}
