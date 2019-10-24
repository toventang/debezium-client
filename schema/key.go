package schema

type KeyMapping struct {
	Schema  KeySchema
	Payload KeyPayload
}

type KeySchema struct {
	Type     string
	Optional bool
	Fields   []FieldItem
	Name     string
}

type KeyPayload struct {
	ID int
}
