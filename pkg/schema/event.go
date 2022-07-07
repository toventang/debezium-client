package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

type CRUD string

const (
	CREATE CRUD = "c"
	UPDATE CRUD = "u"
	DELETE CRUD = "d"
)

type ChangedEvent struct {
	Schema  Schema
	Payload Payload
}

type Schema struct {
	Type     string
	Optional bool
	Fields   []FieldPayload
	Name     string
}

type Payload struct {
	Before map[string]interface{}
	After  map[string]interface{}
	Source Source
	Op     CRUD
	TsMs   int

	DatabaseName string
	SchemaName   string
	DDL          string
	TableChanges []interface{}
}

type TableStructure struct {
	Source struct {
		Server string `json:"server"`
	} `json:"source"`
	Position struct {
		TransactionId string `json:"transaction_id"`
		TsSec         int64  `json:"ts_sec"`
		File          string `json:"file"`
		Pos           int64  `json:"pos"`
		ServerId      int64  `json:"serverId"`
	} `json:"position"`
	DatabaseName string   `json:"databaseName"`
	DDL          string   `json:"ddl"`
	TableChanges []string `json:"tableChanges"`
}

type Source struct {
	Version   string `json:"version"`
	Connector string `json:"connector"`
	Name      string `json:"name"`
	TsMs      int64  `json:"ts_ms"`
	Snapshot  string `json:"snapshot"`
	DB        string `json:"db"`
	Sequence  string `json:"sequence"`
	Table     string `json:"table"`
	ServerId  int64  `json:"server_id"`
	Gtid      string `json:"gtid"`
	File      string `json:"file"`
	Pos       int64  `json:"pos"`
	Row       int64  `json:"row"`
	Thread    int64  `json:"thread"`
	Query     string `json:"query"`
}

func NewChangedEvent(b []byte) (*ChangedEvent, error) {
	m := &ChangedEvent{}
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	err := decoder.Decode(m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// returns all the fields of the event
func (e *ChangedEvent) Fields(primaryKey string) []*Field {
	for _, f := range e.Schema.Fields {
		if !strings.EqualFold(f.Field, "before") {
			continue
		}
		return f.Fields
	}
	return nil
}

func (e *ChangedEvent) GetFieldMappingWithTable(tables []Table) []*FieldMap {
	for _, t := range tables {
		if t.Name == e.Payload.Source.Table {
			return t.FieldMappingMap
		}
	}
	return nil
}

// returns changed fields and the values, and sets the primary key if field name equal to primaryKey
func (e *ChangedEvent) ChangedFieldValues(primaryKey string) []*Field {
	var fields []*Field
	switch e.Payload.Op {
	case CREATE:
		for k, v := range e.Payload.After {
			f, err := e.field(k)
			if err != nil {
				return nil
			}
			fields = append(fields, &Field{
				Field:      k,
				Value:      v,
				PrimaryKey: primaryKey == k || f.PrimaryKey,
				Type:       f.Type,
			})
		}
	case UPDATE:
		for k, v := range e.Payload.After {
			f, err := e.field(k)
			if err != nil {
				return nil
			}

			if primaryKey == k {
				// primary key
				fields = append(fields, &Field{
					Field:      k,
					PrimaryKey: true,
					Value:      v,
					Type:       f.Type,
				})
			}

			for kb, vb := range e.Payload.Before {
				if kb == k && vb != v {
					// updated field
					fields = append(fields, &Field{
						Field:      k,
						Value:      v,
						PrimaryKey: f.PrimaryKey,
						Type:       f.Type,
					})
				}
			}
		}
	case DELETE:
		for k, v := range e.Payload.Before {
			f, err := e.field(k)
			if err != nil {
				return nil
			}

			if primaryKey == k {
				fields = append(fields, &Field{
					Field:      k,
					PrimaryKey: true,
					Value:      v,
					Type:       f.Type,
				})
				break
			}
		}
	}

	return fields
}

// returns the field with the field name k
func (e *ChangedEvent) field(k string) (*Field, error) {
	return getField(e.Fields(""), k)
}

// returns the field with the field name k in fields
func getField(fields []*Field, k string) (*Field, error) {
	for _, f := range fields {
		if strings.EqualFold(f.Field, k) {
			return f, nil
		}
	}
	return nil, fmt.Errorf("field '%s' not exists", k)
}
