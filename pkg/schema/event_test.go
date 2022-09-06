package schema_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/toventang/debezium-client/pkg/schema"
)

var (
	schemaChangedData = []byte(`{
	"schema": {
	   "type": "struct",
	   "fields": [
		  {
			 "type": "struct",
			 "fields": [
				{
				   "type": "string",
				   "optional": false,
				   "field": "version"
				},
				{
				   "type": "string",
				   "optional": false,
				   "field": "connector"
				},
				{
				   "type": "string",
				   "optional": false,
				   "field": "name"
				},
				{
				   "type": "int64",
				   "optional": false,
				   "field": "ts_ms"
				},
				{
				   "type": "string",
				   "optional": true,
				   "name": "io.debezium.data.Enum",
				   "version": 1,
				   "parameters": {
					  "allowed": "true,last,false,incremental"
				   },
				   "default": "false",
				   "field": "snapshot"
				},
				{
				   "type": "string",
				   "optional": false,
				   "field": "db"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "sequence"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "table"
				},
				{
				   "type": "int64",
				   "optional": false,
				   "field": "server_id"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "gtid"
				},
				{
				   "type": "string",
				   "optional": false,
				   "field": "file"
				},
				{
				   "type": "int64",
				   "optional": false,
				   "field": "pos"
				},
				{
				   "type": "int32",
				   "optional": false,
				   "field": "row"
				},
				{
				   "type": "int64",
				   "optional": true,
				   "field": "thread"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "query"
				}
			 ],
			 "optional": false,
			 "name": "io.debezium.connector.mysql.Source",
			 "field": "source"
		  },
		  {
			 "type": "string",
			 "optional": true,
			 "field": "databaseName"
		  },
		  {
			 "type": "string",
			 "optional": true,
			 "field": "schemaName"
		  },
		  {
			 "type": "string",
			 "optional": true,
			 "field": "ddl"
		  },
		  {
			 "type": "array",
			 "items": {
				"type": "struct",
				"fields": [
				   {
					  "type": "string",
					  "optional": false,
					  "field": "type"
				   },
				   {
					  "type": "string",
					  "optional": false,
					  "field": "id"
				   },
				   {
					  "type": "struct",
					  "fields": [
						 {
							"type": "string",
							"optional": true,
							"field": "defaultCharsetName"
						 },
						 {
							"type": "array",
							"items": {
							   "type": "string",
							   "optional": false
							},
							"optional": true,
							"field": "primaryKeyColumnNames"
						 },
						 {
							"type": "array",
							"items": {
							   "type": "struct",
							   "fields": [
								  {
									 "type": "string",
									 "optional": false,
									 "field": "name"
								  },
								  {
									 "type": "int32",
									 "optional": false,
									 "field": "jdbcType"
								  },
								  {
									 "type": "int32",
									 "optional": true,
									 "field": "nativeType"
								  },
								  {
									 "type": "string",
									 "optional": false,
									 "field": "typeName"
								  },
								  {
									 "type": "string",
									 "optional": true,
									 "field": "typeExpression"
								  },
								  {
									 "type": "string",
									 "optional": true,
									 "field": "charsetName"
								  },
								  {
									 "type": "int32",
									 "optional": true,
									 "field": "length"
								  },
								  {
									 "type": "int32",
									 "optional": true,
									 "field": "scale"
								  },
								  {
									 "type": "int32",
									 "optional": false,
									 "field": "position"
								  },
								  {
									 "type": "boolean",
									 "optional": true,
									 "field": "optional"
								  },
								  {
									 "type": "boolean",
									 "optional": true,
									 "field": "autoIncremented"
								  },
								  {
									 "type": "boolean",
									 "optional": true,
									 "field": "generated"
								  },
								  {
									 "type": "string",
									 "optional": true,
									 "field": "comment"
								  }
							   ],
							   "optional": false,
							   "name": "io.debezium.connector.schema.Column"
							},
							"optional": false,
							"field": "columns"
						 },
						 {
							"type": "string",
							"optional": true,
							"field": "comment"
						 }
					  ],
					  "optional": false,
					  "name": "io.debezium.connector.schema.Table",
					  "field": "table"
				   }
				],
				"optional": false,
				"name": "io.debezium.connector.schema.Change"
			 },
			 "optional": false,
			 "field": "tableChanges"
		  }
	   ],
	   "optional": false,
	   "name": "io.debezium.connector.mysql.SchemaChangeValue"
	},
	"payload": {
	   "source": {
		  "version": "1.9.3.Final",
		  "connector": "mysql",
		  "name": "dbserver1",
		  "ts_ms": 1656742839930,
		  "snapshot": "false",
		  "db": "product",
		  "sequence": null,
		  "table": "category",
		  "server_id": 1,
		  "gtid": null,
		  "file": "binlog.000013",
		  "pos": 5226,
		  "row": 0,
		  "thread": null,
		  "query": null
	   },
	   "databaseName": "product",
	   "schemaName": null,
	   "ddl": "ALTER TABLE product.category \nADD COLUMN e varchar(255) NULL AFTER dd",
	   "tableChanges": []
	}
 }`)

	dataUpdated = []byte(`{
	"schema": {
	   "type": "struct",
	   "fields": [
		  {
			 "type": "struct",
			 "fields": [
				{
				   "type": "int64",
				   "optional": false,
				   "field": "id"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "aa"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "b"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "cc"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "dd"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "e"
				}
			 ],
			 "optional": true,
			 "name": "dbserver1.product.category.Value",
			 "field": "before"
		  },
		  {
			 "type": "struct",
			 "fields": [
				{
				   "type": "int64",
				   "optional": false,
				   "field": "id"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "aa"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "b"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "cc"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "dd"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "e"
				}
			 ],
			 "optional": true,
			 "name": "dbserver1.product.category.Value",
			 "field": "after"
		  },
		  {
			 "type": "struct",
			 "fields": [
				{
				   "type": "string",
				   "optional": false,
				   "field": "version"
				},
				{
				   "type": "string",
				   "optional": false,
				   "field": "connector"
				},
				{
				   "type": "string",
				   "optional": false,
				   "field": "name"
				},
				{
				   "type": "int64",
				   "optional": false,
				   "field": "ts_ms"
				},
				{
				   "type": "string",
				   "optional": true,
				   "name": "io.debezium.data.Enum",
				   "version": 1,
				   "parameters": {
					  "allowed": "true,last,false,incremental"
				   },
				   "default": "false",
				   "field": "snapshot"
				},
				{
				   "type": "string",
				   "optional": false,
				   "field": "db"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "sequence"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "table"
				},
				{
				   "type": "int64",
				   "optional": false,
				   "field": "server_id"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "gtid"
				},
				{
				   "type": "string",
				   "optional": false,
				   "field": "file"
				},
				{
				   "type": "int64",
				   "optional": false,
				   "field": "pos"
				},
				{
				   "type": "int32",
				   "optional": false,
				   "field": "row"
				},
				{
				   "type": "int64",
				   "optional": true,
				   "field": "thread"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "query"
				}
			 ],
			 "optional": false,
			 "name": "io.debezium.connector.mysql.Source",
			 "field": "source"
		  },
		  {
			 "type": "string",
			 "optional": false,
			 "field": "op"
		  },
		  {
			 "type": "int64",
			 "optional": true,
			 "field": "ts_ms"
		  },
		  {
			 "type": "struct",
			 "fields": [
				{
				   "type": "string",
				   "optional": false,
				   "field": "id"
				},
				{
				   "type": "int64",
				   "optional": false,
				   "field": "total_order"
				},
				{
				   "type": "int64",
				   "optional": false,
				   "field": "data_collection_order"
				}
			 ],
			 "optional": true,
			 "field": "transaction"
		  }
	   ],
	   "optional": false,
	   "name": "dbserver1.product.category.Envelope"
	},
	"payload": {
	   "before": {
		  "id": 1,
		  "aa": "11",
		  "b": "bb",
		  "cc": "c",
		  "dd": "dd",
		  "e": "e"
	   },
	   "after": {
		  "id": 1,
		  "aa": "aa",
		  "b": "bb",
		  "cc": "c",
		  "dd": "dd",
		  "e": "e"
	   },
	   "source": {
		  "version": "1.9.3.Final",
		  "connector": "mysql",
		  "name": "dbserver1",
		  "ts_ms": 1656745184000,
		  "snapshot": "false",
		  "db": "product",
		  "sequence": null,
		  "table": "category",
		  "server_id": 1,
		  "gtid": null,
		  "file": "binlog.000013",
		  "pos": 6678,
		  "row": 0,
		  "thread": 38,
		  "query": null
	   },
	   "op": "u",
	   "ts_ms": 1656745187489,
	   "transaction": null
	}
 }`)

	dataInserted = []byte(`{
	"schema": {
	   "type": "struct",
	   "fields": [
		  {
			 "type": "struct",
			 "fields": [
				{
				   "type": "int64",
				   "optional": false,
				   "field": "id"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "name"
				},
				{
				   "type": "int32",
				   "optional": false,
				   "default": 0,
				   "field": "parent_id"
				},
				{
				   "type": "int32",
				   "optional": false,
				   "default": 0,
				   "field": "created_at"
				}
			 ],
			 "optional": true,
			 "name": "dbserver1.product.category.Value",
			 "field": "before"
		  },
		  {
			 "type": "struct",
			 "fields": [
				{
				   "type": "int64",
				   "optional": false,
				   "field": "id"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "name"
				},
				{
				   "type": "int32",
				   "optional": false,
				   "default": 0,
				   "field": "parent_id"
				},
				{
				   "type": "int32",
				   "optional": false,
				   "default": 0,
				   "field": "created_at"
				}
			 ],
			 "optional": true,
			 "name": "dbserver1.product.category.Value",
			 "field": "after"
		  },
		  {
			 "type": "struct",
			 "fields": [
				{
				   "type": "string",
				   "optional": false,
				   "field": "version"
				},
				{
				   "type": "string",
				   "optional": false,
				   "field": "connector"
				},
				{
				   "type": "string",
				   "optional": false,
				   "field": "name"
				},
				{
				   "type": "int64",
				   "optional": false,
				   "field": "ts_ms"
				},
				{
				   "type": "string",
				   "optional": true,
				   "name": "io.debezium.data.Enum",
				   "version": 1,
				   "parameters": {
					  "allowed": "true,last,false,incremental"
				   },
				   "default": "false",
				   "field": "snapshot"
				},
				{
				   "type": "string",
				   "optional": false,
				   "field": "db"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "sequence"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "table"
				},
				{
				   "type": "int64",
				   "optional": false,
				   "field": "server_id"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "gtid"
				},
				{
				   "type": "string",
				   "optional": false,
				   "field": "file"
				},
				{
				   "type": "int64",
				   "optional": false,
				   "field": "pos"
				},
				{
				   "type": "int32",
				   "optional": false,
				   "field": "row"
				},
				{
				   "type": "int64",
				   "optional": true,
				   "field": "thread"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "query"
				}
			 ],
			 "optional": false,
			 "name": "io.debezium.connector.mysql.Source",
			 "field": "source"
		  },
		  {
			 "type": "string",
			 "optional": false,
			 "field": "op"
		  },
		  {
			 "type": "int64",
			 "optional": true,
			 "field": "ts_ms"
		  },
		  {
			 "type": "struct",
			 "fields": [
				{
				   "type": "string",
				   "optional": false,
				   "field": "id"
				},
				{
				   "type": "int64",
				   "optional": false,
				   "field": "total_order"
				},
				{
				   "type": "int64",
				   "optional": false,
				   "field": "data_collection_order"
				}
			 ],
			 "optional": true,
			 "field": "transaction"
		  }
	   ],
	   "optional": false,
	   "name": "dbserver1.product.category.Envelope"
	},
	"payload": {
	   "before": null,
	   "after": {
		  "id": 2,
		  "name": "bb",
		  "parent_id": 0,
		  "created_at": 0
	   },
	   "source": {
		  "version": "1.9.4.Final",
		  "connector": "mysql",
		  "name": "dbserver1",
		  "ts_ms": 1657002170000,
		  "snapshot": "false",
		  "db": "product",
		  "sequence": null,
		  "table": "category",
		  "server_id": 1,
		  "gtid": null,
		  "file": "binlog.000009",
		  "pos": 3020,
		  "row": 0,
		  "thread": 34,
		  "query": "INSERT INTO product.category(name) VALUES ('bb')"
	   },
	   "op": "c",
	   "ts_ms": 1657002171006,
	   "transaction": null
	}
 }`)

	dataDeleted = []byte(`{
	"schema": {
	   "type": "struct",
	   "fields": [
		  {
			 "type": "struct",
			 "fields": [
				{
				   "type": "int64",
				   "optional": false,
				   "field": "id"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "name"
				},
				{
				   "type": "int32",
				   "optional": false,
				   "default": 0,
				   "field": "parent_id"
				},
				{
					"name": "io.debezium.time.Timestamp",
					"type": "int32",
					"optional": false,
				    "default": 0,
				    "field": "created_at"
				}
			 ],
			 "optional": true,
			 "name": "dbserver1.product.category.Value",
			 "field": "before"
		  },
		  {
			 "type": "struct",
			 "fields": [
				{
				   "type": "int64",
				   "optional": false,
				   "field": "id"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "name"
				},
				{
				   "type": "int32",
				   "optional": false,
				   "default": 0,
				   "field": "parent_id"
				},
				{
				   "type": "int32",
				   "optional": false,
				   "default": 0,
				   "field": "created_at"
				}
			 ],
			 "optional": true,
			 "name": "dbserver1.product.category.Value",
			 "field": "after"
		  },
		  {
			 "type": "struct",
			 "fields": [
				{
				   "type": "string",
				   "optional": false,
				   "field": "version"
				},
				{
				   "type": "string",
				   "optional": false,
				   "field": "connector"
				},
				{
				   "type": "string",
				   "optional": false,
				   "field": "name"
				},
				{
				   "type": "int64",
				   "optional": false,
				   "field": "ts_ms"
				},
				{
				   "type": "string",
				   "optional": true,
				   "name": "io.debezium.data.Enum",
				   "version": 1,
				   "parameters": {
					  "allowed": "true,last,false,incremental"
				   },
				   "default": "false",
				   "field": "snapshot"
				},
				{
				   "type": "string",
				   "optional": false,
				   "field": "db"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "sequence"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "table"
				},
				{
				   "type": "int64",
				   "optional": false,
				   "field": "server_id"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "gtid"
				},
				{
				   "type": "string",
				   "optional": false,
				   "field": "file"
				},
				{
				   "type": "int64",
				   "optional": false,
				   "field": "pos"
				},
				{
				   "type": "int32",
				   "optional": false,
				   "field": "row"
				},
				{
				   "type": "int64",
				   "optional": true,
				   "field": "thread"
				},
				{
				   "type": "string",
				   "optional": true,
				   "field": "query"
				}
			 ],
			 "optional": false,
			 "name": "io.debezium.connector.mysql.Source",
			 "field": "source"
		  },
		  {
			 "type": "string",
			 "optional": false,
			 "field": "op"
		  },
		  {
			 "type": "int64",
			 "optional": true,
			 "field": "ts_ms"
		  },
		  {
			 "type": "struct",
			 "fields": [
				{
				   "type": "string",
				   "optional": false,
				   "field": "id"
				},
				{
				   "type": "int64",
				   "optional": false,
				   "field": "total_order"
				},
				{
				   "type": "int64",
				   "optional": false,
				   "field": "data_collection_order"
				}
			 ],
			 "optional": true,
			 "field": "transaction"
		  }
	   ],
	   "optional": false,
	   "name": "dbserver1.product.category.Envelope"
	},
	"payload": {
	   "before": {
		  "id": 2,
		  "name": "bb",
		  "parent_id": 0,
		  "created_at": 0
	   },
	   "after": null,
	   "source": {
		  "version": "1.9.4.Final",
		  "connector": "mysql",
		  "name": "dbserver1",
		  "ts_ms": 1657005221000,
		  "snapshot": "false",
		  "db": "product",
		  "sequence": null,
		  "table": "category",
		  "server_id": 1,
		  "gtid": null,
		  "file": "binlog.000009",
		  "pos": 4225,
		  "row": 0,
		  "thread": 34,
		  "query": "DELETE FROM product.category WHERE id = 2"
	   },
	   "op": "d",
	   "ts_ms": 1657005221573,
	   "transaction": null
	}
 }`)
)

func TestParseEvent(t *testing.T) {
	testcases := []struct {
		name  string
		event []byte
	}{
		{
			name:  "Insert",
			event: dataInserted,
		},
		{
			name:  "Update",
			event: dataUpdated,
		},
		{
			name:  "Update",
			event: dataDeleted,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			e, err := schema.NewChangedEvent(tc.event)
			assert.NoError(t, err)
			assert.NotEmpty(t, e.Payload.Source.Name)
		})
	}
}
