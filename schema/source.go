package schema

type Source struct {
	Version           string
	Connector         string
	Name              string
	TsMs              int64
	Snapshot          string
	TxID              int
	LSN               int
	XMin              interface{}
	DB, Schema, Table string
}
