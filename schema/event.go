package schema

type ChangeEvent string

const (
	CREATE ChangeEvent = "c"
	UPDATE ChangeEvent = "u"
	DELETE ChangeEvent = "d"
	READ   ChangeEvent = "r"
)
