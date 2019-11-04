package schema

import "log"

type ChangeEvent string

const (
	CREATE ChangeEvent = "c"
	UPDATE ChangeEvent = "u"
	DELETE ChangeEvent = "d"
	READ   ChangeEvent = "r"
)

func GetCreateEventValues(pk FieldItems, m ValueMapping) FieldItems {
	var fieldItems FieldItems
	for _, s := range m.Schema.Fields {
		if s.Field == "after" {
			for _, f := range s.Fields {
				f.Value = m.Payload.After[f.Field]
				f.PrimaryKey = pk.ContainsKey(f.Field)

				fieldItems = append(fieldItems, f)
			}
			break
		}
	}
	return fieldItems
}

func GetUpdateEventValues(pk FieldItems, m ValueMapping) FieldItems {
	var fieldItems FieldItems

	var fields FieldItems
	for _, s := range m.Schema.Fields {
		if s.Field == "after" {
			fields = s.Fields
			break
		}
	}

	for k, v := range m.Payload.After {
		// Get the value of the modified column and primary key
		isPK := pk.ContainsKey(k)
		if v != m.Payload.Before[k] || isPK {
			log.Println("k: ", k, ", before: ", m.Payload.Before[k], ", after: ", v)
			for _, f := range fields {
				if f.Field == k {
					f.PrimaryKey = isPK
					f.Value = v
					fieldItems = append(fieldItems, f)
				}
			}
		}
	}

	return fieldItems
}

type StringArray []string

func (a StringArray) Contains(s string) bool {
	for _, v := range a {
		if v == s {
			return true
		}
	}
	return false
}
