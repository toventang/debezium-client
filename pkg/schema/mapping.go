package schema

import (
	"fmt"
	"strings"
)

type FieldMap struct {
	Source, Target string
}

func ParseFieldMaps(in []string) ([]*FieldMap, error) {
	m := make([]*FieldMap, len(in))
	for i, s := range in {
		f, err := ParseFieldMap(s)
		if err != nil {
			return nil, err
		}
		m[i] = f
	}
	return m, nil
}

func ParseFieldMap(s string) (*FieldMap, error) {
	arr, err := splitMappingSpec(s)
	if err != nil {
		return nil, err
	}

	m := &FieldMap{}
	switch len(arr) {
	case 1:
		m.Target = arr[0]
	case 2:
		m.Source = arr[0]
		m.Target = arr[1]
	default:
		return nil, errInvalidFieldMapping(s)
	}

	return m, nil
}

func splitMappingSpec(s string) ([]string, error) {
	if strings.Count(s, ":") > 1 {
		return nil, errInvalidFieldMapping(s)
	}

	arr := strings.SplitN(s, ":", 2)
	if arr[0] == "" {
		return nil, errInvalidFieldMapping(s)
	}
	return arr, nil
}

func errInvalidFieldMapping(s string) error {
	return fmt.Errorf("invalid field mapping specification: '%s'", s)
}
