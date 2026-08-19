package platform

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

type JSONMap map[string]any

func (m JSONMap) Value() (driver.Value, error) {
	if m == nil {
		return []byte("{}"), nil
	}
	raw, err := json.Marshal(m)
	return raw, err
}

func (m *JSONMap) Scan(value any) error {
	if value == nil {
		*m = JSONMap{}
		return nil
	}
	var raw []byte
	switch v := value.(type) {
	case []byte:
		raw = v
	case string:
		raw = []byte(v)
	default:
		return fmt.Errorf("unsupported JSONMap scan type %T", value)
	}
	return json.Unmarshal(raw, m)
}

type JSONStringMap map[string]string

func (m JSONStringMap) Value() (driver.Value, error) {
	if m == nil {
		return []byte("{}"), nil
	}
	raw, err := json.Marshal(m)
	return raw, err
}

func (m *JSONStringMap) Scan(value any) error {
	if value == nil {
		*m = JSONStringMap{}
		return nil
	}
	var raw []byte
	switch v := value.(type) {
	case []byte:
		raw = v
	case string:
		raw = []byte(v)
	default:
		return fmt.Errorf("unsupported JSONStringMap scan type %T", value)
	}
	return json.Unmarshal(raw, m)
}

type StringSlice []string

func (s StringSlice) Value() (driver.Value, error) {
	raw, err := json.Marshal([]string(s))
	return raw, err
}

func (s *StringSlice) Scan(value any) error {
	if value == nil {
		*s = StringSlice{}
		return nil
	}
	var raw []byte
	switch v := value.(type) {
	case []byte:
		raw = v
	case string:
		raw = []byte(v)
	default:
		return fmt.Errorf("unsupported StringSlice scan type %T", value)
	}
	return json.Unmarshal(raw, (*[]string)(s))
}
