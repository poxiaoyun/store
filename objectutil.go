package store

import (
	"bytes"
	"encoding/json"
	"reflect"
	"time"

	"xiaoshiai.cn/common/meta"
)

// CopyObject copies the JSON representation of source into target.
func CopyObject(source, target Object) error {
	data, err := json.Marshal(source)
	if err != nil {
		return err
	}
	ResetObject(target)
	return json.Unmarshal(data, target)
}

// ResetObject clears target before decoding a complete replacement into it.
func ResetObject(target Object) {
	value := reflect.ValueOf(target).
		Elem()
	value.Set(reflect.Zero(value.Type()))
}

// ServerTimestampNow returns the JSON precision shared by Store backends.
func ServerTimestampNow() meta.Time {
	now := time.Now().
		UTC().
		Truncate(time.Second)
	return meta.Time{Time: now}
}

// ObjectToMap returns the JSON object representation of obj. JSON numbers use
// json.Number so subsequent merging and decoding cannot round integer values.
func ObjectToMap(obj Object) (map[string]any, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	result := map[string]any{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}
