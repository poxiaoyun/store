package store

import (
	"encoding/json"

	"sigs.k8s.io/yaml"
)

type Serializer interface {
	Encode(obj Object) ([]byte, error)
	Decode(data []byte, obj Object) error
}

var _ Serializer = &JSONSerializer{}

type JSONSerializer struct{}

func (j JSONSerializer) Encode(obj Object) ([]byte, error) {
	return json.Marshal(obj)
}

func (j JSONSerializer) Decode(data []byte, obj Object) error {
	return json.Unmarshal(data, obj)
}

var _ Serializer = &YamlSerializer{}

type YamlSerializer struct{}

func (y YamlSerializer) Encode(obj Object) ([]byte, error) {
	return yaml.Marshal(obj)
}

func (y YamlSerializer) Decode(data []byte, obj Object) error {
	return yaml.Unmarshal(data, obj)
}
