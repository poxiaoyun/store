package store_test

import (
	"testing"
	"xiaoshiai.cn/store"
)

func TestPatchClearsRemovedFields(t *testing.T) {
	for _, kind := range []string{"merge", "json"} {
		t.Run(kind, func(t *testing.T) {
			value := "reserved"
			object := &struct {
				Key     *string `json:"key,omitempty"`
				Enabled bool    `json:"enabled,omitempty"`
				Name    string  `json:"name"`
			}{Key: &value, Enabled: true, Name: "retained"}
			var err error
			if kind == "merge" {
				err = store.JsonMergePatchObject(object, []byte(`{"key":null,"enabled":null}`))
			} else {
				err = store.JsonPatchObject(object, []byte(`[{"op":"remove","path":"/key"},{"op":"remove","path":"/enabled"}]`))
			}
			if err != nil {
				t.Fatal(err)
			}
			if object.Key != nil || object.Enabled || object.Name != "retained" {
				t.Fatalf("patch did not replace removed fields: %#v", object)
			}
		})
	}
}
