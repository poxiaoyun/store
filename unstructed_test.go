package store_test

import (
	"encoding/json"
	"math"
	"testing"

	"xiaoshiai.cn/store"
)

type unstructuredCounter struct {
	store.ObjectMeta `json:",inline"`
	Count            int64   `json:"count"`
	Ratio            float64 `json:"ratio"`
	Status           struct {
		Observed int64 `json:"observed"`
	} `json:"status"`
}

func TestUnstructuredJSONPreservesInt64AcrossTypedConversion(t *testing.T) {
	encoded := []byte(`{"count":9007199254740993,"ratio":1.5,"status":{"observed":9223372036854775807}}`)
	var object store.Unstructured
	if err := json.Unmarshal(encoded, &object); err != nil {
		t.Fatal(err)
	}
	var converted unstructuredCounter
	if err := store.FromUnstructured(&object, &converted); err != nil {
		t.Fatal(err)
	}
	if converted.Count != 9007199254740993 || converted.Status.Observed != math.MaxInt64 || converted.Ratio != 1.5 {
		t.Fatalf("unstructured numeric conversion lost precision: %#v", converted)
	}
	if _, ok := object.Object["count"].(int64); !ok {
		t.Fatalf("integer fact has non-unstructured numeric type %T", object.Object["count"])
	}
	if _, ok := object.Object["ratio"].(float64); !ok {
		t.Fatalf("fraction fact has non-unstructured numeric type %T", object.Object["ratio"])
	}
}
