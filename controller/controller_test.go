package controller_test

import (
	"slices"
	"testing"

	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/controller"
)

func TestScopesRoundTrip(t *testing.T) {
	tests := [][]store.Scope{
		{
			{Resource: "foo", Name: "bar"},
			{Resource: "a", Name: "b"},
		},
		{},
		{
			{Name: "bar"},
		},
		{
			{Name: "bar"},
			{Name: "bar"},
		},
	}

	for _, tt := range tests {
		encoded := controller.EncodeScopes(tt)
		decoded := controller.DecodeScopes(encoded)
		if !slices.Equal(tt, decoded) {
			t.Errorf("DecodeScopes(%q) = %v, want %v", encoded, decoded, tt)
		}
	}
}
