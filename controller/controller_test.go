package controller

import (
	"testing"

	"xiaoshiai.cn/store"
)

func Test_decodeScopes(t *testing.T) {
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
		encoded := EncodeScopes(tt)
		decoded := DecodeScopes(encoded)
		if len(tt) != len(decoded) {
			t.Errorf("decodeScopes() = %v, want %v", decoded, tt)
		}
	}
}
