package controller

import (
	"testing"
)

func TestCensorErrorStr(t *testing.T) {
	tests := []struct {
		msg  string
		want string
	}{
		{
			msg:  "read tcp 127.0.0.1:46782->192.168.1.2:443: connection reset by peer",
			want: "read tcp: connection reset by peer",
		},
	}
	for _, tt := range tests {
		t.Run(tt.msg, func(t *testing.T) {
			if got := CensorErrorStr(tt.msg); got != tt.want {
				t.Errorf("CensorErrorStr() = %v, want %v", got, tt.want)
			}
		})
	}
}
