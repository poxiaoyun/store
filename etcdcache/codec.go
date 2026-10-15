package etcdcache

import (
	stdjson "encoding/json"
	"io"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var _ runtime.Codec = &SimpleJsonCodec{}

type SimpleJsonCodec struct{}

// Decode implements runtime.Codec.
func (s SimpleJsonCodec) Decode(data []byte, defaults *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	if into == nil {
		into = &StorageObject{}
	}
	if err := JsonUnmarshal(data, into); err != nil {
		return nil, nil, err
	}
	return into, defaults, nil
}

// Encode implements runtime.Codec.
func (s SimpleJsonCodec) Encode(obj runtime.Object, w io.Writer) error {
	return stdjson.NewEncoder(w).Encode(obj)
}

// Identifier implements runtime.Codec.
func (s SimpleJsonCodec) Identifier() runtime.Identifier {
	return runtime.Identifier("simplejson")
}
