package storetest_test

import (
	"reflect"
	"testing"

	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/storetest"
)

func TestNewStoreUsesFixtureCapabilities(t *testing.T) {
	want := store.Capabilities{Search: true}
	fixture := storetest.Fixture{
		Capabilities: want,
		New: func(testing.TB, *store.Schema) (store.Store, error) {
			return capabilityStore{CapabilitiesValue: want}, nil
		},
	}
	storage := storetest.NewStore(t, fixture)
	if got := storage.Capabilities(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Capabilities() = %#v, want %#v", got, want)
	}
}

type capabilityStore struct {
	store.Store
	CapabilitiesValue store.Capabilities
}

func (s capabilityStore) Capabilities() store.Capabilities { return s.CapabilitiesValue }
