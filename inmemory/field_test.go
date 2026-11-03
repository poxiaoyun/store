package inmemory_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"xiaoshiai.cn/common/meta"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/inmemory"
	"xiaoshiai.cn/store/selector"
)

type encodedString string

func (s encodedString) MarshalJSON() ([]byte, error) { return json.Marshal("wire:" + string(s)) }
func (s *encodedString) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*s = encodedString(strings.TrimPrefix(value, "wire:"))
	return nil
}

type fieldRecord struct {
	store.ObjectMeta `json:",inline"`
	Encoded          encodedString    `json:"encoded"`
	Optional         string           `json:"optional,omitempty"`
	Nil              *string          `json:"nil"`
	At               meta.Time        `json:"at"`
	Zero             meta.Time        `json:"zero"`
	Hidden           string           `json:"-"`
	Nested           map[string]int64 `json:"nested"`
}

func TestModelFieldQueries(t *testing.T) {
	schema := store.NewSchema()
	if err := schema.Register(&fieldRecord{}, store.ResourceSchema{Indexes: []store.Index{
		{Fields: []string{"encoded"}}, {Fields: []string{"at"}}, {Fields: []string{"nested.count"}},
	}}); err != nil {
		t.Fatal(err)
	}
	source, err := inmemory.New(schema)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	record := &fieldRecord{ObjectMeta: store.ObjectMeta{ID: "record"}, Encoded: "abc", At: meta.Time{Time: at}, Hidden: "private", Nested: map[string]int64{"count": 9007199254740993}}
	if err := source.Create(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	for _, requirement := range []selector.Requirement{
		selector.RequirementEqual("encoded", "wire:abc"),
		selector.RequirementDoesNotExist("optional"),
		selector.RequirementEqual("nil", nil),
		selector.RequirementEqual("zero", nil),
		selector.RequirementDoesNotExist("Hidden"),
		selector.RequirementEqual("at", at.In(time.FixedZone("east", 3600))),
		selector.RequirementGreaterThan("nested.count", int64(9007199254740992)),
		selector.RequirementEqual("nested.count", int64(9007199254740993)),
	} {
		result := &store.List[fieldRecord]{}
		if err := source.List(t.Context(), result, store.WithFieldRequirements(requirement)); err != nil {
			t.Fatal(err)
		}
		if len(result.Items) != 1 {
			t.Fatalf("%v: %+v", requirement, result.Items)
		}
	}
}
