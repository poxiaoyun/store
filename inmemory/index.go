package inmemory

import (
	"cmp"
	"encoding/json"
	"iter"
	"math/big"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/google/btree"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/selector"
)

// objectIndex owns one ordered composite index and its column type statistics.
// The collection lock protects entries and statistics together.
type objectIndex struct {
	definition store.Index
	fields     []indexField
	entries    *btree.BTreeG[indexEntry]
}

type indexField struct {
	path  []string
	scope int
	kinds [indexKindCount]int
}

type indexKind int

const (
	indexNull indexKind = iota
	indexBool
	indexNumber
	indexTime
	indexString
	indexCompound
	indexKindCount
)

// indexValue preserves stored types; selector operand coercion belongs to planning.
type indexValue struct {
	kind    indexKind
	number  *big.Rat
	instant time.Time
	text    string
}

func newIndexValue(value any) indexValue {
	value, _ = selector.ComparisonValue(value)
	if value == nil {
		return indexValue{}
	}
	if at, ok := value.(time.Time); ok {
		return indexValue{kind: indexTime, instant: at}
	}
	ref := reflect.ValueOf(value)
	switch ref.Kind() {
	case reflect.String:
		return indexValue{kind: indexString, text: ref.String()}
	case reflect.Bool:
		if ref.Bool() {
			return indexValue{kind: indexBool, text: "true"}
		}
		return indexValue{kind: indexBool, text: "false"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return indexValue{kind: indexNumber, number: new(big.Rat).SetInt64(ref.Int())}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return indexValue{kind: indexNumber, number: new(big.Rat).SetInt(new(big.Int).SetUint64(ref.Uint()))}
	case reflect.Float32, reflect.Float64:
		return indexValue{kind: indexNumber, number: new(big.Rat).SetFloat64(ref.Float())}
	default:
		// Stored objects have already passed their JSON codecs.
		data, _ := json.Marshal(value)
		return indexValue{kind: indexCompound, text: string(data)}
	}
}

func (v indexValue) compare(other indexValue) int {
	if order := cmp.Compare(v.kind, other.kind); order != 0 {
		return order
	}
	switch v.kind {
	case indexNumber:
		return v.number.Cmp(other.number)
	case indexTime:
		return v.instant.Compare(other.instant)
	default:
		return strings.Compare(v.text, other.text)
	}
}

type indexEntry struct {
	values []indexValue
	key    string
	// edge positions a scan boundary before (-1) or after (+1) every entry
	// sharing its value prefix. Stored entries use zero and a complete key.
	edge int
}

func compareIndexEntries(a, b indexEntry) int {
	for n := range min(len(a.values), len(b.values)) {
		if order := a.values[n].compare(b.values[n]); order != 0 {
			return order
		}
	}
	if len(a.values) < len(b.values) {
		return a.edge
	}
	if len(a.values) > len(b.values) {
		return -b.edge
	}
	if order := cmp.Compare(a.edge, b.edge); order != 0 {
		return order
	}
	return strings.Compare(a.key, b.key)
}

type indexBound struct {
	values    []indexValue
	inclusive bool
}

type indexRange struct {
	lower, upper *indexBound
}

func newIndex(definition store.Index, scopeKeys []string) *objectIndex {
	index := &objectIndex{
		definition: definition,
		entries:    btree.NewG(32, func(a, b indexEntry) bool { return compareIndexEntries(a, b) < 0 }),
	}
	for _, name := range definition.Fields {
		index.fields = append(index.fields, indexField{path: strings.Split(name, "."), scope: slices.Index(scopeKeys, name)})
	}
	return index
}

func (i *objectIndex) entry(key string, object store.Object) indexEntry {
	entry := indexEntry{key: key, values: make([]indexValue, len(i.fields))}
	for n, field := range i.fields {
		var value any
		if field.scope >= 0 {
			if scopes := object.GetScopes(); field.scope < len(scopes) {
				value = scopes[field.scope].Name
			}
		} else {
			value, _ = fieldValue(object, field.path)
		}
		entry.values[n] = newIndexValue(value)
	}
	return entry
}

func (i *objectIndex) checkUnique(resource, key string, object store.Object) error {
	if !i.definition.Unique {
		return nil
	}
	values := i.entry(key, object).values
	if i.definition.Nullable && slices.ContainsFunc(values, func(value indexValue) bool { return value.kind == indexNull }) {
		return nil
	}
	bound := &indexBound{values: values, inclusive: true}
	for candidate := range i.scan(indexRange{lower: bound, upper: bound}, false) {
		if candidate != key {
			return errors.NewAlreadyExists(resource, "index "+i.definition.Name)
		}
	}
	return nil
}

func (i *objectIndex) insert(key string, object store.Object) {
	entry := i.entry(key, object)
	i.entries.ReplaceOrInsert(entry)
	for n, value := range entry.values {
		i.fields[n].kinds[value.kind]++
	}
}

func (i *objectIndex) remove(key string, object store.Object) {
	entry := i.entry(key, object)
	i.entries.Delete(entry)
	for n, value := range entry.values {
		i.fields[n].kinds[value.kind]--
	}
}

// scan visits a bounded interval in either direction, stopping when yield stops.
func (i *objectIndex) scan(bounds indexRange, reverse bool) iter.Seq[string] {
	var lower, upper indexEntry
	if bounds.lower != nil {
		lower = indexEntry{values: bounds.lower.values, edge: 1}
		if bounds.lower.inclusive {
			lower.edge = -1
		}
	}
	if bounds.upper != nil {
		upper = indexEntry{values: bounds.upper.values, edge: -1}
		if bounds.upper.inclusive {
			upper.edge = 1
		}
	}
	return func(yield func(string) bool) {
		if reverse {
			visit := func(entry indexEntry) bool {
				return (bounds.lower == nil || compareIndexEntries(entry, lower) >= 0) && yield(entry.key)
			}
			if bounds.upper == nil {
				i.entries.Descend(visit)
			} else {
				i.entries.DescendLessOrEqual(upper, visit)
			}
			return
		}
		visit := func(entry indexEntry) bool {
			return (bounds.upper == nil || compareIndexEntries(entry, upper) <= 0) && yield(entry.key)
		}
		if bounds.lower == nil {
			i.entries.Ascend(visit)
		} else {
			i.entries.AscendGreaterOrEqual(lower, visit)
		}
	}
}
