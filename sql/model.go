package sql

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"xiaoshiai.cn/common/meta"
)

type ModelCodec struct {
	NameFunc func(string) string
}

func NewModelCodec() *ModelCodec {
	return &ModelCodec{NameFunc: func(name string) string {
		return name
	}}
}

func (h *ModelCodec) Fields(target any) []string {
	fields, err := h.ModelFields(target)
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		names = append(names, field.Name)
	}
	return names
}

// ModelField is one persistence field mapped to a table column.
type ModelField struct {
	Name string
	Type reflect.Type
}

// ModelFields returns the model's columns in declaration order, flattening
// embedded structs and skipping fields excluded from the representation. The
// declared order keeps generated DDL stable.
func (h *ModelCodec) ModelFields(target any) ([]ModelField, error) {
	t := reflect.TypeOf(target)
	for t != nil && (t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice || t.Kind() == reflect.Array) {
		t = t.Elem()
	}
	if t == nil || t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("model %T is not a struct", target)
	}
	var fields []ModelField
	if err := h.collectModelFields(t, &fields, map[string]struct{}{}); err != nil {
		return nil, err
	}
	return fields, nil
}

func (h *ModelCodec) collectModelFields(t reflect.Type, fields *[]ModelField, seen map[string]struct{}) error {
	for i := range t.NumField() {
		structfield := t.Field(i)
		isEmbedded, isIgnore, filedName := structFieldInfo(structfield, reflect.Value{})
		if isIgnore {
			continue
		}
		if isEmbedded {
			if err := h.collectModelFields(structfield.Type, fields, seen); err != nil {
				return err
			}
			continue
		}
		if h.NameFunc != nil {
			filedName = h.NameFunc(filedName)
		}
		if filedName == "" {
			continue
		}
		if _, exists := seen[filedName]; exists {
			// One column per name: a model that declares the same
			// representation name twice cannot be stored faithfully.
			return fmt.Errorf("model %s declares field %q more than once", t, filedName)
		}
		seen[filedName] = struct{}{}
		*fields = append(*fields, ModelField{Name: filedName, Type: structfield.Type})
	}
	return nil
}

func (h *ModelCodec) ToDriverValueMap(target any) map[string]any {
	result := map[string]any{}
	kvs := h.FieldMap(reflect.ValueOf(target), false)
	for name, value := range kvs {
		result[name] = ToDriverValue(value)
	}
	return result
}

// nolint: cyclop
func ToDriverValue(v reflect.Value) any {
	if !v.IsValid() {
		// A nil patch value reaches the driver as NULL.
		return nil
	}
	switch typed := v.Interface().(type) {
	case meta.Time:
		return typed.Time
	case *meta.Time:
		if typed == nil {
			return nil
		}
		return typed.Time
	}
	if driver.IsValue(v.Interface()) {
		return v.Interface()
	}
	if valuer, ok := reflect.TypeAssert[driver.Valuer](v); ok {
		return valuer
	}
	switch v.Kind() {
	case reflect.String:
		return v.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(v.Uint())
	case reflect.Float32, reflect.Float64:
		return v.Float()
	case reflect.Slice:
		if v.Type().
			Elem().
			Kind() == reflect.Uint8 {
			return v.Bytes()
		}
	case reflect.Bool:
		return v.Bool()
	}
	// is nil pointer
	if v.Kind() == reflect.Ptr && v.IsNil() {
		return nil
	}
	return &JsonValuer{Source: v.Interface()}
}

// collectFieldValues gathers every model field by representation name, including
// an omitted optional field that currently holds its zero value.
func (h *ModelCodec) collectFieldValues(v reflect.Value, values map[string]reflect.Value) {
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return
	}
	for i := range v.NumField() {
		structfield := v.Type().
			Field(i)
		fieldvalue := v.Field(i)
		isEmbedded, isIgnore, filedName := structFieldInfo(structfield, reflect.Value{})
		if isIgnore {
			continue
		}
		if isEmbedded {
			h.collectFieldValues(fieldvalue, values)
			continue
		}
		if h.NameFunc != nil {
			filedName = h.NameFunc(filedName)
		}
		if filedName == "" {
			continue
		}
		values[filedName] = fieldvalue
	}
}

func (h *ModelCodec) ScanAll(rows *sql.Rows, dest any) error {
	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Ptr || value.IsNil() || reflect.Indirect(value).
		Kind() != reflect.Slice {
		return errors.New("must pass a pointer to slice")
	}
	directslice := reflect.Indirect(value)
	directslice.SetLen(0)
	itemtp := directslice.Type().
		Elem()
	for rows.Next() {
		itemptr := reflect.New(itemtp)
		if err := h.ScanOne(rows, itemptr.Interface()); err != nil {
			return err
		}
		directslice = reflect.Append(directslice, itemptr.Elem())
	}
	value.Elem().
		Set(directslice)
	return rows.Err()
}

func (h *ModelCodec) ScanOne(rows *sql.Rows, intov any) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	if len(columns) == 0 {
		return nil
	}
	v := indirect(reflect.ValueOf(intov))
	if v.Kind() == reflect.Map {
		return h.scanOneMap(rows, columns, v)
	}

	values := make([]any, 0, len(columns))
	fieldsmap := h.FieldMap(v, true)
	for _, column := range columns {
		if field, ok := fieldsmap[column]; ok {
			values = append(values, ToDriverScanner(field))
		} else {
			values = append(values, new(any)) // scan to empty
		}
	}
	return rows.Scan(values...)
}

func (h *ModelCodec) scanOneMap(rows *sql.Rows, columns []string, v reflect.Value) error {
	mapvtype := v.Type().
		Elem()
	values := make([]any, 0, len(columns))
	mapvalues := make([]reflect.Value, 0, len(columns))
	for range columns {
		mapv := reflect.New(mapvtype).
			Elem()
		values = append(values, ToDriverScanner(mapv))
		mapvalues = append(mapvalues, mapv)
	}
	if err := rows.Scan(values...); err != nil {
		return err
	}
	if !v.IsValid() || v.IsNil() {
		v.Set(reflect.MakeMap(v.Type()))
	}
	for i, column := range columns {
		v.SetMapIndex(reflect.ValueOf(column), mapvalues[i])
	}
	return nil
}

func indirect(v reflect.Value) reflect.Value {
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	return v
}

func (h *ModelCodec) TrySetTime(v reflect.Value, tim time.Time) {
	if !v.IsZero() || !v.CanSet() {
		return
	}
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() == reflect.TypeFor[time.Time]().
		Kind() {
		v.Set(reflect.ValueOf(tim))
	}
}

// IntScanner scans a nullable integer column into any integer field. A NULL
// column leaves the field at its zero value, which is how an omitted optional
// field round-trips.
type IntScanner struct {
	Dest reflect.Value
}

func (i IntScanner) Scan(src any) error {
	var scanned sql.NullInt64
	if err := scanned.Scan(src); err != nil {
		return err
	}
	if !scanned.Valid {
		i.Dest.SetZero()
		return nil
	}
	i.Dest.Set(reflect.ValueOf(scanned.Int64).
		Convert(i.Dest.Type()))
	return nil
}

// FloatScanner scans a nullable floating point column. A NULL column leaves the
// field at its zero value.
type FloatScanner struct {
	Dest reflect.Value
}

func (f FloatScanner) Scan(src any) error {
	var scanned sql.NullFloat64
	if err := scanned.Scan(src); err != nil {
		return err
	}
	if !scanned.Valid {
		f.Dest.SetZero()
		return nil
	}
	f.Dest.Set(reflect.ValueOf(scanned.Float64).
		Convert(f.Dest.Type()))
	return nil
}

// MetaTimeScanner scans a timestamp column into a meta.Time field. A NULL
// column leaves the field at its zero value.
type MetaTimeScanner struct {
	val reflect.Value
}

func (m *MetaTimeScanner) Scan(value any) error {
	var scanned sql.NullTime
	if err := scanned.Scan(value); err != nil {
		return err
	}
	if !scanned.Valid {
		m.val.SetZero()
		return nil
	}
	m.val.Set(reflect.ValueOf(meta.Time{Time: scanned.Time}))
	return nil
}

type NullTimeScanner struct {
	inner *sql.NullTime
	val   reflect.Value
}

func (n *NullTimeScanner) Scan(value any) error {
	if err := n.inner.Scan(value); err != nil {
		return err
	}
	if !n.inner.Valid {
		n.val.SetZero()
		return nil
	}
	// The field is a *time.Time or a *meta.Time; build the pointer its own
	// element type accepts.
	element := n.val.Type().
		Elem()
	scanned := reflect.New(element)
	switch element {
	case metaTimeType:
		scanned.Elem().
			Set(reflect.ValueOf(meta.Time{Time: n.inner.Time}))
	default:
		scanned.Elem().
			Set(reflect.ValueOf(n.inner.Time))
	}
	n.val.Set(scanned)
	return nil
}

type BoolScanner struct {
	Dest reflect.Value
}

func (b BoolScanner) Scan(src any) error {
	// handler bitmap(0,1) to bool
	if data, ok := src.([]byte); ok {
		if len(data) == 0 {
			return nil
		}
		switch data[0] {
		case 0:
			src = false
		case 1:
			src = true
		}
	}
	val, err := driver.Bool.ConvertValue(src)
	if err != nil {
		return err
	}
	bv, _ := val.(bool)
	b.Dest.SetBool(bv)
	return nil
}

type StringScanner struct {
	Dest reflect.Value
}

func (s StringScanner) Scan(src any) error {
	nulstr := sql.NullString{}
	if err := nulstr.Scan(src); err != nil {
		return err
	}
	s.Dest.SetString(nulstr.String)
	return nil
}

type JsonScanner struct {
	Dest reflect.Value
}

func (v JsonScanner) Scan(src any) error {
	var data []byte
	switch val := src.(type) {
	case []byte:
		data = val
	case string:
		data = []byte(val)
	case nil:
	default:
		return fmt.Errorf("unsupported type: %T", src)
	}
	if len(data) == 0 || string(data) == "null" {
		if v.Dest.CanSet() {
			v.Dest.SetZero()
		}
		return nil
	}
	if !v.Dest.CanSet() {
		return fmt.Errorf("cannot set %s", v.Dest.Type())
	}
	// The stored document is the complete value. Decoding merges into an
	// existing map or struct, which would keep values the stored document no
	// longer has, so the destination resets first.
	v.Dest.SetZero()
	return json.Unmarshal(data, v.Dest.Addr().
		Interface())
}

type JsonValuer struct {
	Source any
}

func (a JsonValuer) Value() (driver.Value, error) {
	if a.Source == nil {
		return nil, nil
	}
	return json.Marshal(a.Source)
}

// ToDriverScanner converts a struct fields to pointers to a sql.Valuer
func ToDriverScanner(into reflect.Value) any {
	if scanner, ok := reflect.TypeAssert[sql.Scanner](into); ok {
		return scanner
	}
	switch into.Interface().(type) {
	case time.Time:
		return into.Addr().
			Interface()
	case meta.Time:
		return &MetaTimeScanner{val: into}
	case *meta.Time:
		return &NullTimeScanner{inner: &sql.NullTime{}, val: into}
	case *time.Time:
		return &NullTimeScanner{inner: &sql.NullTime{}, val: into}
	case *any:
		return into.Interface()
	case nil:
		return &AnyScanner{Dest: into}
	}
	switch into.Kind() {
	case reflect.Struct, reflect.Map, reflect.Slice, reflect.Array, reflect.Interface, reflect.Pointer:
		return &JsonScanner{Dest: into}
	case reflect.Bool:
		return &BoolScanner{Dest: into}
	case reflect.String:
		return &StringScanner{Dest: into}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return &IntScanner{Dest: into}
	case reflect.Float32, reflect.Float64:
		return &FloatScanner{Dest: into}
	default:
		return into.Addr().
			Interface()
	}
}

type AnyScanner struct {
	Dest reflect.Value
}

func (a AnyScanner) Scan(src any) error {
	switch val := src.(type) {
	case []byte:
		if len(val) == 0 {
			return nil
		}
		a.Dest.Set(reflect.ValueOf(string(val)))
	case nil:
		a.Dest.SetZero()
	default:
		a.Dest.Set(reflect.ValueOf(src))
	}
	return nil
}

// nolint: cyclop
func (h *ModelCodec) FieldMap(v reflect.Value, withinit bool) map[string]reflect.Value {
	for v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if v.Kind() == reflect.Map {
		ret := map[string]reflect.Value{}
		iter := v.MapRange()
		for iter.Next() {
			iterk, iterv := iter.Key(), iter.Value()
			if iterv.Kind() == reflect.Pointer && iterv.IsNil() && withinit {
				iterv.Set(reflect.New(iterv.Type().
					Elem()))
			}
			ret[fmt.Sprintf("%v", iterk.Interface())] = iterv
		}
		return ret
	}
	if v.Kind() != reflect.Struct {
		return nil
	}
	t := v.Type()
	ret := map[string]reflect.Value{}
	for i := range v.NumField() {
		structfield, fieldvalues := t.Field(i), v.Field(i)
		if fieldvalues.Kind() == reflect.Pointer && fieldvalues.IsNil() && withinit {
			fieldvalues.Set(reflect.New(structfield.Type).
				Elem())
		}
		isEmbedded, isIgnore, filedName := structFieldInfo(structfield, fieldvalues)
		if !withinit && isIgnore {
			continue
		}
		if isEmbedded {
			for k, v := range h.FieldMap(fieldvalues, withinit) {
				// embedded field if lower priority then same name field in parent
				if _, ok := ret[k]; !ok {
					ret[k] = v
				}
			}
			continue
		}
		if h.NameFunc != nil {
			filedName = h.NameFunc(filedName)
		}
		if filedName == "" {
			continue
		}
		ret[filedName] = fieldvalues
	}
	return ret
}

func structFieldInfo(structField reflect.StructField, value reflect.Value) (bool, bool, string) {
	isEmbedded, isIgnored, fieldName := structField.Anonymous, false, structField.Name
	// json
	if jsonTag := structField.Tag.Get("json"); jsonTag != "" {
		opts := strings.Split(jsonTag, ",")
		switch val := opts[0]; val {
		case "-":
			isIgnored = true
		case "":
		default:
			fieldName = val
			isEmbedded = false // if field is embedded,but json tag has name,then it is not embedded
		}
		for _, opt := range opts[1:] {
			switch opt {
			case "omitempty":
				if value.IsValid() && value.IsZero() {
					isIgnored = true
				}
			case "inline":
				isEmbedded = true
			}
		}
	}
	return isEmbedded, isIgnored, fieldName
}
