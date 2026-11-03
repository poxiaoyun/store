package inmemory

import (
	"context"
	"fmt"
	"iter"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/google/btree"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/meta"
	"xiaoshiai.cn/store"
)

// collection owns one resource. Stored objects are immutable; all mutations
// commit the object and its independent indexes under mu.
type collection struct {
	mu         sync.RWMutex
	definition store.ResourceSchema
	objects    map[string]store.Object
	indexes    []*objectIndex
	keys       *btree.BTreeG[string]
}

func newCollection(definition store.ResourceSchema) *collection {
	c := &collection{definition: definition, objects: map[string]store.Object{}, keys: btree.NewOrderedG[string](32)}
	for _, index := range definition.Indexes {
		c.indexes = append(c.indexes, newIndex(index, definition.ScopeKeys))
	}
	return c
}

// prepare copies through the model codec, retaining the schema's native types
// even when the Watch event or caller uses Unstructured.
func (c *collection) prepare(source store.Object, revision int64) (store.Object, error) {
	prototype := c.definition.Object
	if prototype == nil {
		prototype = source
	}
	object := store.NewObject(prototype)
	if err := store.CopyObject(source, object); err != nil {
		return nil, err
	}
	object.SetResourceVersion(revision)
	return object, nil
}

// apply commits a source mutation while retaining its metadata and version.
func (c *collection) apply(kind store.WatchEventType, object store.Object) error {
	key := getkey(object.GetScopes(), object.GetID())
	c.mu.Lock()
	defer c.mu.Unlock()
	switch kind {
	case store.WatchEventDelete:
		c.commit(key, nil)
		return nil
	case store.WatchEventCreate, store.WatchEventUpdate:
		value, err := c.prepare(object, object.GetResourceVersion())
		if err != nil {
			return err
		}
		c.commit(key, value)
		return nil
	default:
		return errors.NewBadRequest("expected object mutation")
	}
}

func (c *collection) checkUnique(key string, object store.Object) error {
	for _, index := range c.indexes {
		if err := index.checkUnique(c.definition.ResourceName, key, object); err != nil {
			return err
		}
	}
	return nil
}

// commit is the sole mutation path after authoritative checks succeed.
func (c *collection) commit(key string, object store.Object) {
	if old, exists := c.objects[key]; exists {
		for _, index := range c.indexes {
			index.remove(key, old)
		}
	}
	if object == nil {
		delete(c.objects, key)
		c.keys.Delete(key)
		return
	}
	c.objects[key] = object
	c.keys.ReplaceOrInsert(key)
	for _, index := range c.indexes {
		index.insert(key, object)
	}
}

func (c *collection) count(ctx context.Context, scopes []store.Scope, opts ...store.CountOption) (int, error) {
	options := store.ApplyCountOptions(opts)
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return 0, err
	}
	objects, err := c.list(ctx, scopes, store.ListOptions{
		IncludeSubScopes:  options.IncludeSubScopes,
		FieldRequirements: options.FieldRequirements,
		LabelRequirements: options.LabelRequirements,
		Search:            options.Search,
		SearchFields:      options.SearchFields,
	})
	if err != nil {
		return 0, err
	}
	return len(objects), nil
}

func (c *collection) getObject(ctx context.Context, scopes []store.Scope, id string, into store.Object, options store.GetOptions) error {
	if options.ResourceVersion != nil && *options.ResourceVersion > 0 {
		return errors.NewUnsupported("collection has no collection freshness version")
	}
	if len(options.Fields) != 0 {
		return errors.NewUnsupported("in-memory store does not support field projection")
	}
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.RLock()
	object := c.objects[getkey(scopes, id)]
	c.mu.RUnlock()
	if object == nil || !matches(object, options.LabelRequirements, options.FieldRequirements) {
		return errors.NewNotFound(c.definition.ResourceName, id)
	}
	if err := store.ValidatePreconditions(object, options.Preconditions); err != nil {
		return err
	}
	return store.CopyObject(object, into)
}

func (c *collection) listObjects(ctx context.Context, scopes []store.Scope, list store.ObjectList, options store.ListOptions) error {
	if options.ResourceVersion != nil && *options.ResourceVersion > 0 {
		return errors.NewUnsupported("collection has no collection freshness version")
	}
	if len(options.Fields) != 0 {
		return errors.NewUnsupported("in-memory store does not support field projection")
	}
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	page, err := store.PrepareListPage(c.definition.ResourceName, scopes, options)
	if err != nil {
		return err
	}
	options = page.Options
	items, newItem, err := store.NewItemFuncFromList(list)
	if err != nil {
		return err
	}
	results, err := c.list(ctx, scopes, options)
	if err != nil {
		return err
	}
	items.Set(reflect.MakeSlice(items.Type(), 0, len(results)))
	for _, object := range results {
		target := newItem()
		if err := store.CopyObject(object, target); err != nil {
			return err
		}
		items.Set(reflect.Append(items, reflect.ValueOf(target).Elem()))
	}
	list.SetResource(c.definition.ResourceName)
	list.SetScopes(slices.Clone(scopes))
	list.SetResourceVersion(0)
	return page.Finish(list)
}

// list executes one filtered scan, sorting only when the index order is incompatible.
func (c *collection) list(ctx context.Context, scopes []store.Scope, options store.ListOptions) ([]store.Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sorts := store.ParseSorts(options.Sort)
	c.mu.RLock()
	plan := c.plan(scopes, options, sorts)
	prefix := getkey(scopes, "")
	values := []store.Object{}
	for key := range c.candidates(scopes, plan) {
		if !strings.HasPrefix(key, prefix) || !options.IncludeSubScopes && strings.Contains(strings.TrimPrefix(key, prefix), "/") {
			continue
		}
		object := c.objects[key]
		if !matches(object, options.LabelRequirements, options.FieldRequirements) || !matchesSearch(object, options.Search, options.SearchFields) {
			continue
		}
		values = append(values, object)
		if plan.ordered && options.Limit > 0 && len(values) >= options.Offset+options.Limit {
			break
		}
	}
	c.mu.RUnlock()
	if !plan.ordered {
		slices.SortStableFunc(values, func(a, b store.Object) int { return compareObjects(a, b, sorts) })
	}
	start, end := min(options.Offset, len(values)), len(values)
	if options.Limit > 0 {
		end = min(start+options.Limit, end)
	}
	return values[start:end], nil
}

func (c *collection) candidates(scopes []store.Scope, plan indexPlan) iter.Seq[string] {
	if plan.index != nil {
		return plan.index.scan(plan.bounds, plan.reverse)
	}
	identity := getkey(scopes, "")
	return func(yield func(string) bool) {
		c.keys.AscendGreaterOrEqual(identity, func(key string) bool { return strings.HasPrefix(key, identity) && yield(key) })
	}
}

func getkey(scopes []store.Scope, id string) string {
	var key strings.Builder
	for _, scope := range scopes {
		key.WriteByte('/')
		key.WriteString(url.PathEscape(scope.Resource))
		key.WriteByte('/')
		key.WriteString(url.PathEscape(scope.Name))
	}
	key.WriteByte('/')
	key.WriteString(url.PathEscape(id))
	return key.String()
}

func matches(object store.Object, labels, fields store.Requirements) bool {
	return store.MatchLabelReqirements(object, labels) && fields.Match(func(name string) (any, bool) { return fieldValue(object, strings.Split(name, ".")) })
}

func matchesSearch(object store.Object, search string, fields []string) bool {
	if search == "" {
		return true
	}
	if len(fields) == 0 {
		fields = []string{"id", "name"}
	}
	search = strings.ToLower(search)
	for _, name := range fields {
		if value, found := fieldValue(object, strings.Split(name, ".")); found && strings.Contains(strings.ToLower(fmt.Sprint(value)), search) {
			return true
		}
	}
	return false
}

func compareObjects(a, b store.Object, sorts []meta.SortField) int {
	for _, order := range sorts {
		left, _ := fieldValue(a, strings.Split(order.Field, "."))
		right, _ := fieldValue(b, strings.Split(order.Field, "."))
		comparison := store.CompareField(left, right)
		if order.Direction == meta.SortDirectionDesc {
			comparison = -comparison
		}
		if comparison != 0 {
			return comparison
		}
	}
	return 0
}
