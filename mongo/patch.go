package mongo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
)

// patchField is one decoded field edit of a Mongo-supported patch.
type patchField struct {
	Column   string
	Path     []string
	Value    any
	Remove   bool
	FromPath []string
}

// patchTest is one JSON Patch "test" condition.
type patchTest struct {
	Path  []string
	Value any
}

// fieldPatch applies relative field operations in one native update. The
// filter carries every condition, so the decision and the write are a single
// document operation.
func (m *MongoStorage) fieldPatch(ctx context.Context, obj store.Object, patch store.Patch, status bool, options store.PatchOptions) error {
	data, err := patch.Data(obj)
	if err != nil {
		return err
	}
	operations, err := decodeFieldPatch(data)
	if err != nil {
		return err
	}
	return m.applyPatchOperations(ctx, obj, operations, false, status, options)
}

// jsonPatch applies the supported subset of RFC 6902: object-field add,
// replace and remove. test, copy, move and array element paths are unsupported.
// replace and remove use native set/unset semantics, so replace creates a
// missing field and remove is a no-op for a missing field.
func (m *MongoStorage) jsonPatch(ctx context.Context, obj store.Object, patch store.Patch, status bool, options store.PatchOptions) error {
	data, err := patch.Data(obj)
	if err != nil {
		return err
	}
	fields, tests, err := decodeJSONPatch(data)
	if err != nil {
		return err
	}
	operations, err := jsonPatchOperations(fields, tests)
	if err != nil {
		return err
	}
	return m.applyPatchOperations(ctx, obj, operations, true, status, options)
}

func jsonPatchOperations(fields []patchField, tests []patchTest) ([]store.FieldOperation, error) {
	if len(tests) > 0 {
		return nil, errors.NewUnsupported("JSON patch test is not supported")
	}
	operations := make([]store.FieldOperation, 0, len(fields))
	for _, field := range fields {
		if len(field.FromPath) > 0 {
			return nil, errors.NewUnsupported("JSON patch copy and move are not supported")
		}
		path := field.Column
		if len(field.Path) > 0 {
			path += "." + strings.Join(field.Path, ".")
		}
		operation := store.FieldOperation{Op: store.FieldOpSet, Path: path, Value: field.Value}
		if field.Remove {
			operation.Op = store.FieldOpUnset
		}
		operations = append(operations, operation)
	}
	return operations, nil
}

func (m *MongoStorage) applyPatchOperations(ctx context.Context, obj store.Object, operations []store.FieldOperation, unsupported, status bool, options store.PatchOptions) error {
	if obj.GetID() == "" {
		return errors.NewBadRequest("id is required")
	}
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	resource, err := m.core.schema.ResourceName(obj)
	if err != nil {
		return err
	}
	definition, err := m.core.schema.Resource(resource)
	if err != nil {
		return err
	}
	if err := m.validateFieldPatch(definition, operations, status); err != nil {
		if unsupported {
			return errors.NewUnsupported(err.Error())
		}
		return err
	}
	generationPaths := []string(nil)
	if !status {
		generationPaths = businessPatchPaths(operations)
	}
	return m.applyFieldOperations(ctx, obj, operations, generationPaths, options)
}

func businessPatchPaths(operations []store.FieldOperation) []string {
	paths := make([]string, 0, len(operations))
	seen := map[string]struct{}{}
	for _, operation := range operations {
		if !isBusinessPath(operation.Path) {
			continue
		}
		if _, exists := seen[operation.Path]; exists {
			continue
		}
		seen[operation.Path] = struct{}{}
		paths = append(paths, operation.Path)
	}
	return paths
}

func isBusinessPath(path string) bool {
	for _, field := range generationIgnoredFields {
		if fieldPathTouches(path, field) {
			return false
		}
	}
	return true
}

func (m *MongoStorage) applyFieldOperations(ctx context.Context, obj store.Object, operations []store.FieldOperation, generationPaths []string, options store.PatchOptions) error {
	return m.on(ctx, obj, false, func(ctx context.Context, col *mongo.Collection, filter bson.D) error {
		existsFilter := append(bson.D(nil), filter...)
		existsFilter = append(existsFilter, bson.E{Key: "id", Value: obj.GetID()})
		filter = append(filter, bson.E{Key: "id", Value: obj.GetID()})
		filter = ConditionsMatch(filter, options.LabelRequirements, options.FieldRequirements, "")
		if preconditions := options.Preconditions; preconditions != nil {
			if preconditions.UID != nil {
				filter = append(filter, bson.E{Key: "uid", Value: *preconditions.UID})
			}
			if preconditions.ResourceVersion != nil {
				filter = append(filter, bson.E{Key: "resourceVersion", Value: *preconditions.ResourceVersion})
			}
		}
		update, err := buildFieldPatchUpdate(operations, generationPaths)
		if err != nil {
			return err
		}
		result := col.FindOneAndUpdate(ctx, filter, update,
			mongooptions.FindOneAndUpdate().SetReturnDocument(mongooptions.After))
		if err := result.Err(); err != nil {
			if err == mongo.ErrNoDocuments {
				if existsErr := col.FindOne(ctx, existsFilter).Err(); existsErr == nil {
					return errors.NewConflict(col.Name(), obj.GetID(), fmt.Errorf("write conditions did not match"))
				} else if existsErr != mongo.ErrNoDocuments {
					return WarpMongoError(existsErr, col, obj)
				}
			}
			return WarpMongoError(err, col, obj)
		}
		return nil
	})
}

func (m *MongoStorage) requireStatusResource(obj store.Object) error {
	resource, err := m.core.schema.ResourceName(obj)
	if err != nil {
		return err
	}
	definition, err := m.core.schema.Resource(resource)
	if err != nil {
		return err
	}
	if !definition.StatusResource {
		return errors.NewUnsupported("resource has no status")
	}
	return nil
}

func (m *MongoStorage) validateFieldPatch(definition store.ResourceSchema, operations []store.FieldOperation, status bool) error {
	if status && !definition.StatusResource {
		return errors.NewUnsupported("resource has no status")
	}
	forbidden := append(serverOwnedUpdateFields, "generation", "finalizers")
	for _, operation := range operations {
		if operation.Op == store.FieldOpExpr && operation.Expr == nil {
			return errors.NewBadRequest("field patch expression is required")
		}
		if operation.Expr != nil {
			if err := operation.Expr.Validate(); err != nil {
				return err
			}
		}
		if status {
			if !fieldPathTouches(operation.Path, "status") {
				return errors.NewBadRequest("status patch may only modify status")
			}
		} else if fieldPathTouches(operation.Path, "status") {
			return errors.NewBadRequest("status must be patched through Status()")
		}
		for _, field := range forbidden {
			if fieldPathTouches(operation.Path, field) {
				return errors.NewBadRequest(fmt.Sprintf("field patch cannot modify %s", field))
			}
		}
	}
	return nil
}

// buildFieldPatchUpdate uses a classic update document only when operations
// are disjoint, no business field is touched, and no operation needs an
// expression. Overlapping paths and expressions need one pipeline stage per
// operation so the declared order and the stored values are observed.
func buildFieldPatchUpdate(operations []store.FieldOperation, generationPaths []string) (any, error) {
	if len(generationPaths) == 0 && !fieldPatchOperationsOverlap(operations) && !fieldPatchHasExpression(operations) {
		return buildClassicFieldPatch(operations), nil
	}
	return buildOrderedFieldPatch(operations, generationPaths)
}

func fieldPatchHasExpression(operations []store.FieldOperation) bool {
	for _, operation := range operations {
		if operation.Op == store.FieldOpExpr {
			return true
		}
	}
	return false
}

func fieldPatchOperationsOverlap(operations []store.FieldOperation) bool {
	for index := range operations {
		for previous := range index {
			if fieldPathTouches(operations[index].Path, operations[previous].Path) {
				return true
			}
		}
	}
	return false
}

func buildClassicFieldPatch(operations []store.FieldOperation) bson.D {
	set := bson.D{}
	unset := bson.D{}
	inc := bson.D{{Key: "resourceVersion", Value: int64(1)}}
	for _, operation := range operations {
		value := normalizeFieldPatchValue(operation.Value)
		switch operation.Op {
		case store.FieldOpSet:
			set = append(set, bson.E{Key: operation.Path, Value: value})
		case store.FieldOpUnset:
			unset = append(unset, bson.E{Key: operation.Path, Value: ""})
		case store.FieldOpInc:
			inc = append(inc, bson.E{Key: operation.Path, Value: value})
		}
	}
	update := bson.D{{Key: "$inc", Value: inc}}
	if len(set) > 0 {
		update = append(update, bson.E{Key: "$set", Value: set})
	}
	if len(unset) > 0 {
		update = append(update, bson.E{Key: "$unset", Value: unset})
	}
	return update
}

// buildOrderedFieldPatch applies one operation per pipeline stage so
// overlapping paths observe the results of earlier operations. Business paths
// are saved before the operations and Generation advances only if one of them
// actually changed. The whole pipeline is one atomic update.
func buildOrderedFieldPatch(operations []store.FieldOperation, generationPaths []string) (mongo.Pipeline, error) {
	sentinel := primitive.NewObjectID()
	pipeline := mongo.Pipeline{}
	if len(generationPaths) > 0 {
		before := bson.D{}
		for index, field := range generationPaths {
			before = append(before, bson.E{
				Key:   fmt.Sprintf("g%d", index),
				Value: bson.D{{Key: "$ifNull", Value: bson.A{"$" + field, sentinel}}},
			})
		}
		pipeline = append(pipeline, bson.D{{Key: "$set", Value: bson.D{{Key: "__store_generation", Value: before}}}})
	}
	for _, operation := range operations {
		value := normalizeFieldPatchValue(operation.Value)
		switch operation.Op {
		case store.FieldOpSet:
			pipeline = append(pipeline, bson.D{{Key: "$set", Value: bson.D{
				{Key: operation.Path, Value: bson.D{{Key: "$literal", Value: value}}},
			}}})
		case store.FieldOpUnset:
			pipeline = append(pipeline, bson.D{{Key: "$unset", Value: operation.Path}})
		case store.FieldOpInc:
			pipeline = append(pipeline, bson.D{{Key: "$set", Value: bson.D{
				{Key: operation.Path, Value: bson.D{{Key: "$add", Value: bson.A{
					bson.D{{Key: "$ifNull", Value: bson.A{"$" + operation.Path, int64(0)}}},
					value,
				}}}},
			}}})
		case store.FieldOpExpr:
			if operation.Expr == nil {
				return nil, errors.NewBadRequest("field patch expression is required")
			}
			compiled, err := buildExpr(*operation.Expr)
			if err != nil {
				return nil, err
			}
			pipeline = append(pipeline, bson.D{{Key: "$set", Value: bson.D{
				{Key: operation.Path, Value: compiled},
			}}})
		}
	}
	pipeline = append(pipeline, bson.D{{Key: "$set", Value: bson.D{
		{Key: "resourceVersion", Value: bson.D{{Key: "$add", Value: bson.A{"$resourceVersion", int64(1)}}}},
	}}})
	if len(generationPaths) == 0 {
		return pipeline, nil
	}
	changes := bson.A{}
	for index, field := range generationPaths {
		changes = append(changes, bson.D{{Key: "$ne", Value: bson.A{
			bson.D{{Key: "$ifNull", Value: bson.A{"$" + field, sentinel}}},
			"$__store_generation.g" + strconv.Itoa(index),
		}}})
	}
	pipeline = append(pipeline,
		bson.D{{Key: "$set", Value: bson.D{{Key: "generation", Value: bson.D{{Key: "$cond", Value: bson.A{
			bson.D{{Key: "$or", Value: changes}},
			bson.D{{Key: "$add", Value: bson.A{"$generation", int64(1)}}},
			"$generation",
		}}}}}}},
		bson.D{{Key: "$unset", Value: "__store_generation"}},
	)
	return pipeline, nil
}

func fieldPathTouches(path, field string) bool {
	return path == field || strings.HasPrefix(path, field+".") || strings.HasPrefix(field, path+".")
}

func normalizeFieldPatchValue(value any) any {
	switch typed := value.(type) {
	case json.Number:
		if integer, err := typed.Int64(); err == nil {
			return integer
		}
		if floating, err := typed.Float64(); err == nil {
			return floating
		}
		return typed.String()
	case []any:
		values := make([]any, len(typed))
		for index := range typed {
			values[index] = normalizeFieldPatchValue(typed[index])
		}
		return values
	case map[string]any:
		values := make(map[string]any, len(typed))
		for key, item := range typed {
			values[key] = normalizeFieldPatchValue(item)
		}
		return values
	default:
		return value
	}
}

// decodeFieldPatch decodes one field patch for the Mongo adapter.
func decodeFieldPatch(data []byte) ([]store.FieldOperation, error) {
	if !json.Valid(data) {
		return nil, errors.NewBadRequest("invalid field patch data")
	}
	var patch store.FieldPatch
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&patch); err != nil {
		return nil, errors.NewBadRequest("invalid field patch data")
	}
	if len(patch.Operations) == 0 {
		return nil, errors.NewBadRequest("field patch has no operations")
	}
	for _, operation := range patch.Operations {
		switch operation.Op {
		case store.FieldOpSet, store.FieldOpUnset:
		case store.FieldOpInc:
			if !fieldPatchNumber(operation.Value) {
				return nil, errors.NewBadRequest("inc value must be a number")
			}
		case store.FieldOpExpr:
			if operation.Expr == nil {
				return nil, errors.NewBadRequest("field patch expression is required")
			}
			if err := operation.Expr.Validate(); err != nil {
				return nil, err
			}
		default:
			return nil, errors.NewBadRequest(fmt.Sprintf("invalid field patch op: %s", operation.Op))
		}
		if operation.Path == "" {
			return nil, errors.NewBadRequest("field patch path is required")
		}
	}
	return patch.Operations, nil
}

func fieldPatchNumber(value any) bool {
	switch value.(type) {
	case json.Number, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	default:
		return false
	}
}

func applyMergePatchFields(target store.Object, fields []patchField) error {
	values, err := store.ObjectToMap(target)
	if err != nil {
		return err
	}
	for _, field := range fields {
		path := append([]string{field.Column}, field.Path...)
		if field.Remove {
			deletePatchPath(values, path)
			continue
		}
		setPatchPath(values, path, field.Value)
	}
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}
	store.ResetObject(target)
	if err := json.Unmarshal(data, target); err != nil {
		return errors.NewBadRequest(err.Error())
	}
	return nil
}

func setPatchPath(object map[string]any, path []string, value any) {
	current := object
	for _, segment := range path[:len(path)-1] {
		child, ok := current[segment].(map[string]any)
		if !ok {
			child = map[string]any{}
			current[segment] = child
		}
		current = child
	}
	current[path[len(path)-1]] = value
}

func deletePatchPath(object map[string]any, path []string) {
	current := object
	for _, segment := range path[:len(path)-1] {
		child, ok := current[segment].(map[string]any)
		if !ok {
			return
		}
		current = child
	}
	delete(current, path[len(path)-1])
}

// decodeMergePatch returns the field edits and the patch's resourceVersion
// condition. A merge patch merges into its document paths, keeping the keys it
// does not mention.
func decodeMergePatch(data []byte) ([]patchField, int64, error) {
	fields, err := decodeMergePatchFields(data)
	if err != nil {
		return nil, 0, err
	}
	filtered := make([]patchField, 0, len(fields))
	version := int64(0)
	for _, field := range fields {
		if field.Column == "resourceVersion" && len(field.Path) == 0 {
			number, ok := patchVersionNumber(field.Value)
			if !ok {
				return nil, 0, errors.NewBadRequest("resourceVersion must be an integer")
			}
			version = number
			continue
		}
		filtered = append(filtered, field)
	}
	return filtered, version, nil
}

// decodeMergePatchFields decodes an RFC 7386 merge patch. A nested object merges
// into its document path, an array or scalar replaces its value, and null
// removes it.
func decodeMergePatchFields(data []byte) ([]patchField, error) {
	var merged map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&merged); err != nil {
		return nil, errors.NewBadRequest("invalid merge patch data")
	}
	fields := make([]patchField, 0, len(merged))
	for _, name := range sortedMapKeys(merged) {
		decodeMergePatchField([]string{name}, merged[name], &fields)
	}
	return fields, nil
}

func decodeMergePatchField(path []string, value any, fields *[]patchField) {
	if object, ok := value.(map[string]any); ok && len(object) > 0 {
		for _, name := range sortedMapKeys(object) {
			decodeMergePatchField(append(slices.Clone(path), name), object[name], fields)
		}
		return
	}
	field := patchField{Column: path[0], Path: path[1:], Value: value}
	if value == nil {
		field.Remove = true
	}
	*fields = append(*fields, field)
}

// decodeJSONPatch decodes an RFC 6902 JSON Patch into field edits and tests.
func decodeJSONPatch(data []byte) ([]patchField, []patchTest, error) {
	var operations []map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&operations); err != nil {
		return nil, nil, errors.NewBadRequest("invalid JSON patch data")
	}
	fields := make([]patchField, 0, len(operations))
	tests := make([]patchTest, 0, len(operations))
	for _, operation := range operations {
		op, _ := operation["op"].(string)
		rawpath, ok := operation["path"].(string)
		if !ok || rawpath == "" {
			return nil, nil, errors.NewBadRequest("patch operation has no path")
		}
		path, err := store.JSONPointerPath(rawpath)
		if err != nil {
			return nil, nil, err
		}
		if op == "test" {
			value, exists := operation["value"]
			if !exists {
				return nil, nil, errors.NewBadRequest("test operation has no value")
			}
			tests = append(tests, patchTest{Path: path, Value: value})
			continue
		}
		switch op {
		case "add", "replace":
			fields = append(fields, patchField{Column: path[0], Path: path[1:], Value: operation["value"]})
		case "remove":
			fields = append(fields, patchField{Column: path[0], Path: path[1:], Remove: true})
		case "copy", "move":
			rawfrom, ok := operation["from"].(string)
			if !ok || rawfrom == "" {
				return nil, nil, errors.NewBadRequest("patch operation has no from")
			}
			from, err := store.JSONPointerPath(rawfrom)
			if err != nil {
				return nil, nil, err
			}
			fields = append(fields, patchField{Column: path[0], Path: path[1:], FromPath: from})
			if op == "move" {
				fields = append(fields, patchField{Column: from[0], Path: from[1:], Remove: true})
			}
		default:
			return nil, nil, errors.NewBadRequest(fmt.Sprintf("invalid patch op: %s", op))
		}
	}
	return fields, tests, nil
}

// patchVersionNumber reads an integer patch version.
func patchVersionNumber(value any) (int64, bool) {
	switch typed := value.(type) {
	case float64:
		return int64(typed), true
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	case json.Number:
		number, err := typed.Int64()
		return number, err == nil
	default:
		return 0, false
	}
}

// sortedMapKeys returns map keys in a stable order.
func sortedMapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
