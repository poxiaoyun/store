package mongo

import (
	"bytes"
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
	"k8s.io/apimachinery/pkg/api/resource"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/log"
	"xiaoshiai.cn/common/meta"
	libreflect "xiaoshiai.cn/common/reflect"
	"xiaoshiai.cn/common/selector"
	"xiaoshiai.cn/store"
)

type MongoDBOptions struct {
	Address    string `json:"address,omitempty"`
	Username   string `json:"username,omitempty"`
	Password   string `json:"password,omitempty" config:"password,sensitive"`
	Database   string `json:"database,omitempty"`
	ReplicaSet string `json:"replicaSet,omitempty"`
	Direct     bool   `json:"direct,omitempty"`
}

func NewDefaultMongoOptions(dbname string) *MongoDBOptions {
	return &MongoDBOptions{
		Address:    "localhost:27017",
		Username:   "admin",
		Password:   "",
		Database:   dbname,
		ReplicaSet: "",
		Direct:     false,
	}
}

var _ store.TransactionStore = &MongoStorage{}
var _ store.PingableStore = &MongoStorage{}

var GlobalBsonRegistry = bson.NewRegistry()

func init() {
	quantityType := reflect.TypeFor[resource.Quantity]()
	GlobalBsonRegistry.RegisterTypeEncoder(quantityType, BsonQuantityCodec{})
	GlobalBsonRegistry.RegisterTypeDecoder(quantityType, BsonQuantityCodec{})

	timeType := reflect.TypeFor[meta.Time]()
	GlobalBsonRegistry.RegisterTypeEncoder(timeType, BsonTimeCodec{})
	GlobalBsonRegistry.RegisterTypeDecoder(timeType, BsonTimeCodec{})
}

var (
	_ bsoncodec.ValueEncoder = BsonQuantityCodec{}
	_ bsoncodec.ValueDecoder = BsonQuantityCodec{}
)

type BsonTimeCodec struct{}

// DecodeValue implements bsoncodec.ValueDecoder.
func (b BsonTimeCodec) DecodeValue(ctx bsoncodec.DecodeContext, vr bsonrw.ValueReader, v reflect.Value) error {
	var value time.Time
	switch vr.Type() {
	case bson.TypeDateTime:
		datetime, err := vr.ReadDateTime()
		if err != nil {
			return err
		}
		dateTime := primitive.DateTime(datetime)
		value = dateTime.Time()
	case bson.TypeString:
		text, err := vr.ReadString()
		if err != nil {
			return err
		}
		value, err = time.Parse(time.RFC3339, text)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("cannot decode %s as time", vr.Type())
	}
	v.Set(reflect.ValueOf(meta.Time{Time: value}))
	return nil
}

// EncodeValue implements bsoncodec.ValueEncoder.
func (BsonTimeCodec) EncodeValue(ctx bsoncodec.EncodeContext, vw bsonrw.ValueWriter, v reflect.Value) error {
	t, ok := v.Interface().(meta.Time)
	if !ok {
		return stderrors.New("invalid time")
	}
	tim := t.Truncate(time.Millisecond)
	return vw.WriteDateTime(int64(primitive.NewDateTimeFromTime(tim)))
}

type BsonQuantityCodec struct{}

// DecodeValue implements bsoncodec.ValueDecoder.
func (b BsonQuantityCodec) DecodeValue(ctx bsoncodec.DecodeContext, vr bsonrw.ValueReader, v reflect.Value) error {
	str, err := vr.ReadString()
	if err != nil {
		return err
	}
	quantity, err := resource.ParseQuantity(str)
	if err != nil {
		return err
	}
	v.Set(reflect.ValueOf(quantity))
	return nil
}

// EncodeValue implements bsoncodec.ValueEncoder.
func (BsonQuantityCodec) EncodeValue(ctx bsoncodec.EncodeContext, vw bsonrw.ValueWriter, v reflect.Value) error {
	quantity, ok := v.Interface().(resource.Quantity)
	if !ok {
		return stderrors.New("invalid quantity")
	}
	vw.WriteString(quantity.String())
	return nil
}

func NewMongoStorage(ctx context.Context, schema *store.Schema, options *MongoDBOptions) (*MongoStorage, error) {
	schema, err := schema.Clone()
	if err != nil {
		return nil, err
	}
	mongoBsonOptions := &mongooptions.BSONOptions{UseJSONStructTags: true, OmitZeroStruct: true}
	mongoBsonRegistry := GlobalBsonRegistry

	db, err := NewMongoDB(ctx, mongoBsonOptions, mongoBsonRegistry, options)
	if err != nil {
		return nil, err
	}
	core := &MongoStorageCore{
		db:             db,
		schema:         schema,
		bsonRegistry:   mongoBsonRegistry,
		bsonOptions:    mongoBsonOptions,
		collections:    map[string]*mongo.Collection{},
		collectionLock: sync.RWMutex{},
		logger: log.FromContext(ctx).
			WithName("mongo-storage"),
	}
	if err := core.initCollections(ctx); err != nil {
		return nil, err
	}
	storage := &MongoStorage{core: core}
	return storage, nil
}

func NewMongoDB(ctx context.Context,
	bsonOptions *mongooptions.BSONOptions,
	bsonRegistry *bsoncodec.Registry,
	opts *MongoDBOptions,
) (*mongo.Database, error) {
	connectopt := mongooptions.Client().
		SetConnectTimeout(time.Second * 10).
		SetHosts([]string{opts.Address}).
		SetBSONOptions(bsonOptions).
		SetRegistry(bsonRegistry).
		SetReplicaSet(opts.ReplicaSet)

	if opts.Username != "" && opts.Password != "" {
		connectopt.SetAuth(mongooptions.Credential{Username: opts.Username, Password: opts.Password})
	}
	if opts.Direct {
		connectopt.SetDirect(true)
	}
	cli, err := mongo.Connect(ctx, connectopt)
	if err != nil {
		return nil, errors.NewInternalError(err)
	}
	if err := cli.Ping(ctx, nil); err != nil {
		return nil, errors.NewInternalError(err)
	}
	return cli.Database(opts.Database), nil
}

type MongoStorageCore struct {
	schema         *store.Schema
	db             *mongo.Database
	bsonRegistry   *bsoncodec.Registry
	bsonOptions    *mongooptions.BSONOptions
	collections    map[string]*mongo.Collection
	collectionLock sync.RWMutex
	logger         log.Logger
}

func (m *MongoStorageCore) initCollections(ctx context.Context) error {
	for _, resource := range m.schema.Resources() {
		definition, err := m.schema.Resource(resource)
		if err != nil {
			return err
		}
		col := m.db.Collection(resource)
		m.collections[resource] = col
		indexes := make([]mongo.IndexModel, 0, len(definition.Indexes))
		for _, index := range definition.Indexes {
			indexOptions := mongooptions.Index().
				SetName(index.Name).
				SetUnique(index.Unique)
			if index.Nullable {
				indexOptions.SetPartialFilterExpression(PartialFilterExpression(index.Fields))
			}
			indexes = append(indexes, mongo.IndexModel{Keys: listToBsonD(index.Fields), Options: indexOptions})
		}
		m.logger.V(5).
			Info("init indexes", "collection", col.Name(), "indexes", indexes)
		if _, err := col.Indexes().
			CreateMany(ctx, indexes); err != nil {
			return err
		}
		// https://www.mongodb.com/docs/manual/reference/command/collMod
		cmd := bson.D{
			{Key: "collMod", Value: col.Name()},
			{Key: "changeStreamPreAndPostImages", Value: bson.M{"enabled": true}},
		}
		m.logger.V(5).
			Info("init collection", "collection", col.Name(), "cmd", cmd)
		if err := m.db.RunCommand(ctx, cmd).
			Err(); err != nil {
			return err
		}
	}
	return nil
}

func PartialFilterExpression(fields []string) bson.M {
	expressions := make([]bson.M, 0, len(fields))
	for _, field := range fields {
		expressions = append(expressions, bson.M{field: bson.M{"$exists": true}})
	}
	return bson.M{"$and": expressions}
}

type MongoStorage struct {
	core   *MongoStorageCore
	scopes []store.Scope
}

// Capabilities implements store.Store.
func (m *MongoStorage) Capabilities() store.Capabilities {
	return store.Capabilities{
		LabelSelector:    true,
		FieldSelector:    true,
		Search:           true,
		Sort:             true,
		SubScopes:        true,
		Projection:       true,
		OptimisticLock:   true,
		Watch:            true,
		Transaction:      true,
		SecondaryIndexes: true,
		UniqueIndexes:    true,
	}
}

func (m *MongoStorage) Ping(ctx context.Context) error {
	return m.core.db.Client().
		Ping(ctx, nil)
}

// Schema implements store.Store.
func (m *MongoStorage) Schema() *store.Schema {
	return m.core.schema.Snapshot()
}

func (m *MongoStorage) Database() *mongo.Database {
	return m.core.db
}

// Scope implements Storage.
func (m *MongoStorage) Scope(scopes ...store.Scope) store.Store {
	if len(scopes) == 0 {
		return m
	}
	return &MongoStorage{core: m.core, scopes: append(slices.Clone(m.scopes), scopes...)}
}

// Count implements Storage.
func (m *MongoStorage) Count(ctx context.Context, obj store.Object, opts ...store.CountOption) (int, error) {
	options := store.ApplyCountOptions(opts)
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return 0, err
	}
	var count int
	err := m.on(ctx, obj, options.IncludeSubScopes, func(ctx context.Context, col *mongo.Collection, filter bson.D) error {
		filter = ConditionsMatch(filter, options.LabelRequirements, options.FieldRequirements, "")
		m.core.logger.V(5).
			Info("count", "collection", col.Name(), "filter", filter)
		doccount, err := col.CountDocuments(ctx, filter)
		if err != nil {
			return WarpMongoError(err, col, obj)
		}
		count = int(doccount)
		return nil
	})
	return count, err
}

// Create implements Storage.
func (m *MongoStorage) Create(ctx context.Context, into store.Object, opts ...store.CreateOption) error {
	_ = store.ApplyCreateOptions(opts)
	return m.on(ctx, into, false, func(ctx context.Context, col *mongo.Collection, filter bson.D) error {
		store.PrepareObjectForCreate(into, col.Name(), slices.Clone(m.scopes))
		into.SetResourceVersion(1)
		data, err := m.objectDocument(into)
		if err != nil {
			return err
		}
		// before creation
		m.core.logger.V(5).
			Info("create", "collection", col.Name(), "data", data)
		result, err := col.InsertOne(ctx, data)
		if err != nil {
			return WarpMongoError(err, col, into)
		}
		switch id := result.InsertedID.(type) {
		case string:
		case primitive.ObjectID:
			_ = id
		}
		return nil
	})
}

func (m *MongoStorage) objectDocument(into any) (bson.D, error) {
	if uns, ok := into.(*store.Unstructured); ok {
		into = uns.Object
	}
	var encoded bytes.Buffer
	writer, err := bsonrw.NewBSONValueWriter(&encoded)
	if err != nil {
		return nil, errors.NewInternalError(err)
	}
	encoder, err := bson.NewEncoder(writer)
	if err != nil {
		return nil, errors.NewInternalError(err)
	}
	encoder.SetRegistry(m.core.bsonRegistry)
	encoder.UseJSONStructTags()
	encoder.OmitZeroStruct()
	if err := encoder.Encode(into); err != nil {
		return nil, errors.NewBadRequest(fmt.Sprintf("invalid object: %v", err))
	}
	var data bson.D
	decoder, err := bson.NewDecoder(bsonrw.NewBSONDocumentReader(encoded.Bytes()))
	if err != nil {
		return nil, errors.NewInternalError(err)
	}
	decoder.SetRegistry(m.core.bsonRegistry)
	if err := decoder.Decode(&data); err != nil {
		return nil, errors.NewInternalError(err)
	}
	data = slices.DeleteFunc(data, func(field bson.E) bool { return field.Key == "resource" })
	// Scope fields remain a lossy projection for declared indexes and callers;
	// the ordered scopes array is the only identity used by Store queries.
loop:
	for _, projection := range SetScopesFields(nil, m.scopes) {
		for i, d := range data {
			if d.Key == projection.Key {
				// if field is not empty and not equal to condition value, return error
				if d.Value != nil && !reflect.ValueOf(d.Value).
					IsZero() && !reflect.DeepEqual(d.Value, projection.Value) {
					return nil, errors.NewBadRequest(fmt.Sprintf("conflict condition and object field: %s", projection.Key))
				}
				// set field to condition value
				data[i].Value = projection.Value
				continue loop
			}
		}
		// set new field
		data = append(data, projection)
	}
	return data, nil
}

// Delete implements Storage.
func (m *MongoStorage) Delete(ctx context.Context, obj store.Object, opts ...store.DeleteOption) error {
	id := obj.GetID()
	if id == "" {
		return errors.NewBadRequest("id is required")
	}
	options := store.ApplyDeleteOptions(opts)
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	return m.on(ctx, obj, false, func(ctx context.Context, col *mongo.Collection, filter bson.D) error {
		filter = append(filter, bson.E{Key: "id", Value: id})
		for {
			current := store.NewObject(obj)
			found := col.FindOne(ctx, filter)
			if err := found.Decode(current); err != nil {
				return WarpMongoError(err, col, obj)
			}
			current.SetResource(col.Name())
			current.SetScopes(slices.Clone(m.scopes))
			if err := store.ValidatePreconditions(current, options.Preconditions); err != nil {
				return err
			}
			if err := store.ValidateDeleteRequirements(current, options.LabelRequirements, options.FieldRequirements); err != nil {
				return err
			}
			policy := store.DeletePropagationBackground
			if options.PropagationPolicy != nil {
				policy = *options.PropagationPolicy
			}
			versionFilter := append(slices.Clone(filter), bson.E{Key: "resourceVersion", Value: current.GetResourceVersion()})
			if store.PrepareObjectForDelete(current, policy) {
				result, err := col.DeleteOne(ctx, versionFilter)
				if err != nil {
					return WarpMongoError(err, col, obj)
				}
				if result.DeletedCount == 0 {
					continue
				}
				return store.CopyObject(current, obj)
			}
			current.SetResourceVersion(current.GetResourceVersion() + 1)
			data, err := m.objectDocument(current)
			if err != nil {
				return err
			}
			result, err := col.ReplaceOne(ctx, versionFilter, data)
			if err != nil {
				return WarpMongoError(err, col, obj)
			}
			if result.MatchedCount == 0 {
				continue
			}
			return store.CopyObject(current, obj)
		}
	})
}

// DeleteAllOf implements Storage.
func (m *MongoStorage) DeleteBatch(ctx context.Context, obj store.ObjectList, opts ...store.DeleteBatchOption) error {
	options := store.ApplyDeleteBatchOptions(opts)
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	return m.on(ctx, obj, false, func(ctx context.Context, col *mongo.Collection, filter bson.D) error {
		filter = ConditionsMatch(filter, options.LabelRequirements, options.FieldRequirements, "")
		m.core.logger.V(5).
			Info("delete all", "collection", col.Name(), "filter", filter)
		if _, err := col.DeleteMany(ctx, filter); err != nil {
			return WarpMongoError(err, col, nil)
		}
		return nil
	})
}

// Get implements Storage.
func (m *MongoStorage) Get(ctx context.Context, id string, obj store.Object, opts ...store.GetOption) error {
	if id == "" {
		return errors.NewBadRequest("id is required")
	}
	options := store.ApplyGetOptions(opts)
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	return m.on(ctx, obj, false, func(ctx context.Context, col *mongo.Collection, filter bson.D) error {
		filter = append(filter, bson.E{Key: "id", Value: id})
		filter = ConditionsMatch(filter, options.LabelRequirements, options.FieldRequirements, "")
		findopt := mongooptions.FindOne()
		if len(options.Fields) != 0 {
			project := bson.M{}
			for _, field := range options.Fields {
				project[field] = 1
			}
			if options.Preconditions != nil {
				project["id"] = 1
				project["uid"] = 1
				project["resourceVersion"] = 1
			}
			findopt = findopt.SetProjection(project)
		}
		m.core.logger.V(5).
			Info("get", "collection", col.Name(), "filter", filter)
		if err := col.FindOne(ctx, filter, findopt).
			Decode(obj); err != nil {
			return WarpMongoError(err, col, obj)
		}
		obj.SetResource(col.Name())
		obj.SetScopes(slices.Clone(m.scopes))
		return store.ValidatePreconditions(obj, options.Preconditions)
	})
}

// Update implements Storage.
func (m *MongoStorage) Update(ctx context.Context, obj store.Object, opts ...store.UpdateOption) error {
	options := store.ApplyUpdateOptions(opts)
	return m.replace(ctx, obj, options.LabelRequirements, options.FieldRequirements, false, obj.GetResourceVersion(), func(current, desired store.Object) error {
		return store.CopyObject(obj, desired)
	})
}

// Patch implements Storage.
func (m *MongoStorage) Patch(ctx context.Context, obj store.Object, patch store.Patch, opts ...store.PatchOption) error {
	return m.patch(ctx, obj, patch, false, store.ApplyPatchOptions(opts))
}

// patch applies one patch to the latest stored document with a single update.
// The patch carries no precondition on the version the caller observed: only its
// own resourceVersion, its tests and the caller's preconditions decide whether it
// applies, so concurrent writers do not conflict.
func (m *MongoStorage) patch(ctx context.Context, obj store.Object, patch store.Patch, status bool, options store.PatchOptions) error {
	if obj.GetID() == "" {
		return errors.NewBadRequest("id is required")
	}
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	decoded, err := store.DecodePatch(patch, obj)
	if err != nil {
		return err
	}
	update, emptiesFinalizers, err := m.patchUpdate(decoded, status)
	if err != nil {
		return err
	}
	conditions, err := patchConditions(decoded, options.Preconditions)
	if err != nil {
		return err
	}
	return m.on(ctx, obj, false, func(ctx context.Context, col *mongo.Collection, scope bson.D) error {
		selection := append(scope, bson.E{Key: "id", Value: obj.GetID()})
		selection = ConditionsMatch(selection, options.LabelRequirements, options.FieldRequirements, "")
		updated := store.NewObject(obj)
		filter := bson.D{{Key: "$and", Value: bson.A{selection, conditions}}}
		result := col.FindOneAndUpdate(ctx, filter, update,
			mongooptions.FindOneAndUpdate().SetReturnDocument(mongooptions.After))
		if err := result.Decode(updated); err != nil {
			if !stderrors.Is(err, mongo.ErrNoDocuments) {
				return WarpMongoError(err, col, obj)
			}
			// A patch with no condition of its own can only miss because the
			// object is gone or unselected, which needs no re-read.
			if len(conditions) == 0 {
				return WarpMongoError(err, col, obj)
			}
			// A miss is a missing or unselected object, a stale condition, or
			// both. Re-reading without the patch's own conditions tells them apart.
			current := store.NewObject(obj)
			if err := col.FindOne(ctx, selection).Decode(current); err != nil {
				return WarpMongoError(err, col, obj)
			}
			current.SetResource(col.Name())
			if err := store.ValidatePreconditions(current, options.Preconditions); err != nil {
				return err
			}
			if decoded.Version != 0 && current.GetResourceVersion() != decoded.Version {
				return errors.NewConflict(col.Name(), obj.GetID(), fmt.Errorf("resourceVersion %d does not match", decoded.Version))
			}
			if len(decoded.Tests) > 0 {
				return errors.NewCustomError(http.StatusUnprocessableEntity, errors.StatusReasonInvalid,
					fmt.Sprintf("invalid %s %q: patch test failed", col.Name(), obj.GetID()))
			}
			return errors.NewConflict(col.Name(), obj.GetID(), fmt.Errorf("object changed during the write"))
		}
		updated.SetResource(col.Name())
		updated.SetScopes(m.scopes)
		if err := store.CopyObject(updated, obj); err != nil {
			return errors.NewInternalError(err)
		}
		if emptiesFinalizers {
			// The patch emptied the last finalizer of a terminating object, so
			// delete it at the version this write just produced.
			conditions := append(scope,
				bson.E{Key: "id", Value: obj.GetID()},
				bson.E{Key: "resourceVersion", Value: updated.GetResourceVersion()},
				bson.E{Key: "deletionTimestamp", Value: bson.M{"$ne": nil}},
			)
			if _, err := col.DeleteOne(ctx, conditions); err != nil {
				return WarpMongoError(err, col, obj)
			}
		}
		return nil
	})
}

// patchUpdate renders one patch as an update pipeline. Writing the generation and
// the version inside the pipeline keeps the whole patch in one statement.
func (m *MongoStorage) patchUpdate(decoded store.DecodedPatch, status bool) (bson.A, bool, error) {
	owned := map[string]struct{}{"generation": {}}
	for _, name := range store.ServerOwnedUpdateFields() {
		owned[name] = struct{}{}
	}
	scopeFields := make(map[string]struct{}, len(m.scopes))
	for _, scope := range m.scopes {
		scopeFields[store.ScopeResourceToFieldName(scope.Resource)] = struct{}{}
	}
	patched := make(map[string]any, len(decoded.Fields))
	for _, field := range decoded.Fields {
		patched[field.Column] = nil
	}
	business := store.ObjectBusinessFields(patched)
	set := bson.D{}
	unset := bson.A{}
	changed := bson.A{}
	emptiesFinalizers := false
	for _, field := range decoded.Fields {
		if _, skip := owned[field.Column]; skip {
			continue
		}
		if _, skip := scopeFields[field.Column]; skip {
			continue
		}
		// A normal write keeps the stored status; the status subresource writes
		// nothing else.
		if status != (field.Column == "status") {
			continue
		}
		path, err := documentPath(field.Column, field.Path)
		if err != nil {
			return nil, false, err
		}
		// A copy or move reads another field of the same document; every other
		// value is a literal.
		value := any(bson.D{{Key: "$literal", Value: field.Value}})
		if len(field.FromPath) > 0 {
			from, err := documentPath(field.FromPath[0], field.FromPath[1:])
			if err != nil {
				return nil, false, err
			}
			value = "$" + from
		}
		if field.Remove {
			unset = append(unset, path)
		} else {
			set = append(set, bson.E{Key: path, Value: value})
		}
		if _, ok := business[field.Column]; ok {
			// Removing a value changes the object only when it was present.
			previous := value
			if field.Remove {
				previous = nil
			}
			changed = append(changed, bson.D{{Key: "$ne", Value: bson.A{"$" + path, previous}}})
		}
		if field.Column == "finalizers" && field.EmptiesValue() {
			emptiesFinalizers = true
		}
	}
	if len(changed) > 0 {
		set = append(set, bson.E{Key: "generation", Value: bson.D{{Key: "$add", Value: bson.A{
			"$generation",
			bson.D{{Key: "$cond", Value: bson.A{bson.D{{Key: "$or", Value: changed}}, 1, 0}}},
		}}}})
	}
	set = append(set, bson.E{Key: "resourceVersion", Value: bson.D{{Key: "$add", Value: bson.A{"$resourceVersion", 1}}}})
	pipeline := bson.A{bson.D{{Key: "$set", Value: set}}}
	if len(unset) > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$unset", Value: unset}})
	}
	return pipeline, emptiesFinalizers, nil
}

// documentPath renders a field path. A segment that carries a dot or starts with
// a dollar cannot be addressed as a document path.
func documentPath(column string, path []string) (string, error) {
	segments := append([]string{column}, path...)
	for _, segment := range segments {
		if strings.Contains(segment, ".") || strings.HasPrefix(segment, "$") {
			return "", errors.NewUnsupported(fmt.Sprintf("field %q cannot be addressed as a document path", segment))
		}
	}
	return strings.Join(segments, "."), nil
}

// patchConditions renders the conditions a patch write must satisfy: the
// caller's preconditions, the patch's own version and its tests.
func patchConditions(decoded store.DecodedPatch, preconditions *store.Preconditions) (bson.D, error) {
	conditions := bson.D{}
	if preconditions != nil {
		if preconditions.UID != nil {
			conditions = append(conditions, bson.E{Key: "uid", Value: *preconditions.UID})
		}
		if preconditions.ResourceVersion != nil {
			conditions = append(conditions, bson.E{Key: "resourceVersion", Value: *preconditions.ResourceVersion})
		}
	}
	if decoded.Version != 0 {
		conditions = append(conditions, bson.E{Key: "resourceVersion", Value: decoded.Version})
	}
	if len(decoded.Tests) > 0 {
		tests := make(bson.A, 0, len(decoded.Tests))
		for _, test := range decoded.Tests {
			path, err := documentPath(test.Path[0], test.Path[1:])
			if err != nil {
				return nil, err
			}
			tests = append(tests, bson.D{{Key: "$eq", Value: bson.A{"$" + path, bson.D{{Key: "$literal", Value: test.Value}}}}})
		}
		conditions = append(conditions, bson.E{Key: "$expr", Value: bson.D{{Key: "$and", Value: tests}}})
	}
	return conditions, nil
}

// PatchBatch implements store.Store.
func (m *MongoStorage) PatchBatch(ctx context.Context, obj store.ObjectList, patch store.PatchBatch, opts ...store.PatchBatchOption) error {
	options := store.ApplyPatchBatchOptions(opts)
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	return m.on(ctx, obj, false, func(ctx context.Context, col *mongo.Collection, filter bson.D) error {
		filter = ConditionsMatch(filter, options.LabelRequirements, options.FieldRequirements, "")
		update, err := convertBatchPatch(patch, []string{"creator", "creationTimestamp", "resourceVersion", "status", "generation", "scopes"}, nil)
		if err != nil {
			return err
		}
		update = append(update, bson.E{Key: "$inc", Value: bson.D{{Key: "resourceVersion", Value: 1}}})
		m.core.logger.V(5).
			Info("batch patch", "collection", col.Name(), "filter", filter, "update", update)
		if _, err := col.UpdateMany(ctx, filter, update); err != nil {
			return ConvetMongoListError(err, col)
		}
		return nil
	})
}

// List implements Storage.
func (m *MongoStorage) List(ctx context.Context, list store.ObjectList, opts ...store.ListOption) error {
	options := store.ApplyListOptions(opts)
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	if options.Continue != nil {
		return errors.NewUnsupported("MongoDB store does not support cursor pagination")
	}

	// if projection is empty, set projection from list object
	// currently, we don't use this feature
	return m.on(ctx, list, options.IncludeSubScopes, func(ctx context.Context, col *mongo.Collection, filter bson.D) error {
		pipeline := listPipeline(filter, nil, options, options.Fields, nil)
		m.core.logger.V(5).
			Info("list", "collection", col.Name(), "pipeline", pipeline)
		cur, err := col.Aggregate(ctx, pipeline)
		if err != nil {
			return ConvetMongoListError(err, col)
		}
		defer cur.Close(ctx)
		if cur.Next(ctx) {
			if err := cur.Decode(list); err != nil {
				return ConvetMongoListError(err, col)
			}
		}
		// set empty list if no items instead of nil
		setEmptyItemsIfNil(list)
		total := 0
		if decodedTotal := list.GetTotal(); decodedTotal != nil {
			total = *decodedTotal
		}
		store.SetWindowListMetadata(list, total)

		// set resource for each item
		store.ForEachItem(list, func(item store.Object) error {
			item.SetResource(col.Name())
			return nil
		})
		return nil
	})
}

func setEmptyItemsIfNil(list store.ObjectList) {
	items, err := store.GetItemsPtr(list)
	if err != nil {
		return
	}
	v := reflect.ValueOf(items)
	v = reflect.Indirect(v)
	if v.Len() == 0 {
		v.Set(reflect.MakeSlice(v.Type(), 0, 0))
	}
}

func listPipeline(match bson.D, pre []any, opts store.ListOptions, fields []string, post []any) bson.A {
	if search := opts.Search; search != "" {
		if len(opts.SearchFields) > 0 {
			match = append(match, searchStage(opts.SearchFields, search))
		} else {
			match = append(match, searchStage([]string{"id", "name"}, search))
		}
	}
	match = ConditionsMatch(match, opts.LabelRequirements, opts.FieldRequirements, "")
	pipeline := bson.A{}
	// pre conditions
	pipeline = append(pipeline, pre...)
	// filter
	if len(match) > 0 {
		pipeline = append(pipeline, bson.M{"$match": match})
	}
	// project
	if len(fields) > 0 {
		project := bson.M{"scopes": 1}
		for _, field := range fields {
			project[field] = 1
		}
		pipeline = append(pipeline, bson.M{"$project": project})
	}
	// sort
	pipeline = append(pipeline, sortstage(opts.Sort))
	// items
	itemspipeline := bson.A{}
	// window
	if offset := max(opts.Offset, 0); offset > 0 {
		itemspipeline = append(itemspipeline, bson.M{"$skip": offset})
	}
	if opts.Limit > 0 {
		itemspipeline = append(itemspipeline, bson.M{"$limit": opts.Limit})
	}
	// post conditions
	pipeline = append(pipeline, post...)
	// facet
	pipeline = append(pipeline, bson.M{
		"$facet": bson.M{
			"items": itemspipeline,
			"total": bson.A{bson.M{"$count": "count"}},
		},
	})
	// final project
	pipeline = append(pipeline, bson.M{
		"$project": bson.M{
			"items": 1,
			"total": bson.M{"$arrayElemAt": bson.A{"$total.count", 0}},
		},
	})
	return pipeline
}

func sortstage(sort string) bson.M {
	sorts := bson.D{}
	for _, s := range store.ParseSorts(sort) {
		if s.Field == "time" {
			s.Field = "creationTimestamp"
		}
		direction := 1
		if s.Direction == meta.SortDirectionDesc {
			direction = -1
		}
		sorts = append(sorts, bson.E{Key: s.Field, Value: direction})
	}
	if len(sorts) > 0 {
		return bson.M{"$sort": sorts}
	} else {
		return bson.M{"$sort": bson.D{{Key: "creationTimestamp", Value: -1}}}
	}
}

func scopesmatch(scopes []store.Scope, includeSubScopes bool) bson.D {
	if !includeSubScopes && len(scopes) == 0 {
		return bson.D{{Key: "scopes", Value: bson.D{{Key: "$in", Value: bson.A{nil, bson.A{}}}}}}
	}
	match := make(bson.D, 0, 3*len(scopes)+1)
	if !includeSubScopes {
		match = append(match, bson.E{Key: "scopes", Value: bson.D{{Key: "$size", Value: len(scopes)}}})
	}
	for index, scope := range scopes {
		prefix := "scopes." + strconv.Itoa(index)
		match = append(match,
			bson.E{Key: prefix, Value: bson.D{{Key: "$exists", Value: true}}},
			bson.E{Key: prefix + ".resource", Value: scopeFieldMatch(scope.Resource)},
			bson.E{Key: prefix + ".name", Value: scopeFieldMatch(scope.Name)},
		)
	}
	return match
}

func scopeFieldMatch(value string) any {
	if value == "" {
		// Scope's JSON omitempty makes a missing field the empty string.
		return bson.D{{Key: "$in", Value: bson.A{nil, ""}}}
	}
	return value
}

// ConditionsMatch appends label and field requirements for the selected document prefix.
func ConditionsMatch(match bson.D, labels store.Requirements, fields store.Requirements, prefix string) bson.D {
	if len(fields) != 0 {
		match = append(match, bson.E{Key: "$and", Value: mongoFieldRequirements(fields, prefix)})
	}
	if len(labels) != 0 {
		match = append(match, bson.E{Key: "$expr", Value: mongoLabelRequirements(labels, "$"+prefix+"labels")})
	}
	return match
}

func mongoFieldRequirements(requirements store.Requirements, prefix string) bson.A {
	expressions := make(bson.A, 0, len(requirements))
	for _, requirement := range requirements {
		expressions = append(expressions, mongoFieldRequirement(requirement, prefix))
	}
	return expressions
}

func mongoFieldRequirement(requirement selector.Requirement, prefix string) bson.D {
	if (requirement.Operator == selector.In || requirement.Operator == selector.NotIn) && len(requirement.Values) == 0 {
		return bson.D{{Key: "$expr", Value: false}}
	}
	key := prefix + requirement.Key
	switch requirement.Operator {
	case selector.None:
		return bson.D{{Key: "$expr", Value: false}}
	case selector.All:
		return bson.D{}
	case selector.And:
		if len(requirement.Requirements) == 0 {
			return bson.D{}
		}
		return bson.D{{Key: "$and", Value: mongoFieldRequirements(requirement.Requirements, prefix)}}
	case selector.Or:
		if len(requirement.Requirements) == 0 {
			return bson.D{{Key: "$expr", Value: false}}
		}
		return bson.D{{Key: "$or", Value: mongoFieldRequirements(requirement.Requirements, prefix)}}
	case selector.Not:
		return bson.D{{Key: "$nor", Value: bson.A{mongoFieldRequirement(requirement.Requirements[0], prefix)}}}
	case selector.Equals, selector.DoubleEquals:
		return bson.D{{Key: key, Value: requirement.Values[0]}}
	case selector.NotEquals:
		return bson.D{{Key: key, Value: bson.M{"$ne": requirement.Values[0]}}}
	case selector.In:
		return bson.D{{Key: key, Value: bson.M{"$in": requirement.Values}}}
	case selector.NotIn:
		return bson.D{{Key: key, Value: bson.M{"$nin": requirement.Values}}}
	case selector.Exists:
		return bson.D{{Key: key, Value: bson.M{"$exists": true}}}
	case selector.DoesNotExist:
		return bson.D{{Key: key, Value: bson.M{"$exists": false}}}
	case selector.GreaterThan:
		return bson.D{{Key: key, Value: bson.M{"$gt": requirement.Values[0]}}}
	case selector.LessThan:
		return bson.D{{Key: key, Value: bson.M{"$lt": requirement.Values[0]}}}
	case selector.GreaterThanOrEqual:
		return bson.D{{Key: key, Value: bson.M{"$gte": requirement.Values[0]}}}
	case selector.LessThanOrEqual:
		return bson.D{{Key: key, Value: bson.M{"$lte": requirement.Values[0]}}}
	case selector.Contains:
		return bson.D{{Key: key, Value: bson.M{"$all": requirement.Values}}}
	case selector.Like:
		return bson.D{searchStage(strings.Split(key, ","), store.AnyToString(requirement.Values[0]))}
	default:
		return bson.D{{Key: "$expr", Value: false}}
	}
}

func mongoLabelRequirements(requirements store.Requirements, input string) any {
	if len(requirements) == 0 {
		return true
	}
	expressions := make(bson.A, 0, len(requirements))
	for _, requirement := range requirements {
		expressions = append(expressions, mongoLabelRequirement(requirement, input))
	}
	return bson.D{{Key: "$and", Value: expressions}}
}

func mongoLabelRequirement(requirement selector.Requirement, input string) any {
	if (requirement.Operator == selector.In || requirement.Operator == selector.NotIn) && len(requirement.Values) == 0 {
		return false
	}
	switch requirement.Operator {
	case selector.None:
		return false
	case selector.All:
		return true
	case selector.And:
		return mongoLabelRequirements(requirement.Requirements, input)
	case selector.Or:
		if len(requirement.Requirements) == 0 {
			return false
		}
		expressions := make(bson.A, 0, len(requirement.Requirements))
		for _, child := range requirement.Requirements {
			expressions = append(expressions, mongoLabelRequirement(child, input))
		}
		return bson.D{{Key: "$or", Value: expressions}}
	case selector.Not:
		return bson.D{{Key: "$not", Value: bson.A{mongoLabelRequirement(requirement.Requirements[0], input)}}}
	}

	value := bson.D{{Key: "$ifNull", Value: bson.A{
		bson.D{{Key: "$getField", Value: bson.D{
			{Key: "field", Value: bson.D{{Key: "$literal", Value: requirement.Key}}},
			{Key: "input", Value: input},
		}}},
		nil,
	}}}
	comparison := bson.A{value, nil}
	switch requirement.Operator {
	case selector.Equals, selector.DoubleEquals:
		comparison[1] = bson.D{{Key: "$literal", Value: requirement.Values[0]}}
		return bson.D{{Key: "$eq", Value: comparison}}
	case selector.NotEquals:
		comparison[1] = bson.D{{Key: "$literal", Value: requirement.Values[0]}}
		return bson.D{{Key: "$ne", Value: comparison}}
	case selector.In, selector.NotIn:
		expression := bson.D{{Key: "$in", Value: bson.A{
			value,
			bson.D{{Key: "$literal", Value: bson.A(requirement.Values)}},
		}}}
		if requirement.Operator == selector.NotIn {
			return bson.D{{Key: "$not", Value: bson.A{expression}}}
		}
		return expression
	case selector.Exists:
		return bson.D{{Key: "$ne", Value: comparison}}
	case selector.DoesNotExist:
		return bson.D{{Key: "$eq", Value: comparison}}
	case selector.GreaterThan:
		comparison[1] = bson.D{{Key: "$literal", Value: requirement.Values[0]}}
		return bson.D{{Key: "$gt", Value: comparison}}
	case selector.LessThan:
		comparison[1] = bson.D{{Key: "$literal", Value: requirement.Values[0]}}
		return bson.D{{Key: "$lt", Value: comparison}}
	case selector.GreaterThanOrEqual:
		comparison[1] = bson.D{{Key: "$literal", Value: requirement.Values[0]}}
		return bson.D{{Key: "$gte", Value: comparison}}
	case selector.LessThanOrEqual:
		comparison[1] = bson.D{{Key: "$literal", Value: requirement.Values[0]}}
		return bson.D{{Key: "$lte", Value: comparison}}
	default:
		return false
	}
}

func searchStage(keys []string, search string) bson.E {
	if search == "" || len(keys) == 0 {
		return bson.E{}
	}
	// do not support regex, it raise error when search is invalid regex
	search = escapeRegex(search)
	if len(keys) > 1 {
		fields := bson.A{}
		for _, key := range keys {
			fields = append(fields, bson.D{{Key: key, Value: bson.M{"$regex": search, "$options": "i"}}})
		}
		return bson.E{Key: "$or", Value: fields}
	}
	// https://www.mongodb.com/docs/manual/reference/operator/query/regex/
	return bson.E{Key: keys[0], Value: bson.M{"$regex": search, "$options": "i"}}
}

var escapeRegexReplacer = strings.NewReplacer(
	".", "\\.",
	"*", "\\*",
	"+", "\\+",
	"?", "\\?",
	"^", "\\^",
	"$", "\\$",
	"{", "\\{",
	"}", "\\}",
	"(", "\\(",
	")", "\\)",
	"|", "\\|",
	"[", "\\[",
	"]", "\\]",
	"\\", "\\\\")

func escapeRegex(input string) string {
	return escapeRegexReplacer.Replace(input)
}

func convertBatchPatch(patch store.PatchBatch, excludes []string, includes []string) (bson.D, error) {
	data := patch.Data()
	patchtype := patch.Type()
	return patchToMongoUpdate(patchtype, data, excludes, includes)
}

func patchToMongoUpdate(patchtype store.PatchType, data []byte, excludes []string, includes []string) (bson.D, error) {
	switch patchtype {
	case store.PatchTypeMergePatch:
		patchmap := map[string]any{}
		if err := json.Unmarshal(data, &patchmap); err != nil {
			return nil, errors.NewBadRequest("invalid patch data")
		}
		update, err := MergePatchToBsonUpdate(patchmap, excludes, includes)
		if err != nil {
			return nil, errors.NewBadRequest(fmt.Sprintf("invalid patch data: %v", err))
		}
		return update, nil
	case store.PatchTypeJSONPatch:
		patchlist := []map[string]any{}
		if err := json.Unmarshal(data, &patchlist); err != nil {
			return nil, errors.NewBadRequest("invalid patch data")
		}
		update, err := JsonPatchToBsonUpdate(patchlist, excludes, includes)
		if err != nil {
			return nil, errors.NewBadRequest(fmt.Sprintf("invalid patch data: %v", err))
		}
		return update, nil
	default:
		return nil, errors.NewBadRequest(fmt.Sprintf("invalid patch type: %s", patchtype))
	}
}

func WarpMongoError(err error, col *mongo.Collection, obj store.Object) error {
	if err != nil {
		if obj == nil {
			obj = &store.ObjectMeta{}
		}
		return ConvetMongoError(err, col, obj.GetID())
	}
	return nil
}

func ConvetMongoListError(err error, col *mongo.Collection) error {
	mongoerr, ok := err.(mongo.CommandError)
	if ok {
		switch mongoerr.Code {
		case 51091:
			return errors.NewBadRequest("invalid search expression")
		}
	}
	return errors.NewInternalError(err)
}

func ConvetMongoError(err error, col *mongo.Collection, name string) error {
	if stderrors.Is(err, mongo.ErrNoDocuments) {
		return errors.NewNotFound(col.Name(), name)
	}
	if mongo.IsDuplicateKeyError(err) {
		return errors.NewAlreadyExists(col.Name(), name)
	}
	return errors.NewInternalError(err)
}

// Status implements Storage.
func (m *MongoStorage) Status() store.StatusStorage {
	return &MongoStorageStatus{MongoStorage: m}
}

type MongoStorageStatus struct {
	*MongoStorage
}

// Patch implements StatusStorage.
func (m *MongoStorageStatus) Patch(ctx context.Context, obj store.Object, patch store.Patch, opts ...store.PatchOption) error {
	return m.patch(ctx, obj, patch, true, store.ApplyPatchOptions(opts))
}

// Update implements StatusStorage.
func (m *MongoStorageStatus) Update(ctx context.Context, obj store.Object, opts ...store.UpdateOption) error {
	options := store.ApplyUpdateOptions(opts)
	return m.replace(ctx, obj, options.LabelRequirements, options.FieldRequirements, true, obj.GetResourceVersion(), func(current, desired store.Object) error {
		return store.CopyObject(obj, desired)
	})
}

func (m *MongoStorage) replace(
	ctx context.Context,
	obj store.Object,
	labelRequirements store.Requirements,
	fieldRequirements store.Requirements,
	status bool,
	requestedVersion int64,
	change func(current, desired store.Object) error,
) error {
	if obj.GetID() == "" {
		return errors.NewBadRequest("id is required")
	}
	if err := validateSelectorRequirements(labelRequirements, fieldRequirements); err != nil {
		return err
	}
	return m.on(ctx, obj, false, func(ctx context.Context, col *mongo.Collection, filter bson.D) error {
		filter = append(filter, bson.E{Key: "id", Value: obj.GetID()})
		filter = ConditionsMatch(filter, labelRequirements, fieldRequirements, "")
		current := store.NewObject(obj)
		found := col.FindOne(ctx, filter)
		if err := found.Decode(current); err != nil {
			return WarpMongoError(err, col, obj)
		}
		current.SetResource(col.Name())
		current.SetScopes(slices.Clone(m.scopes))
		if requestedVersion != 0 && requestedVersion != current.GetResourceVersion() {
			return errors.NewConflict(col.Name(), obj.GetID(), fmt.Errorf("resourceVersion %d does not match", requestedVersion))
		}
		desired := store.NewObject(obj)
		if err := change(current, desired); err != nil {
			return err
		}
		completeDeletion, err := store.PrepareObjectForUpdate(current, desired, status)
		if err != nil {
			return errors.NewInternalError(err)
		}
		versionFilter := append(slices.Clone(filter), bson.E{Key: "resourceVersion", Value: current.GetResourceVersion()})
		if completeDeletion {
			result, err := col.DeleteOne(ctx, versionFilter)
			if err != nil {
				return WarpMongoError(err, col, obj)
			}
			if result.DeletedCount == 0 {
				return errors.NewConflict(col.Name(), obj.GetID(), fmt.Errorf("object changed during update"))
			}
			return store.CopyObject(desired, obj)
		}
		desired.SetResourceVersion(current.GetResourceVersion() + 1)
		data, err := m.objectDocument(desired)
		if err != nil {
			return err
		}
		result, err := col.ReplaceOne(ctx, versionFilter, data)
		if err != nil {
			return WarpMongoError(err, col, obj)
		}
		if result.MatchedCount == 0 {
			return errors.NewConflict(col.Name(), obj.GetID(), fmt.Errorf("object changed during update"))
		}
		return store.CopyObject(desired, obj)
	})
}

func validateSelectorRequirements(labelRequirements, fieldRequirements store.Requirements) error {
	if err := store.ValidateSelectorRequirements(labelRequirements, fieldRequirements); err != nil {
		return errors.NewBadRequest(err.Error())
	}
	if requirementOperatorExists(labelRequirements, selector.Contains, selector.Like) ||
		requirementOperatorExists(fieldRequirements, selector.Contains, selector.Like) {
		return errors.NewUnsupported("MongoDB store does not support Contains or Like requirements")
	}
	return nil
}

func requirementOperatorExists(requirements store.Requirements, operators ...selector.Operator) bool {
	for _, requirement := range requirements {
		if slices.Contains(operators, requirement.Operator) || requirementOperatorExists(requirement.Requirements, operators...) {
			return true
		}
	}
	return false
}

func (m *MongoStorage) on(ctx context.Context, into any, includeSubScopes bool, fn func(ctx context.Context, col *mongo.Collection, filter bson.D) error) error {
	if into == nil {
		return errors.NewBadRequest("object is nil")
	}
	colname, err := m.getCollectionName(into)
	if err != nil {
		return err
	}

	m.core.collectionLock.RLock()
	collection, ok := m.core.collections[colname]
	m.core.collectionLock.RUnlock()
	if !ok {
		m.core.collectionLock.Lock()
		defer m.core.collectionLock.Unlock()
		if collection, ok = m.core.collections[colname]; !ok {
			collection = m.core.db.Collection(colname)
			m.core.collections[colname] = collection
		}
	}
	filter := scopesmatch(m.scopes, includeSubScopes)
	return fn(ctx, collection, filter)
}

func (m *MongoStorage) getCollectionName(into any) (string, error) {
	return store.GetResource(into)
}

func listToBsonD(list []string) bson.D {
	d := bson.D{}
	for _, s := range list {
		d = append(d, bson.E{Key: s, Value: 1})
	}
	return d
}

var jsonPatchUnescape = strings.NewReplacer("~1", "/", "~0", "~")

func matchFieldFunc(list []string, s string) bool {
	return slices.ContainsFunc(list, func(l string) bool {
		return l == s || strings.HasPrefix(s+".", l)
	})
}

func JsonPatchToBsonUpdate(patches []map[string]any, excludes []string, includes []string) (bson.D, error) {
	update := bson.D{}

	for _, patch := range patches {
		pathval, opval, value := patch["path"], patch["op"], patch["value"]
		path, ok := pathval.(string)
		if !ok || path == "" {
			return nil, fmt.Errorf("invalid patch path: %v", pathval)
		}
		op, ok := opval.(string)
		if !ok || op == "" {
			return nil, fmt.Errorf("invalid patch op: %v", opval)
		}
		if path[0] == '/' {
			path = path[1:]
		}
		bsonpath := jsonPatchUnescape.Replace(path)

		// filter fields
		if matchFieldFunc(excludes, bsonpath) ||
			len(includes) > 0 && !matchFieldFunc(includes, bsonpath) {
			continue
		}
		switch op {
		case "add":
			lastElement := path
			if index := strings.LastIndex(path, "/"); index > 0 {
				lastElement = path[index+1:]
			}
			if _, err := strconv.ParseInt(lastElement, 10, 64); err != nil {
				if lastElement == "-" {
					update = append(update, bson.E{Key: "$push", Value: bson.D{{Key: bsonpath, Value: value}}})
				} else {
					update = append(update, bson.E{Key: "$set", Value: bson.D{{Key: bsonpath, Value: value}}})
				}
				continue
			}
			update = append(update, bson.E{Key: "$push", Value: bson.D{{Key: bsonpath, Value: bson.D{{Key: "$each", Value: bson.A{value}}}}}})
		case "remove":
			update = append(update, bson.E{Key: "$unset", Value: bson.D{{Key: bsonpath, Value: ""}}})
		case "replace":
			update = append(update, bson.E{Key: "$set", Value: bson.D{{Key: bsonpath, Value: value}}})
		default:
			return nil, fmt.Errorf("invalid patch op: %v", op)
		}
	}
	return update, nil
}

// MergePatchToBsonUpdate converts a map to a bson.D for use in a merge patch.
// e.g. {"a": 1, "b": {"c": 2}} -> bson.D{{"a", 1}, {"b.c", 2}}
func MergePatchToBsonUpdate(data map[string]any, excludes []string, includes []string) (bson.D, error) {
	d, err := FlattenData(data, -1, excludes, includes)
	if err != nil {
		return nil, err
	}
	return bson.D{{Key: "$set", Value: d}}, nil
}

func FlattenData(data any, depth int, excludes []string, includes []string) (bson.D, error) {
	into := bson.D{}
	err := libreflect.FlattenStruct("", depth, reflect.ValueOf(data), func(name string, v reflect.Value) error {
		name = strings.TrimPrefix(name, ".")
		if matchFieldFunc(excludes, name) || len(includes) > 0 && !matchFieldFunc(includes, name) {
			return nil
		}
		// case of any type but nil value
		if !v.IsValid() {
			into = append(into, bson.E{Key: name})
		} else {
			into = append(into, bson.E{Key: name, Value: v.Interface()})
		}
		return nil
	})
	return into, err
}

// SetScopesFields sets the scope's as fields in the data map.
// exaple:
//
//	if data = {"foo": "bar"} create/update under scopes [{"resource": "tenants", "name": "default"}]
//	the final data will be:
//	{"foo": "bar", "tenant": "default"}
func SetScopesFields(data bson.D, scopes []store.Scope) bson.D {
	for _, scope := range scopes {
		field := store.ScopeResourceToFieldName(scope.Resource)
		if field == "" {
			continue
		}
		data = setOrReplaceField(data, field, scope.Name)
	}
	return data
}

func setOrReplaceField(data bson.D, key string, value any) bson.D {
	for i, d := range data {
		if d.Key == key {
			data[i].Value = value
			return data
		}
	}
	return append(data, bson.E{Key: key, Value: value})
}
