package mongo

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"xiaoshiai.cn/store"
)

var _ store.Sequence = (*Sequence)(nil)

// Sequence persists sequences in a MongoDB collection. The namespace is stored as
// the document _id, whose built-in unique index makes allocation atomic.
type Sequence struct {
	collection *mongo.Collection
}

// NewSequence creates an allocator backed by collection.
func NewSequence(collection *mongo.Collection) *Sequence {
	return &Sequence{collection: collection}
}

// Next implements store.Sequence.
func (a *Sequence) Next(ctx context.Context, namespace string) (uint64, error) {
	result := struct {
		Value uint64 `bson:"value"`
	}{}
	err := a.collection.FindOneAndUpdate(
		ctx,
		bson.M{"_id": namespace},
		bson.M{"$inc": bson.M{"value": 1}},
		options.FindOneAndUpdate().
			SetUpsert(true).
			SetReturnDocument(options.After),
	).
		Decode(&result)
	return result.Value, err
}
