package mongo

import (
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestPartialFilterExpression(t *testing.T) {
	want := bson.M{"$and": []bson.M{
		{"email": bson.M{"$exists": true}},
		{"tenant": bson.M{"$exists": true}},
	}}
	if got := PartialFilterExpression([]string{"email", "tenant"}); !reflect.DeepEqual(got, want) {
		t.Fatalf("PartialFilterExpression() = %#v, want %#v", got, want)
	}
}
