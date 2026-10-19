package mongo_test

import (
	"testing"

	testmongodb "xiaoshiai.cn/common/testkit/mongodb"
	"xiaoshiai.cn/store/mongo"
)

func TestSequenceIntegration(t *testing.T) {
	uri := testmongodb.RequireURI(t)
	database := testmongodb.RequireDatabase(t, uri)
	allocator := mongo.NewSequence(database.Collection("sequences"))

	first, err := allocator.Next(t.Context(), "issues/repository")
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	second, err := allocator.Next(t.Context(), "issues/repository")
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	other, err := allocator.Next(t.Context(), "issues/other")
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if first != 1 || second != 2 || other != 1 {
		t.Fatalf("sequences = (%d, %d, %d), want (1, 2, 1)", first, second, other)
	}
}
