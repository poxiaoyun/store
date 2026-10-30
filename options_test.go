package store

import (
	"reflect"
	"testing"
	"time"

	"xiaoshiai.cn/store/selector"
)

func TestApplyListOptionsComposesInOrder(t *testing.T) {
	first := selector.RequirementEqual("state", "active")
	second := selector.RequirementEqual("tenant", "tenant-1")

	options := ApplyListOptions([]ListOption{
		WithOffset(1),
		WithLimit(10),
		WithFieldRequirements(first),
		WithOffset(2),
		WithLimit(25),
		WithFieldRequirements(second),
	})
	if options.Offset != 2 || options.Limit != 25 {
		t.Fatalf("pagination = offset %d limit %d, want offset 2 limit 25", options.Offset, options.Limit)
	}
	if want := (Requirements{first, second}); !reflect.DeepEqual(options.FieldRequirements, want) {
		t.Fatalf("FieldRequirements = %#v, want %#v", options.FieldRequirements, want)
	}
}

func TestApplyListOptionsAcceptsPaginationForms(t *testing.T) {
	tests := []struct {
		name    string
		options []ListOption
		want    ListOptions
	}{
		{name: "unbounded", want: ListOptions{}},
		{
			name:    "window",
			options: []ListOption{WithOffset(2), WithLimit(25)},
			want:    ListOptions{Offset: 2, Limit: 25},
		},
		{
			name:    "first cursor batch",
			options: []ListOption{WithContinueLimit("", 25)},
			want:    ListOptions{Limit: 25, Continue: ""},
		},
		{
			name:    "later cursor batch",
			options: []ListOption{WithContinueLimit("next", 25)},
			want:    ListOptions{Limit: 25, Continue: "next"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := ApplyListOptions(test.options)
			if !reflect.DeepEqual(options, test.want) {
				t.Fatalf("ApplyListOptions() = %#v, want %#v", options, test.want)
			}
		})
	}
}

func TestSharedOptionsApplyAcrossOperations(t *testing.T) {
	resourceVersion := WithResourceVersion(7)
	get := ApplyGetOptions([]GetOption{resourceVersion})
	list := ApplyListOptions([]ListOption{resourceVersion, WithSubScopes()})
	watch := ApplyWatchOptions([]WatchOption{resourceVersion, WithSubScopes()})
	deleted := ApplyDeleteOptions([]DeleteOption{resourceVersion})

	if get.ResourceVersion == nil || *get.ResourceVersion != 7 {
		t.Fatalf("Get ResourceVersion = %#v", get.ResourceVersion)
	}
	if list.ResourceVersion == nil || *list.ResourceVersion != 7 || !list.IncludeSubScopes {
		t.Fatalf("List options = %#v", list)
	}
	if watch.ResourceVersion == nil || *watch.ResourceVersion != 7 || !watch.IncludeSubScopes {
		t.Fatalf("Watch options = %#v", watch)
	}
	if deleted.Preconditions == nil || deleted.Preconditions.ResourceVersion == nil || *deleted.Preconditions.ResourceVersion != 7 {
		t.Fatalf("Delete options = %#v", deleted)
	}

	ttl := WithTTL(time.Minute)
	create := ApplyCreateOptions([]CreateOption{ttl})
	update := ApplyUpdateOptions([]UpdateOption{ttl})
	if create.TTL != time.Minute || update.TTL != time.Minute {
		t.Fatalf("TTL create=%s update=%s", create.TTL, update.TTL)
	}

	dryRun := WithDryRun()
	if !ApplyCreateOptions([]CreateOption{dryRun}).
		DryRun || !ApplyPatchOptions([]PatchOption{dryRun}).
		DryRun {
		t.Fatal("WithDryRun did not enable every supported operation")
	}
}

func TestPreconditionsOptionOverwritesOnlyPresentValues(t *testing.T) {
	resourceVersion := int64(7)
	options := ApplyDeleteOptions([]DeleteOption{
		WithUID("uid-1"),
		WithPreconditions(Preconditions{ResourceVersion: &resourceVersion}),
		WithUID("uid-2"),
	})

	if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != "uid-2" {
		t.Fatalf("UID precondition = %#v", options.Preconditions)
	}
	if options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion != 7 {
		t.Fatalf("ResourceVersion precondition = %#v", options.Preconditions)
	}
}
