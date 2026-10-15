package store

import (
	"reflect"
	"testing"

	"time"
	"xiaoshiai.cn/common/meta"
	"xiaoshiai.cn/common/selector"
)

func TestApplyListOptionsComposesInOrder(t *testing.T) {
	first := selector.RequirementEqual("state", "active")
	second := selector.RequirementEqual("tenant", "tenant-1")

	options := ApplyListOptions([]ListOption{
		WithOffset(1),
		WithFieldRequirements(first),
		WithOffset(2),
		WithFieldRequirements(second),
	})
	if options.Offset != 2 {
		t.Fatalf("offset = %d, want 2", options.Offset)
	}
	if want := (Requirements{first, second}); !reflect.DeepEqual(options.FieldRequirements, want) {
		t.Fatalf("FieldRequirements = %#v, want %#v", options.FieldRequirements, want)
	}
}

func TestApplyListOptionsEmptyReturnsZeroValue(t *testing.T) {
	options := ApplyListOptions(nil)
	if !reflect.DeepEqual(options, ListOptions{}) {
		t.Fatalf("ApplyListOptions(nil) = %#v", options)
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
			options: []ListOption{WithContinue("", 25)},
			want:    ListOptions{Limit: 25, Continue: meta.Ptr("")},
		},
		{
			name:    "later cursor batch",
			options: []ListOption{WithContinue("next", 25)},
			want:    ListOptions{Limit: 25, Continue: meta.Ptr("next")},
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

func TestApplyListOptionsAppliesPaginationInOrder(t *testing.T) {
	options := ApplyListOptions([]ListOption{
		WithLimit(10),
		WithOffset(2),
		WithLimit(25),
	})
	want := ListOptions{Offset: 2, Limit: 25}
	if !reflect.DeepEqual(options, want) {
		t.Fatalf("ApplyListOptions() = %#v, want %#v", options, want)
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
	if !ApplyCreateOptions([]CreateOption{dryRun}).DryRun || !ApplyPatchOptions([]PatchOption{dryRun}).DryRun {
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

var benchmarkListOptions ListOptions

type legacyListOption func(*ListOptions)

//go:noinline
func applyLegacyListOptions(options []legacyListOption) ListOptions {
	resolved := ListOptions{}
	for _, option := range options {
		option(&resolved)
	}
	return resolved
}

//go:noinline
func legacyResolvedListOptions(options ListOptions) []legacyListOption {
	return []legacyListOption{func(target *ListOptions) {
		*target = options
	}}
}

// BenchmarkResolvedListOptions isolates option construction and application;
// selector parsing and backend query work are deliberately outside its scope.
func BenchmarkResolvedListOptions(b *testing.B) {
	public := ListOptions{
		Offset: 20,
		Limit:  20,
		Search: "worker",
		Sort:   "creationTimestamp-",
	}

	b.Run("captured-functional-option", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			benchmarkListOptions = applyLegacyListOptions(legacyResolvedListOptions(public))
		}
	})

	b.Run("concrete-options", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			options := []ListOption{
				metaListOption{
					Offset:   public.Offset,
					Limit:    public.Limit,
					Search:   public.Search,
					Sort:     public.Sort,
					Continue: public.Continue,
				},
			}
			benchmarkListOptions = ApplyListOptions(options)
		}
	})
}

func BenchmarkApplyListOptionsEmpty(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		benchmarkListOptions = ApplyListOptions(nil)
	}
}
