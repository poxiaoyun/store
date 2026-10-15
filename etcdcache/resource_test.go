package etcdcache

import (
	"context"
	stderrors "errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestResourceRegistryInitializesResourceOnce(t *testing.T) {
	var calls atomic.Int32
	fieldsSeen := make(chan []string, 1)
	factory := func(_ context.Context, resource schema.GroupResource, indexFields []string) (*resourceDB, error) {
		calls.Add(1)
		fieldsSeen <- append([]string(nil), indexFields...)
		return &resourceDB{resource: resource}, nil
	}
	registry := newResourceRegistry(
		context.Background(),
		map[string][]string{"widgets": {"email", "name"}},
		factory,
		nil,
	)
	t.Cleanup(registry.Close)

	const goroutines = 32
	results := make(chan *resourceDB, goroutines)
	errors := make(chan error, goroutines)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			db, err := registry.get("widgets")
			results <- db
			errors <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errors)

	for err := range errors {
		if err != nil {
			t.Fatalf("get resource: %v", err)
		}
	}
	var first *resourceDB
	for db := range results {
		if first == nil {
			first = db
			continue
		}
		if db != first {
			t.Fatal("concurrent callers received different resource databases")
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("factory calls = %d, want 1", got)
	}
	if got := <-fieldsSeen; !reflect.DeepEqual(got, []string{"email", "name"}) {
		t.Fatalf("index fields = %v", got)
	}
}

func TestResourceRegistryReturnsFactoryErrorsAndRetries(t *testing.T) {
	wantErr := stderrors.New("open resource")
	var calls atomic.Int32
	registry := newResourceRegistry(
		context.Background(),
		nil,
		func(_ context.Context, resource schema.GroupResource, _ []string) (*resourceDB, error) {
			if calls.Add(1) == 1 {
				return nil, wantErr
			}
			return &resourceDB{resource: resource}, nil
		},
		nil,
	)
	t.Cleanup(registry.Close)

	if _, err := registry.get("widgets"); !stderrors.Is(err, wantErr) {
		t.Fatalf("first get error = %v, want %v", err, wantErr)
	}
	if _, err := registry.get("widgets"); err != nil {
		t.Fatalf("second get error = %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("factory calls = %d, want 2", got)
	}
}

func TestResourceRegistryClosesResourcesWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var resourceCloses atomic.Int32
	var ownerCloses atomic.Int32
	registry := newResourceRegistry(
		ctx,
		nil,
		func(_ context.Context, resource schema.GroupResource, _ []string) (*resourceDB, error) {
			return &resourceDB{
				resource: resource,
				destroy: func() {
					resourceCloses.Add(1)
				},
			}, nil
		},
		func() {
			ownerCloses.Add(1)
		},
	)

	if _, err := registry.get("widgets"); err != nil {
		t.Fatalf("get resource: %v", err)
	}
	cancel()
	select {
	case <-registry.Done():
	case <-time.After(time.Second):
		t.Fatal("registry did not close after its context ended")
	}

	registry.Close()
	if got := resourceCloses.Load(); got != 1 {
		t.Fatalf("resource closes = %d, want 1", got)
	}
	if got := ownerCloses.Load(); got != 1 {
		t.Fatalf("owner closes = %d, want 1", got)
	}
	if _, err := registry.get("widgets"); !stderrors.Is(err, errResourceRegistryClosed) {
		t.Fatalf("get after close error = %v, want %v", err, errResourceRegistryClosed)
	}
}

func TestResourceRegistryCloseCancelsResourceContext(t *testing.T) {
	var resourceContext context.Context
	registry := newResourceRegistry(
		context.Background(),
		nil,
		func(ctx context.Context, resource schema.GroupResource, _ []string) (*resourceDB, error) {
			resourceContext = ctx
			return &resourceDB{resource: resource}, nil
		},
		nil,
	)

	if _, err := registry.get("widgets"); err != nil {
		t.Fatalf("get resource: %v", err)
	}
	registry.Close()
	select {
	case <-resourceContext.Done():
		if !stderrors.Is(resourceContext.Err(), context.Canceled) {
			t.Fatalf("resource context error = %v, want context canceled", resourceContext.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("resource context was not canceled by Close")
	}
}

func TestResourceRegistryCloseWaitsForResourceConstruction(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var resourceCloses atomic.Int32
	var ownerCloses atomic.Int32
	registry := newResourceRegistry(
		context.Background(),
		nil,
		func(_ context.Context, resource schema.GroupResource, _ []string) (*resourceDB, error) {
			close(started)
			<-release
			return &resourceDB{
				resource: resource,
				destroy:  func() { resourceCloses.Add(1) },
			}, nil
		},
		func() { ownerCloses.Add(1) },
	)

	getDone := make(chan error, 1)
	go func() {
		_, err := registry.get("widgets")
		getDone <- err
	}()
	<-started
	closeDone := make(chan struct{})
	go func() {
		registry.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
		t.Fatal("Close returned while resource construction was still running")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	<-closeDone
	if err := <-getDone; !stderrors.Is(err, errResourceRegistryClosed) {
		t.Fatalf("get error = %v, want %v", err, errResourceRegistryClosed)
	}
	if got := resourceCloses.Load(); got != 1 {
		t.Fatalf("resource closes = %d, want 1", got)
	}
	if got := ownerCloses.Load(); got != 1 {
		t.Fatalf("owner closes = %d, want 1", got)
	}
}
