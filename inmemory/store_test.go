package inmemory_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
	"k8s.io/apiserver/pkg/storage/etcd3/testserver"
	commonerrors "xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/meta"
	testmongo "xiaoshiai.cn/common/testkit/mongodb"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/etcd"
	"xiaoshiai.cn/store/inmemory"
	"xiaoshiai.cn/store/mongo"
	"xiaoshiai.cn/store/selector"
)

type rule struct {
	store.ObjectMeta `json:",inline"`
	Service          string    `json:"service"`
	Subject          string    `json:"subject"`
	Start            meta.Time `json:"start"`
	End              meta.Time `json:"end"`
	Values           []string  `json:"values"`
}

func schema(t testing.TB) *store.Schema {
	t.Helper()
	s := store.NewSchema()
	if err := s.Register(&rule{}, store.ResourceSchema{Indexes: []store.Index{{Name: "selection", Fields: []string{"service", "subject", "start", "creationTimestamp", "id"}}}}); err != nil {
		t.Fatal(err)
	}
	return s
}

func startCache(t testing.TB, source store.Store, resources ...store.Object) *inmemory.CacheStore {
	t.Helper()
	c, err := inmemory.NewCacheStore(source, inmemory.CacheStoreOptions{Resources: resources})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- c.Run(t.Context()) }()
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	})
	return c
}

func waitSync(t testing.TB, cache *inmemory.CacheStore) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	if err := cache.WaitSync(ctx); err != nil {
		t.Fatal(err)
	}
}

func until(t testing.TB, check func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if check() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("cache did not converge")
		case <-ticker.C:
		}
	}
}

func list(t testing.TB, s store.Store, opts ...store.ListOption) []rule {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	result := &store.List[rule]{}
	if err := s.List(ctx, result, append(opts, store.WithResourceVersion(0))...); err != nil {
		t.Fatal(err)
	}
	return result.Items
}

func TestCacheBackends(t *testing.T) {
	for _, backend := range []string{"memory", "etcd", "mongo"} {
		t.Run(backend, func(t *testing.T) {
			definitions := schema(t)
			var source store.Store
			switch backend {
			case "memory":
				memory, err := inmemory.New(definitions)
				if err != nil {
					t.Fatal(err)
				}
				source = memory
			case "etcd":
				client := testserver.RunEtcd(t, nil)
				remote, err := etcd.NewEtcdStoreFromClient(client, definitions, "/cache/"+uuid.NewString())
				if err != nil {
					t.Fatal(err)
				}
				source = remote
			case "mongo":
				uri := testmongo.RequireURI(t)
				db := testmongo.RequireDatabase(t, uri)
				connection, err := connstring.ParseAndValidate(uri)
				if err != nil {
					t.Fatal(err)
				}
				remote, err := mongo.NewMongoStorage(t.Context(), definitions, &mongo.MongoDBOptions{Address: connection.Hosts[0], Database: db.Name(), ReplicaSet: connection.ReplicaSet, Direct: connection.DirectConnection, Username: connection.Username, Password: connection.Password})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := remote.Close(); err != nil {
						t.Error(err)
					}
				})
				source = remote
			}
			first, second := startCache(t, source, &rule{}), startCache(t, source, &rule{})
			waitSync(t, first)
			waitSync(t, second)
			// The empty initial snapshot is a usable collection.
			if got := list(t, first); len(got) != 0 {
				t.Fatal(got)
			}
			at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			base := &rule{ObjectMeta: store.ObjectMeta{ID: "base", Labels: map[string]string{"region": "east"}}, Service: "svc", Subject: "meter", Start: meta.Time{Time: at.Add(-time.Hour)}, End: meta.Time{Time: at.Add(24 * time.Hour)}, Values: []string{"original"}}
			promo := &rule{ObjectMeta: store.ObjectMeta{ID: "promo", Labels: map[string]string{"region": "east"}}, Service: "svc", Subject: "meter", Start: meta.Time{Time: at}, End: meta.Time{Time: at.Add(time.Hour)}}
			future := &rule{ObjectMeta: store.ObjectMeta{ID: "future", Labels: map[string]string{"region": "east"}}, Service: "svc", Subject: "meter", Start: meta.Time{Time: at.Add(2 * time.Hour)}, End: meta.Time{Time: at.Add(3 * time.Hour)}}
			for _, value := range []*rule{base, promo, future} {
				if err := first.Create(t.Context(), value); err != nil {
					t.Fatal(err)
				}
			}
			for _, c := range []*inmemory.CacheStore{first, second} {
				until(t, func() bool { return len(list(t, c)) == 3 })
			}
			for _, test := range []struct {
				at time.Time
				id string
			}{{at.Add(-time.Nanosecond), "base"}, {at, "promo"}, {at.Add(time.Hour), "base"}, {at.Add(2 * time.Hour), "future"}, {at.Add(3 * time.Hour), "base"}} {
				found := list(t, second, store.WithFieldRequirements(selector.RequirementEqual("service", "svc"), selector.RequirementEqual("subject", "meter"), selector.RequirementLessThanOrEqual("start", test.at), selector.RequirementGreaterThan("end", test.at)), store.WithLabelRequirementsFromSet(map[string]string{"region": "east"}), store.WithSort("start-,creationTimestamp-,id-"), store.WithLimit(1))
				if len(found) != 1 || found[0].ID != test.id {
					t.Fatalf("at %s: %+v", test.at, found)
				}
			}
			paged := &store.List[rule]{}
			pageIDs := []string{}
			for {
				if err := second.List(t.Context(), paged,
					store.WithResourceVersion(0),
					store.WithFieldRequirements(selector.RequirementEqual("service", "svc"), selector.RequirementEqual("subject", "meter"), selector.RequirementLessThanOrEqual("start", at.Add(2*time.Hour))),
					store.WithSort("start-,creationTimestamp-,id-"), store.WithContinueLimit(paged.Continue, 1)); err != nil {
					t.Fatal(err)
				}
				for _, object := range paged.Items {
					pageIDs = append(pageIDs, object.ID)
				}
				if paged.Continue == "" {
					break
				}
			}
			if !reflect.DeepEqual(pageIDs, []string{"future", "promo", "base"}) {
				t.Fatal(pageIDs)
			}
			got := &rule{}
			if err := second.Get(t.Context(), base.ID, got, store.WithResourceVersion(0)); err != nil {
				t.Fatal(err)
			}
			got.Labels["region"] = "changed"
			got.Values[0] = "changed"
			if err := second.Get(t.Context(), base.ID, got, store.WithResourceVersion(0)); err != nil {
				t.Fatal(err)
			}
			if got.Labels["region"] != "east" || got.Values[0] != "original" || got.ResourceVersion != base.ResourceVersion || got.UID != base.UID || !got.CreationTimestamp.Equal(&base.CreationTimestamp) {
				t.Fatal(got)
			}
			// A live update must release the previous index entry.
			if err := source.Patch(t.Context(), base, store.FieldPatch{Operations: []store.FieldOperation{{Op: store.FieldOpSet, Path: "subject", Value: "other"}}}); err != nil {
				t.Fatal(err)
			}
			until(t, func() bool {
				return len(list(t, second, store.WithFieldRequirements(selector.RequirementEqual("subject", "other")))) == 1
			})
			if err := first.Delete(t.Context(), promo); err != nil {
				t.Fatal(err)
			}
			for _, c := range []*inmemory.CacheStore{first, second} {
				until(t, func() bool { return len(list(t, c)) == 2 })
			}
			broken := &interruptedSource{Store: source, cut: make(chan struct{}), reconnect: make(chan struct{}), resume: make(chan struct{})}
			recovering := startCache(t, broken, &rule{})
			waitSync(t, recovering)
			if len(list(t, recovering)) != 2 {
				t.Fatal("initial recovery fixture")
			}
			close(broken.cut)
			select {
			case <-broken.reconnect:
			case <-time.After(15 * time.Second):
				t.Fatal("no reconnect")
			}
			if err := source.Delete(t.Context(), future); err != nil {
				t.Fatal(err)
			}
			waiting, cancel := context.WithCancel(t.Context())
			cancel()
			if err := recovering.WaitSync(waiting); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			close(broken.resume)
			waitSync(t, recovering)
			if got := list(t, recovering); len(got) != 1 || got[0].ID != base.ID {
				t.Fatalf("rebuild: %+v", got)
			}
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}
			if err := source.Get(t.Context(), base.ID, &rule{}); err != nil {
				t.Fatalf("borrowed source closed: %v", err)
			}
			finalized := &rule{ObjectMeta: store.ObjectMeta{ID: "finalized", Finalizers: []string{"hold"}}, Service: "svc"}
			if err := second.Create(t.Context(), finalized); err != nil {
				t.Fatal(err)
			}
			until(t, func() bool {
				return len(list(t, second, store.WithFieldRequirements(selector.RequirementEqual("id", finalized.ID)))) == 1
			})
			if err := second.Delete(t.Context(), finalized); err != nil {
				t.Fatal(err)
			}
			until(t, func() bool {
				objects := list(t, second, store.WithFieldRequirements(selector.RequirementEqual("id", finalized.ID)))
				if len(objects) != 1 {
					t.Fatal("finalized object disappeared before physical deletion")
				}
				return objects[0].DeletionTimestamp != nil
			})
			if err := second.Get(t.Context(), finalized.ID, finalized); err != nil {
				t.Fatal(err)
			}
			finalized.Finalizers = nil
			if err := second.Update(t.Context(), finalized); err != nil {
				t.Fatal(err)
			}
			until(t, func() bool {
				return len(list(t, second, store.WithFieldRequirements(selector.RequirementEqual("id", finalized.ID)))) == 0
			})
			recreated := &rule{ObjectMeta: store.ObjectMeta{ID: finalized.ID}, Service: "replacement"}
			if err := second.Create(t.Context(), recreated); err != nil {
				t.Fatal(err)
			}
			until(t, func() bool {
				objects := list(t, second, store.WithFieldRequirements(selector.RequirementEqual("id", finalized.ID)))
				return len(objects) == 1 && objects[0].UID == recreated.UID
			})
			if recreated.UID == finalized.UID {
				t.Fatal("recreated object retained the deleted incarnation's UID")
			}
			if objects := list(t, second, store.WithFieldRequirements(selector.RequirementEqual("service", "svc"), selector.RequirementEqual("id", finalized.ID))); len(objects) != 0 {
				t.Fatalf("recreated object retained an old index entry: %+v", objects)
			}
		})
	}
}

type scriptSource struct {
	store.Store
	watches chan *scriptWatch
	opened  chan struct{}
}

type scriptWatch struct {
	events  chan store.WatchEvent
	stopped atomic.Bool
}

func (w *scriptWatch) Events() <-chan store.WatchEvent { return w.events }
func (w *scriptWatch) Stop()                           { w.stopped.Store(true) }
func (s *scriptSource) Watch(ctx context.Context, _ store.ObjectList, opts ...store.WatchOption) (store.Watcher, error) {
	if !store.ApplyWatchOptions(opts).SendInitialEvents {
		return nil, commonerrors.NewUnsupported("expected initial Watch")
	}
	select {
	case s.opened <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case w := <-s.watches:
		return w, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestSnapshotAndRebuild(t *testing.T) {
	base, err := inmemory.New(schema(t))
	if err != nil {
		t.Fatal(err)
	}
	upstream := &scriptSource{Store: base, watches: make(chan *scriptWatch, 2), opened: make(chan struct{}, 2)}
	first := &scriptWatch{events: make(chan store.WatchEvent, 10)}
	upstream.watches <- first
	c := startCache(t, upstream, &rule{})
	<-upstream.opened
	obj := func(id, subject string) *rule {
		return &rule{ObjectMeta: store.ObjectMeta{ID: id, UID: id, Resource: "rules", ResourceVersion: 5}, Service: "svc", Subject: subject}
	}
	first.events <- store.WatchEvent{Type: store.WatchEventCreate, Object: obj("a", "old")}
	first.events <- store.WatchEvent{Type: store.WatchEventCreate, Object: obj("b", "gone")}
	first.events <- store.WatchEvent{Type: store.WatchEventUpdate, Object: obj("a", "new")}
	first.events <- store.WatchEvent{Type: store.WatchEventDelete, Object: obj("b", "gone")}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	err = c.WaitSync(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitSync returned before Bookmark: %v", err)
	}
	readctx, stop := context.WithTimeout(t.Context(), time.Second)
	if err := c.List(readctx, &store.List[rule]{}, store.WithResourceVersion(0)); !commonerrors.IsCode(err, 503) {
		t.Fatalf("unavailable cache read: %v", err)
	}
	stop()
	// Default management queries use the authoritative source even before sync.
	if err := c.List(t.Context(), &store.List[rule]{}); err != nil {
		t.Fatal(err)
	}
	first.events <- store.WatchEvent{Type: store.WatchEventBookmark, ResourceVersion: 50}
	waitSync(t, c)
	if got := list(t, c, store.WithFieldRequirements(selector.RequirementEqual("service", "svc"), selector.RequirementEqual("subject", "new"))); len(got) != 1 || got[0].UID != "a" || got[0].ResourceVersion != 5 {
		t.Fatal(got)
	}
	close(first.events)
	<-upstream.opened // Invalidation precedes the next Watch request.
	ctx, cancel = context.WithTimeout(t.Context(), 30*time.Millisecond)
	err = c.WaitSync(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitSync returned before rebuild: %v", err)
	}
	readctx, stop = context.WithTimeout(t.Context(), time.Second)
	if err := c.List(readctx, &store.List[rule]{}, store.WithResourceVersion(0)); !commonerrors.IsCode(err, 503) {
		t.Fatalf("unavailable cache read: %v", err)
	}
	stop()
	second := &scriptWatch{events: make(chan store.WatchEvent, 1)}
	upstream.watches <- second
	second.events <- store.WatchEvent{Type: store.WatchEventBookmark}
	waitSync(t, c)
	if got := list(t, c, store.WithFieldRequirements(selector.RequirementEqual("service", "svc"))); len(got) != 0 {
		t.Fatalf("deleted object survived rebuild: %+v", got)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(t.Context(), "a", &rule{}, store.WithResourceVersion(0)); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
}

// interruptedSource breaks a live watch and holds the next connection while
// the real backend changes. It exercises recovery through Store's public seam.
type interruptedSource struct {
	store.Store
	mu        sync.Mutex
	calls     int
	cut       chan struct{}
	reconnect chan struct{}
	resume    chan struct{}
}

func (s *interruptedSource) Watch(ctx context.Context, list store.ObjectList, opts ...store.WatchOption) (store.Watcher, error) {
	s.mu.Lock()
	call := s.calls
	s.calls++
	s.mu.Unlock()
	if call > 0 {
		close(s.reconnect)
		select {
		case <-s.resume:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	upstream, err := s.Store.Watch(ctx, list, opts...)
	if err != nil {
		return nil, err
	}
	if call > 0 {
		return upstream, nil
	}
	watcher := &forwardWatch{Watcher: upstream, events: make(chan store.WatchEvent)}
	go func() {
		defer close(watcher.events)
		defer upstream.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.cut:
				return
			case event, ok := <-upstream.Events():
				if !ok {
					return
				}
				select {
				case watcher.events <- event:
				case <-ctx.Done():
					return
				case <-s.cut:
					return
				}
			}
		}
	}()
	return watcher, nil
}

type forwardWatch struct {
	store.Watcher
	events chan store.WatchEvent
}

func (w *forwardWatch) Events() <-chan store.WatchEvent { return w.events }

func TestCoverageAndPagination(t *testing.T) {
	source, err := inmemory.New(schema(t))
	if err != nil {
		t.Fatal(err)
	}
	scoped := source.Scope(store.Scope{Resource: "tenant", Name: "a/b"})
	for index := range 1100 {
		region := "west"
		if index < 3 {
			region = "east"
		}
		object := &rule{ObjectMeta: store.ObjectMeta{ID: fmt.Sprintf("%04d", index), Labels: map[string]string{"region": region}}, Service: "svc", Subject: "meter"}
		if err := scoped.Create(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
	c := startCache(t, source, &rule{})
	waitSync(t, c)
	view := c.Scope(store.Scope{Resource: "tenant", Name: "a/b"})
	options := []store.ListOption{store.WithResourceVersion(0), store.WithFieldRequirements(selector.RequirementEqual("service", "svc"), selector.RequirementEqual("subject", "meter")), store.WithLabelRequirementsFromSet(map[string]string{"region": "east"}), store.WithSort("id-"), store.WithLimit(1)}
	result := &store.List[rule]{}
	ids := []string{}
	for {
		if err := view.List(t.Context(), result, append(options, store.WithContinueLimit(result.Continue, 1))...); err != nil {
			t.Fatal(err)
		}
		if len(result.Items) != 1 {
			t.Fatal(result)
		}
		ids = append(ids, result.Items[0].ID)
		if result.Continue == "" {
			break
		}
	}
	if !reflect.DeepEqual(ids, []string{"0002", "0001", "0000"}) {
		t.Fatal(ids)
	}
	if got := list(t, c); len(got) != 0 {
		t.Fatalf("scope leak: %d", len(got))
	}
	if got := list(t, c, store.WithSubScopes()); len(got) != 1100 {
		t.Fatal(len(got))
	}
}

func TestReadIntentAndClose(t *testing.T) {
	source, err := inmemory.New(schema(t))
	if err != nil {
		t.Fatal(err)
	}
	c, err := inmemory.NewCacheStore(source, inmemory.CacheStoreOptions{Resources: []store.Object{&rule{}}})
	if err != nil {
		t.Fatal(err)
	}
	object := &rule{ObjectMeta: store.ObjectMeta{ID: "authoritative"}}
	if err := c.Create(t.Context(), object); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(t.Context(), object.ID, &rule{}); err != nil {
		t.Fatal(err)
	}
	readctx, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	if err := c.Get(readctx, object.ID, &rule{}, store.WithResourceVersion(0)); !commonerrors.IsCode(err, 503) {
		t.Fatalf("unsynced cached Get: %v", err)
	}
	if err := c.Get(t.Context(), object.ID, &rule{}, store.WithResourceVersion(1)); !commonerrors.IsUnsupported(err) {
		t.Fatalf("positive freshness: %v", err)
	}
	if err := c.List(t.Context(), &store.List[rule]{}, store.WithResourceVersion(1)); !commonerrors.IsUnsupported(err) {
		t.Fatalf("positive freshness: %v", err)
	}
	if err := c.List(t.Context(), &store.List[store.Unstructured]{Resource: "uncached"}, store.WithResourceVersion(0)); !commonerrors.IsUnsupported(err) {
		t.Fatalf("unregistered resource: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- c.WaitSync(t.Context()) }()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("closed WaitSync: %v", err)
	}
	if err := source.Get(t.Context(), object.ID, &rule{}); err != nil {
		t.Fatalf("borrowed source: %v", err)
	}
}

func TestCloseDuringInitialSync(t *testing.T) {
	source, err := inmemory.New(schema(t))
	if err != nil {
		t.Fatal(err)
	}
	upstream := &scriptSource{Store: source, watches: make(chan *scriptWatch), opened: make(chan struct{})}
	c := startCache(t, upstream, &rule{})
	<-upstream.opened
	done := make(chan error, 1)
	go func() { done <- c.WaitSync(t.Context()) }()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("WaitSync after close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("close did not release WaitSync")
	}
}

func TestWaitSyncAllResources(t *testing.T) {
	definitions := schema(t)
	if err := definitions.Register(&item{}, store.ResourceSchema{}); err != nil {
		t.Fatal(err)
	}
	source, err := inmemory.New(definitions)
	if err != nil {
		t.Fatal(err)
	}
	upstream := &scriptSource{Store: source, watches: make(chan *scriptWatch, 2), opened: make(chan struct{}, 2)}
	first, second := &scriptWatch{events: make(chan store.WatchEvent, 1)}, &scriptWatch{events: make(chan store.WatchEvent, 1)}
	upstream.watches <- first
	upstream.watches <- second
	c := startCache(t, upstream, &rule{}, &item{})
	<-upstream.opened
	<-upstream.opened
	first.events <- store.WatchEvent{Type: store.WatchEventBookmark}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	err = c.WaitSync(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitSync accepted a partial resource set: %v", err)
	}
	second.events <- store.WatchEvent{Type: store.WatchEventBookmark}
	waitSync(t, c)
	for _, result := range []store.ObjectList{&store.List[rule]{}, &store.List[item]{}} {
		if err := c.List(t.Context(), result, store.WithResourceVersion(0)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRepeatedSnapshots(t *testing.T) {
	source, err := inmemory.New(schema(t))
	if err != nil {
		t.Fatal(err)
	}
	upstream := &scriptSource{Store: source, watches: make(chan *scriptWatch), opened: make(chan struct{}, 1)}
	cache := startCache(t, upstream, &rule{})
	watches := []*scriptWatch{}
	for cycle := range 4 {
		<-upstream.opened
		if err := cache.List(t.Context(), &store.List[rule]{}, store.WithResourceVersion(0)); !commonerrors.IsCode(err, 503) {
			t.Fatalf("cycle %d exposed incomplete state: %v", cycle, err)
		}
		watch := &scriptWatch{events: make(chan store.WatchEvent, 4)}
		watches = append(watches, watch)
		upstream.watches <- watch
		object := &rule{ObjectMeta: store.ObjectMeta{ID: "same", UID: fmt.Sprint(cycle), ResourceVersion: int64(cycle + 1)}, Service: "svc", Subject: fmt.Sprint(cycle)}
		switch cycle {
		case 0:
			// Losing the stream before Bookmark discards the entire staged snapshot.
			watch.events <- store.WatchEvent{Type: store.WatchEventCreate, Object: object}
			close(watch.events)
			continue
		case 1, 2:
			// The next incarnation has the same ID and a new UID.
			watch.events <- store.WatchEvent{Type: store.WatchEventCreate, Object: object}
		case 3:
			// An empty replacement must remove both objects and indexed values.
		}
		watch.events <- store.WatchEvent{Type: store.WatchEventBookmark, ResourceVersion: 1000}
		waitSync(t, cache)
		got := list(t, cache, store.WithFieldRequirements(selector.RequirementEqual("service", "svc")))
		if cycle < 3 {
			if len(got) != 1 || got[0].UID != object.UID || got[0].ResourceVersion != object.ResourceVersion {
				t.Fatalf("cycle %d: %+v", cycle, got)
			}
		} else if len(got) != 0 {
			t.Fatalf("empty replacement retained objects: %+v", got)
		}
		if old := list(t, cache, store.WithFieldRequirements(selector.RequirementEqual("service", "svc"), selector.RequirementEqual("subject", fmt.Sprint(cycle-1)))); len(old) != 0 {
			t.Fatalf("cycle %d retained old index entries: %+v", cycle, old)
		}
		if cycle == 1 {
			watch.events <- store.WatchEvent{Error: commonerrors.NewResourceExpired("rules", "checkpoint expired")}
		} else if cycle == 2 {
			close(watch.events)
		}
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	for i, watch := range watches {
		if !watch.stopped.Load() {
			t.Fatalf("watch %d was not released", i)
		}
	}
}

// resourceWatchSource controls each resource's Watch independently at the Store seam.
type resourceWatchSource struct {
	store.Store
	resources map[string]*scriptSource
}

func (s *resourceWatchSource) Watch(ctx context.Context, list store.ObjectList, options ...store.WatchOption) (store.Watcher, error) {
	name, err := s.Schema().ResourceName(list)
	if err != nil {
		return nil, err
	}
	return s.resources[name].Watch(ctx, list, options...)
}

func TestCacheResourceRecovery(t *testing.T) {
	definitions := schema(t)
	if err := definitions.Register(&item{}, store.ResourceSchema{}); err != nil {
		t.Fatal(err)
	}
	source, err := inmemory.New(definitions)
	if err != nil {
		t.Fatal(err)
	}
	rules := &scriptSource{Store: source, watches: make(chan *scriptWatch, 1), opened: make(chan struct{}, 1)}
	items := &scriptSource{Store: source, watches: make(chan *scriptWatch, 1), opened: make(chan struct{}, 1)}
	first := &scriptWatch{events: make(chan store.WatchEvent, 2)}
	peer := &scriptWatch{events: make(chan store.WatchEvent, 2)}
	rules.watches <- first
	items.watches <- peer
	upstream := &resourceWatchSource{Store: source, resources: map[string]*scriptSource{"rules": rules, "items": items}}
	cache := startCache(t, upstream, &rule{}, &item{})
	<-rules.opened
	<-items.opened
	first.events <- store.WatchEvent{Type: store.WatchEventBookmark}
	peer.events <- store.WatchEvent{Type: store.WatchEventCreate, Object: &item{ObjectMeta: store.ObjectMeta{ID: "peer", UID: "peer"}, Code: "retained"}}
	peer.events <- store.WatchEvent{Type: store.WatchEventBookmark}
	waitSync(t, cache)
	close(first.events)
	<-rules.opened
	if err := cache.Get(t.Context(), "missing", &rule{}, store.WithResourceVersion(0)); !commonerrors.IsCode(err, 503) {
		t.Fatalf("disconnected resource: %v", err)
	}
	retained := &item{}
	if err := cache.Get(t.Context(), "peer", retained, store.WithResourceVersion(0)); err != nil {
		t.Fatal(err)
	}
	if retained.Code != "retained" {
		t.Fatal(retained)
	}
	canceled, cancel := context.WithCancel(t.Context())
	canceledWait := make(chan error, 1)
	go func() { canceledWait <- cache.WaitSync(canceled) }()
	waiters := make(chan error, 4)
	for range 4 {
		go func() { waiters <- cache.WaitSync(t.Context()) }()
	}
	cancel()
	if err := <-canceledWait; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceling one waiter: %v", err)
	}
	second := &scriptWatch{events: make(chan store.WatchEvent, 1)}
	rules.watches <- second
	second.events <- store.WatchEvent{Type: store.WatchEventBookmark}
	for range 4 {
		if err := <-waiters; err != nil {
			t.Fatalf("other waiter interrupted: %v", err)
		}
	}
	if got := list(t, cache); len(got) != 0 {
		t.Fatal(got)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	for _, watch := range []*scriptWatch{first, peer, second} {
		if !watch.stopped.Load() {
			t.Fatal("Watch was not released")
		}
	}
}

func TestCacheReadsFollowIntent(t *testing.T) {
	source, err := inmemory.New(schema(t))
	if err != nil {
		t.Fatal(err)
	}
	original := &rule{
		ObjectMeta: store.ObjectMeta{ID: "rule", Labels: map[string]string{"region": "east"}},
		Service:    "svc", Subject: "old", Values: []string{"original"},
	}
	if err := source.Create(t.Context(), original); err != nil {
		t.Fatal(err)
	}
	upstream := &scriptSource{Store: source, watches: make(chan *scriptWatch, 1), opened: make(chan struct{}, 1)}
	watch := &scriptWatch{events: make(chan store.WatchEvent, 2)}
	upstream.watches <- watch
	cache := startCache(t, upstream, &rule{})
	<-upstream.opened
	watch.events <- store.WatchEvent{Type: store.WatchEventCreate, Object: original}
	watch.events <- store.WatchEvent{Type: store.WatchEventBookmark, ResourceVersion: 999}
	waitSync(t, cache)
	// Hold propagation while writes commit to the source.
	if err := cache.Patch(t.Context(), &rule{ObjectMeta: store.ObjectMeta{ID: "rule"}}, store.FieldPatch{Operations: []store.FieldOperation{{Op: store.FieldOpSet, Path: "subject", Value: "new"}}}); err != nil {
		t.Fatal(err)
	}
	authoritative := &rule{}
	if err := cache.Get(t.Context(), "rule", authoritative); err != nil {
		t.Fatal(err)
	}
	if authoritative.Subject != "new" {
		t.Fatalf("default Get did not read source: %+v", authoritative)
	}
	management := &store.List[rule]{}
	if err := cache.List(t.Context(), management, store.WithFieldRequirements(selector.RequirementEqual("subject", "new"))); err != nil {
		t.Fatal(err)
	}
	if len(management.Items) != 1 {
		t.Fatalf("default List did not read source: %+v", management.Items)
	}
	cached := list(t, cache, store.WithFieldRequirements(selector.RequirementEqual("service", "svc"), selector.RequirementEqual("subject", "old")))
	if len(cached) != 1 || cached[0].ResourceVersion != original.ResourceVersion {
		t.Fatalf("cache changed before Watch propagation: %+v", cached)
	}
	cached[0].Labels["region"] = "changed"
	cached[0].Values[0] = "changed"
	cached[0].Subject = "changed"
	got := &rule{}
	if err := cache.Get(t.Context(), "rule", got, store.WithResourceVersion(0)); err != nil {
		t.Fatal(err)
	}
	if got.Subject != "old" || got.Labels["region"] != "east" || got.Values[0] != "original" {
		t.Fatalf("List result mutated cache: %+v", got)
	}
	got.Subject = "changed"
	got.Labels["region"] = "changed"
	got.Values[0] = "changed"
	cached = list(t, cache, store.WithFieldRequirements(selector.RequirementEqual("service", "svc"), selector.RequirementEqual("subject", "old")))
	if len(cached) != 1 || cached[0].Subject != "old" || cached[0].Labels["region"] != "east" || cached[0].Values[0] != "original" {
		t.Fatalf("Get result mutated cache or index: %+v", cached)
	}
	watch.events <- store.WatchEvent{Type: store.WatchEventUpdate, Object: authoritative}
	until(t, func() bool {
		values := list(t, cache, store.WithFieldRequirements(selector.RequirementEqual("service", "svc"), selector.RequirementEqual("subject", "new")))
		return len(values) == 1 && values[0].ResourceVersion == authoritative.ResourceVersion
	})
	if old := list(t, cache, store.WithFieldRequirements(selector.RequirementEqual("service", "svc"), selector.RequirementEqual("subject", "old"))); len(old) != 0 {
		t.Fatalf("update retained old index: %+v", old)
	}
	if err := cache.Delete(t.Context(), authoritative); err != nil {
		t.Fatal(err)
	}
	if err := cache.Get(t.Context(), "rule", &rule{}); !commonerrors.IsNotFound(err) {
		t.Fatalf("default Get after deletion: %v", err)
	}
	if err := cache.Get(t.Context(), "rule", &rule{}, store.WithResourceVersion(0)); err != nil {
		t.Fatalf("cached Get before propagation: %v", err)
	}
	watch.events <- store.WatchEvent{Type: store.WatchEventDelete, Object: authoritative}
	until(t, func() bool { return len(list(t, cache)) == 0 })
}
