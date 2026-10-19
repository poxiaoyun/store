package inmemory

import (
	"bytes"
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
)

// NewQueue creates a process-local queue.
func NewQueue() *Queue {
	return &Queue{items: map[string]*queueItem{}, changed: make(chan struct{})}
}

var _ store.Queue = (*Queue)(nil)

// Queue stores queued work in process memory. Construct it with NewQueue.
type Queue struct {
	mu      sync.Mutex
	items   map[string]*queueItem
	pending queueHeap
	changed chan struct{}
}

type queueItem struct {
	id          string
	data        []byte
	availableAt time.Time
	// index is -1 once claimed, including after successful completion.
	index int
}

type queueHeap []*queueItem

func (h queueHeap) Len() int           { return len(h) }
func (h queueHeap) Less(i, j int) bool { return h[i].availableAt.Before(h[j].availableAt) }
func (h queueHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}
func (h *queueHeap) Push(value any) {
	item := value.(*queueItem)
	item.index = len(*h)
	*h = append(*h, item)
}
func (h *queueHeap) Pop() any {
	last := len(*h) - 1
	item := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	item.index = -1
	return item
}

// Enqueue implements store.Queue.
func (q *Queue) Enqueue(ctx context.Context, id string, data []byte, options store.EnqueueOptions) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == "" {
		id = uuid.NewString()
	}
	item, exists := q.items[id]
	if exists && item.index < 0 {
		return nil
	}
	if !exists {
		item = &queueItem{id: id}
		q.items[id] = item
	}
	item.data = bytes.Clone(data)
	item.availableAt = time.Now().
		Add(options.After)
	if exists {
		heap.Fix(&q.pending, item.index)
	} else {
		heap.Push(&q.pending, item)
	}
	close(q.changed)
	q.changed = make(chan struct{})
	return nil
}

// Consume implements store.Queue.
func (q *Queue) Consume(ctx context.Context, handler store.QueueHandleFunc, options store.ConsumeOptions) error {
	if options.MaxWorkers < 0 {
		return errors.NewBadRequest("MaxWorkers must not be negative")
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	group, ctx := errgroup.WithContext(ctx)
	for range max(1, options.MaxWorkers) {
		group.Go(func() error {
			for {
				item, err := q.claim(ctx)
				if err != nil {
					return err
				}
				err = handler(ctx, item.id, bytes.Clone(item.data))
				if err != nil {
					// Stop this consumer before making the failed message available again.
					cancel(err)
				}
				q.mu.Lock()
				if err != nil {
					heap.Push(&q.pending, item)
					close(q.changed)
					q.changed = make(chan struct{})
				} else {
					item.data = nil
				}
				q.mu.Unlock()
				if err != nil {
					return err
				}
			}
		})
	}
	if err := group.Wait(); err != nil {
		return context.Cause(ctx)
	}
	return nil
}

func (q *Queue) claim(ctx context.Context) (*queueItem, error) {
	for {
		q.mu.Lock()
		if err := context.Cause(ctx); err != nil {
			q.mu.Unlock()
			return nil, err
		}
		var timer *time.Timer
		var due <-chan time.Time
		if len(q.pending) > 0 {
			wait := time.Until(q.pending[0].availableAt)
			if wait <= 0 {
				item := heap.Pop(&q.pending).(*queueItem)
				q.mu.Unlock()
				return item, nil
			}
			timer = time.NewTimer(wait)
			due = timer.C
		}
		changed := q.changed
		q.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		case <-due:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}
