package controller

import (
	"time"

	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
)

type TypedQueue[T comparable] interface {
	Add(key T)
	Get() (item T, shutdown bool)
	Done(item T)
	Forget(item T)
	ShutDown()
	AddAfter(key T, after time.Duration)
	AddRateLimited(key T)

	When(item T) time.Duration
}

func NewDefaultTypedQueue[T comparable](name string, rateLimiter workqueue.TypedRateLimiter[T]) TypedQueue[T] {
	if rateLimiter == nil {
		rateLimiter = workqueue.DefaultTypedControllerRateLimiter[T]()
	}
	return NewTypedRateLimitingQueueWithConfig(rateLimiter, workqueue.TypedRateLimitingQueueConfig[T]{Name: name})
}

type DefaultTypedQueue[T comparable] TypedQueue[T]

func NewTypedRateLimitingQueueWithConfig[T comparable](rateLimiter workqueue.TypedRateLimiter[T], config workqueue.TypedRateLimitingQueueConfig[T]) *RateLimitingType[T] {
	if config.Clock == nil {
		config.Clock = clock.RealClock{}
	}
	if config.DelayingQueue == nil {
		config.DelayingQueue = workqueue.NewTypedDelayingQueueWithConfig(workqueue.TypedDelayingQueueConfig[T]{
			Name:            config.Name,
			MetricsProvider: config.MetricsProvider,
			Clock:           config.Clock,
		})
	}
	return &RateLimitingType[T]{
		TypedDelayingInterface: config.DelayingQueue,
		RateLimiter:            rateLimiter,
	}
}

// rateLimitingType wraps an Interface and provides rateLimited re-enquing
type RateLimitingType[T comparable] struct {
	workqueue.TypedDelayingInterface[T]
	RateLimiter workqueue.TypedRateLimiter[T]
}

// AddRateLimited AddAfter's the item based on the time when the rate limiter says it's ok
func (q *RateLimitingType[T]) AddRateLimited(item T) {
	q.TypedDelayingInterface.AddAfter(item, q.RateLimiter.When(item))
}

func (q *RateLimitingType[T]) NumRequeues(item T) int {
	return q.RateLimiter.NumRequeues(item)
}

func (q *RateLimitingType[T]) Forget(item T) {
	q.RateLimiter.Forget(item)
}

func (q *RateLimitingType[T]) When(item T) time.Duration {
	return q.RateLimiter.When(item)
}
