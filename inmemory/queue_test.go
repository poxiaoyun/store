package inmemory_test

import (
	"testing"

	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/inmemory"
	"xiaoshiai.cn/store/storetest"
)

func TestQueueConformance(t *testing.T) {
	storetest.RunQueue(t, func(*testing.T) store.Queue { return inmemory.NewQueue() })
}
