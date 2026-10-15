package etcd

import (
	"context"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
)

const (
	defaultLeaseReuseDurationSeconds   = 60
	defaultLeaseReuseDurationPercent   = 0.1
	defaultLeaseMaxObjectCountPerLease = 1000
)

// leaseManager is used to manage leases requested from etcd. If a new write
// needs a lease that has similar expiration time to the previous one, the old
// lease will be reused to reduce the overhead of etcd, since lease operations
// are expensive. In the implementation, we only store one previous lease,
// since all the events have the same ttl.
type leaseManager struct {
	client                  *kubernetes.Client // etcd client used to grant leases
	leaseMu                 sync.Mutex
	prevLeaseID             clientv3.LeaseID
	prevLeaseExpirationTime time.Time
	// The period of time in seconds and percent of TTL that each lease is
	// reused. The minimum of them is used to avoid unreasonably large
	// numbers.
	leaseReuseDurationSeconds   int64
	leaseReuseDurationPercent   float64
	leaseMaxAttachedObjectCount int64
	leaseAttachedObjectCount    int64
}

func newEtcd3LeaseManager(client *kubernetes.Client, leaseReuseDurationSeconds int64, leaseReuseDurationPercent float64, maxObjectCount int64) *leaseManager {
	if maxObjectCount <= 0 {
		maxObjectCount = defaultLeaseMaxObjectCountPerLease
	}
	if leaseReuseDurationSeconds <= 0 {
		leaseReuseDurationSeconds = defaultLeaseReuseDurationSeconds
	}
	if leaseReuseDurationPercent <= 0 {
		leaseReuseDurationPercent = defaultLeaseReuseDurationPercent
	}
	return &leaseManager{
		client:                      client,
		leaseReuseDurationSeconds:   leaseReuseDurationSeconds,
		leaseReuseDurationPercent:   leaseReuseDurationPercent,
		leaseMaxAttachedObjectCount: maxObjectCount,
	}
}

// GetLease returns a lease based on requested ttl: if the cached previous
// lease can be reused, reuse it; otherwise request a new one from etcd.
func (l *leaseManager) GetLease(ctx context.Context, ttl int64) (clientv3.LeaseID, error) {
	now := time.Now()
	l.leaseMu.Lock()
	defer l.leaseMu.Unlock()
	// check if previous lease can be reused
	reuseDurationSeconds := int64(l.leaseReuseDurationPercent * float64(ttl))
	if reuseDurationSeconds > l.leaseReuseDurationSeconds {
		reuseDurationSeconds = l.leaseReuseDurationSeconds
	}
	valid := now.Add(time.Duration(ttl) * time.Second).Before(l.prevLeaseExpirationTime)
	sufficient := now.Add(time.Duration(ttl+reuseDurationSeconds) * time.Second).After(l.prevLeaseExpirationTime)

	// We count all operations that happened in the same lease, regardless of success or failure.
	if valid && sufficient && l.leaseAttachedObjectCount < l.leaseMaxAttachedObjectCount {
		l.leaseAttachedObjectCount++
		return l.prevLeaseID, nil
	}
	// request a lease with a little extra ttl from etcd
	ttl += reuseDurationSeconds
	lcr, err := l.client.Lease.Grant(ctx, ttl)
	if err != nil {
		return clientv3.LeaseID(0), err
	}
	// cache the new lease id
	l.prevLeaseID = lcr.ID
	l.prevLeaseExpirationTime = now.Add(time.Duration(ttl) * time.Second)
	l.leaseAttachedObjectCount = 1
	return lcr.ID, nil
}
