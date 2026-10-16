// Package lease provides Store-backed distributed locks and leader election.
package lease

import (
	"context"
	"errors"
	"time"

	"xiaoshiai.cn/common/meta"
	"xiaoshiai.cn/store"
)

// ErrLockLost indicates that an operation no longer owns its lease.
var ErrLockLost = errors.New("lease: lock lost")

// Locker serializes operations with the same name across processes.
type Locker interface {
	WithLock(ctx context.Context, name string, operation func(context.Context) error) error
}

// LeaderElection runs an operation while leadership is held.
type LeaderElection interface {
	OnLeader(ctx context.Context, operation func(context.Context) error) error
}

// Options configures Store-backed leases.
type Options struct {
	Identity      string
	LeaseDuration time.Duration
	RetryPeriod   time.Duration
}

// Lease is the persisted record shared by lease-based primitives.
type Lease struct {
	store.ObjectMeta     `json:",inline"`
	HolderIdentity       string    `json:"holderIdentity"`
	LeaseDurationSeconds int       `json:"leaseDurationSeconds"`
	AcquireTime          meta.Time `json:"acquireTime"`
	RenewTime            meta.Time `json:"renewTime"`
	LeaderTransitions    int       `json:"leaderTransitions"`
}
