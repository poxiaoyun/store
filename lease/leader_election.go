package lease

import (
	"context"
	"fmt"
)

// NewLeaderElection adapts one named Locker operation to LeaderElection.
func NewLeaderElection(locker Locker, name string) LeaderElection {
	return &leaderElection{locker: locker, name: name}
}

type leaderElection struct {
	locker Locker
	name   string
}

func (e *leaderElection) OnLeader(ctx context.Context, operation func(context.Context) error) error {
	if err := e.locker.WithLock(ctx, e.name, operation); err != nil {
		return fmt.Errorf("leader election failed: %w", err)
	}
	return nil
}
