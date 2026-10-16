package controller

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
	"xiaoshiai.cn/common/log"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/lease"
)

type Runable interface {
	Name() string
	Run(ctx context.Context) error
}

func NewControllerManager() *ControllerManager {
	return &ControllerManager{
		Controllers: map[string]Runable{},
	}
}

type ControllerManagerOptions struct {
	LeaderElection    bool   `json:"leaderElection"`
	LeaderElectionKey string `json:"leaderElectionKey"`
}

func (c *ControllerManager) WithStoreLeaderElection(storage store.Store, key string, ttl time.Duration) *ControllerManager {
	storage = storage.Scope(store.Scope{Resource: "namespaces", Name: "leader-election"})
	locker := lease.NewStoreLocker(storage, lease.Options{LeaseDuration: ttl})
	c.LeaderElection = lease.NewLeaderElection(locker, key)
	return c
}

type ControllerManager struct {
	Controllers         map[string]Runable
	LeaderElection      lease.LeaderElection
	DisabledControllers []string
	EnableControllers   []string
}

func (c *ControllerManager) DisableController(name ...string) {
	c.DisabledControllers = append(c.DisabledControllers, name...)
}

func (c *ControllerManager) EnableController(name ...string) {
	c.EnableControllers = append(c.EnableControllers, name...)
}

func (c *ControllerManager) AddController(controller Runable) error {
	if c.Controllers == nil {
		c.Controllers = map[string]Runable{}
	}
	name := controller.Name()
	if _, ok := c.Controllers[name]; ok {
		return fmt.Errorf("controller %s already exists", name)
	}
	c.Controllers[name] = controller
	return nil
}

func (c *ControllerManager) Run(ctx context.Context) error {
	log := log.FromContext(ctx)
	if c.LeaderElection != nil {
		log.Info("controller manager run with leader election")
		return c.LeaderElection.OnLeader(ctx, func(ctx context.Context) error {
			log.Info("controller manager run on leader elected")
			return c.run(ctx)
		})
	} else {
		log.Info("controller manager run without leader election")
		return c.run(ctx)
	}
}

func (c *ControllerManager) enabled(name string) bool {
	if len(c.EnableControllers) == 0 {
		return !contains(c.DisabledControllers, name)
	} else {
		return contains(c.EnableControllers, name)
	}
}

func contains(names []string, name string) bool {
	for _, n := range names {
		if n == name || n == "*" {
			return true
		}
	}
	return false
}

func (c *ControllerManager) run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)
	for name, controller := range c.Controllers {
		if !c.enabled(name) {
			log.FromContext(ctx).
				Info("skip disabled controller", "controller", name)
			continue
		}
		controller := controller
		eg.Go(func() error {
			if err := controller.Run(ctx); err != nil {
				log.FromContext(ctx).
					Error(err, "controller run failed", "controller", name)
				return err
			}
			return nil
		})
	}
	return eg.Wait()
}
