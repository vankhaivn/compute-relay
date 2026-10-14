package runtimehost

import (
	"github.com/vankhaivn/compute-relay/internal/cleanup"
)

// cleanupEngine applies accepted cleanup requests with the same frozen-binding resolver as
// the collector.
func cleanupEngine(repo cleanup.WorkRepository, resolver cleanup.Resolver) (*cleanup.Engine, error) {
	engine, err := cleanup.New(repo, resolver, cleanup.DefaultConfig())
	if err != nil {
		return nil, ErrState
	}
	return engine, nil
}
