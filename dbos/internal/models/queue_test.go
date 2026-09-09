package models

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQueueConfigResolveLimits(t *testing.T) {
	two, one := 2, 1
	rl := &RateLimiter{Limit: 5, Period: time.Second}
	prl := &RateLimiter{Limit: 3, Period: time.Minute}

	plain := QueueConfig{GlobalConcurrency: &two, WorkerConcurrency: &one, RateLimit: rl}
	require.False(t, plain.IsPartitioned())
	require.False(t, plain.IsLegacyPartitioned())
	require.Equal(t, ResolvedQueueLimits{GlobalConcurrency: &two, WorkerConcurrency: &one, RateLimit: rl}, plain.ResolveLimits())

	legacy := plain
	legacy.PartitionQueue = true
	require.True(t, legacy.IsPartitioned())
	require.True(t, legacy.IsLegacyPartitioned())
	require.Equal(t, ResolvedQueueLimits{PartitionConcurrency: &two, PartitionWorkerConcurrency: &one, PartitionRateLimit: rl}, legacy.ResolveLimits())

	limited := plain
	limited.PartitionConcurrency = &one
	limited.PartitionRateLimit = prl
	require.True(t, limited.HasPartitionLimits())
	require.True(t, limited.IsPartitioned())
	require.False(t, limited.IsLegacyPartitioned())
	require.Equal(t, ResolvedQueueLimits{
		GlobalConcurrency: &two, WorkerConcurrency: &one, RateLimit: rl,
		PartitionConcurrency: &one, PartitionRateLimit: prl,
	}, limited.ResolveLimits())

	// A healed row (flag plus limits) is not the legacy mode.
	both := limited
	both.PartitionQueue = true
	require.False(t, both.IsLegacyPartitioned())
	require.Equal(t, limited.ResolveLimits(), both.ResolveLimits())
}
