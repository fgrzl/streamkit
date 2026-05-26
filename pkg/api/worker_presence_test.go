package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWorkerPresenceTTLUsesMinimumGracePeriod(t *testing.T) {
	require.Equal(t, 30*time.Second, WorkerPresenceTTL(1))
	require.Equal(t, 90*time.Second, WorkerPresenceTTL(30))
}

func TestWorkerPresenceRenewIntervalDefaults(t *testing.T) {
	require.EqualValues(t, DefaultWorkerPresenceRenewIntervalSeconds, WorkerPresenceRenewIntervalSeconds(0))
	require.EqualValues(t, 15, WorkerPresenceRenewIntervalSeconds(15))
}

func TestWorkerPresenceRenewIntervalClampsLargeValues(t *testing.T) {
	require.EqualValues(t, MaxWorkerPresenceHeartbeatIntervalSeconds, WorkerPresenceRenewIntervalSeconds(7200))
	require.EqualValues(t, MaxWorkerPresenceHeartbeatIntervalSeconds, ClampWorkerPresenceHeartbeatIntervalSeconds(7200))
}
