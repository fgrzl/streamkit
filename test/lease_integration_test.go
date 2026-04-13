package test

import (
	"context"
	"testing"
	"time"

	"github.com/fgrzl/streamkit/pkg/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestShouldReturnLeaseNotAcquiredGivenActiveLeaseWhenSecondHolderRequestsSameKey(t *testing.T) {
	for name, h := range configurations() {
		t.Run(name, func(t *testing.T) {
			harness := h(t)
			storeID := uuid.New()
			key := "lease-key-contention"

			leaseAcquired := make(chan struct{})
			releaseLease := make(chan struct{})
			firstErrCh := make(chan error, 1)

			go func() {
				err := harness.Client.WithLease(t.Context(), storeID, key, 2*time.Second, func(context.Context) error {
					close(leaseAcquired)
					<-releaseLease
					return nil
				})
				firstErrCh <- err
			}()

			require.Eventually(t, func() bool {
				select {
				case <-leaseAcquired:
					return true
				default:
					return false
				}
			}, 2*time.Second, 10*time.Millisecond)

			err := harness.Client.WithLease(t.Context(), storeID, key, 2*time.Second, func(context.Context) error {
				return nil
			})
			require.ErrorIs(t, err, client.ErrLeaseNotAcquired)

			close(releaseLease)
			require.NoError(t, <-firstErrCh)

			err = harness.Client.WithLease(t.Context(), storeID, key, 2*time.Second, func(context.Context) error {
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func TestShouldCancelLeaseContextGivenParentContextCanceledWhenWithLeaseRuns(t *testing.T) {
	for name, h := range configurations() {
		t.Run(name, func(t *testing.T) {
			harness := h(t)
			storeID := uuid.New()
			key := "lease-key-parent-cancel"

			parentCtx, cancelParent := context.WithCancel(t.Context())
			started := make(chan struct{})

			errCh := make(chan error, 1)
			go func() {
				err := harness.Client.WithLease(parentCtx, storeID, key, 3*time.Second, func(leaseCtx context.Context) error {
					close(started)
					<-leaseCtx.Done()
					return leaseCtx.Err()
				})
				errCh <- err
			}()

			require.Eventually(t, func() bool {
				select {
				case <-started:
					return true
				default:
					return false
				}
			}, 2*time.Second, 10*time.Millisecond)

			cancelParent()

			require.Eventually(t, func() bool {
				select {
				case err := <-errCh:
					return err == context.Canceled
				default:
					return false
				}
			}, 2*time.Second, 10*time.Millisecond)
		})
	}
}
