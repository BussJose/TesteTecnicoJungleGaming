package wageringtest

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/monii/backend-challenge-go/internal/application/wagering"
	"github.com/monii/backend-challenge-go/internal/domain/id"
)

func TestSuiteInMemory(t *testing.T) {
	RunSuite(t, func(t *testing.T) *Env {
		store := NewMemStore()
		clock := NewFakeClock(time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC))
		var n atomic.Int64
		return &Env{
			Clock:  clock,
			Events: store.Events,
			Outbox: store,
			OutboxPending: func(_ context.Context, w id.ID) (int, error) {
				return store.PendingOutbox(w), nil
			},
			NewService: func() *wagering.Service {
				return wagering.NewService(store, wagering.DefaultConfig(),
					wagering.WithClock(clock.Now), wagering.WithInstanceID(fmt.Sprintf("instance-%d", n.Add(1))))
			},
		}
	})
}
