package wageringtest

import (
	"fmt"
	"testing"
	"time"

	"github.com/monii/backend-challenge-go/internal/application/wagering"
)

func TestSuiteInMemory(t *testing.T) {
	RunSuite(t, func(t *testing.T) *Env {
		store := NewMemStore()
		clock := NewFakeClock(time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC))
		n := 0
		return &Env{
			Clock:  clock,
			Events: store.Events,
			NewService: func() *wagering.Service {
				n++
				return wagering.NewService(store, wagering.DefaultConfig(),
					wagering.WithClock(clock.Now), wagering.WithInstanceID(fmt.Sprintf("instance-%d", n)))
			},
		}
	})
}
