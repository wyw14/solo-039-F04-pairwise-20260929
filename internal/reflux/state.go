package reflux

import (
	"context"
	"github.com/wyw14/solo-039/internal/model"
	"sync"
	"time"
)

type State struct {
	mu        sync.RWMutex
	value     model.Reflux
	cancel    context.CancelFunc
	curve     *curveState
	lastCurve Curve
}

func New() *State { return &State{value: model.Reflux{PumpCapacity: 1}} }

func (s *State) Snapshot() model.Reflux { s.mu.RLock(); defer s.mu.RUnlock(); return s.value }

// Ramp 把回流比逐步逼近单一目标；每个周期只走一步，
// 超过目标时精确落在目标上，不产生回调或超调。
func (s *State) Ramp(ctx context.Context, c model.Campaign, target float64, tick time.Duration) error {
	child, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancel = cancel
	s.value.CommandEpoch = c.Epoch
	s.mu.Unlock()
	defer cancel()
	for {
		s.mu.Lock()
		if s.value.Ratio >= target {
			s.mu.Unlock()
			return nil
		}
		next := s.value.Ratio + defaultStep
		if next > target {
			next = target
		}
		s.value.Ratio = next
		s.mu.Unlock()
		select {
		case <-child.Done():
			return child.Err()
		case <-time.After(tick):
		}
	}
}

func (s *State) Stop() {
	s.mu.Lock()
	cancel := s.cancel
	s.cancel = nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *State) SetLease(lease model.Lease) { s.mu.Lock(); s.value.Lease = lease; s.mu.Unlock() }
