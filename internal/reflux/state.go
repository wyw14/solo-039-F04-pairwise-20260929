package reflux

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/wyw14/solo-039/internal/model"
)

type State struct {
	mu     sync.RWMutex
	value  model.Reflux
	cancel context.CancelFunc

	curve      *Curve
	current    int
	results    []SegmentResult
	holdReason string
}

func New() *State { return &State{value: model.Reflux{PumpCapacity: 1}} }

func (s *State) Snapshot() model.Reflux { s.mu.RLock(); defer s.mu.RUnlock(); return s.value }

// Ramp 按固定步长把回流比推向单一目标（保留给恢复/审计路径使用）。
// 批次阶段驱动应走 CommitCurve + FollowCurve + AdvanceSegment。
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
		s.value.Ratio += 0.1
		s.mu.Unlock()
		select {
		case <-child.Done():
			return child.Err()
		case <-time.After(tick):
		}
	}
}

// CommitCurve 提交当前批次命令代次下的分段目标曲线。
// extend 为 true 时只能向尚未提交的新阶段追加，不能替换、跳段或回退目标。
func (s *State) CommitCurve(c model.Campaign, segments []CurveSegment, extend bool) error {
	if err := ValidateCurve(segments); err != nil {
		return err
	}
	segments = withDefaults(segments)
	s.mu.Lock()
	defer s.mu.Unlock()
	if extend {
		if s.curve == nil {
			return errors.New("no active reflux curve to extend")
		}
		seen := make(map[model.Phase]bool, len(s.curve.Segments)+len(segments))
		lastTarget := -1.0
		for _, existing := range s.curve.Segments {
			seen[existing.Phase] = true
			if existing.Target > lastTarget {
				lastTarget = existing.Target
			}
		}
		for _, segment := range segments {
			if seen[segment.Phase] {
				return fmt.Errorf("reflux curve phase %s is already submitted", segment.Phase)
			}
			if segment.Target < lastTarget {
				return fmt.Errorf("reflux curve phase %s target %.3f falls back from %.3f", segment.Phase, segment.Target, lastTarget)
			}
			seen[segment.Phase] = true
			lastTarget = segment.Target
		}
		s.curve.Segments = append(s.curve.Segments, segments...)
		return nil
	}
	if s.curve != nil && s.current < len(s.curve.Segments) {
		return errors.New("a reflux curve is already active; extend it segment by segment or wait for completion")
	}
	s.curve = &Curve{Epoch: c.Epoch, Segments: segments}
	s.current = 0
	s.results = nil
	s.holdReason = ""
	s.value.CommandEpoch = c.Epoch
	return nil
}

// FollowCurve 对当前段执行逐步控制，直到该段目标到达后返回该段结果，但不会自动跳到下一段。
// 控制循环在到达前被取消时，当前段保持 held 并记录原因，已完成段的结果不受影响。
func (s *State) FollowCurve(ctx context.Context, c model.Campaign, tick time.Duration) (SegmentResult, error) {
	s.mu.Lock()
	if s.curve == nil {
		s.mu.Unlock()
		return SegmentResult{}, errors.New("no reflux curve committed")
	}
	if s.current >= len(s.curve.Segments) {
		s.mu.Unlock()
		return SegmentResult{}, errors.New("reflux curve is already complete")
	}
	curve := s.curve
	segment := curve.Segments[s.current]
	result := s.ensureResultLocked(c.Epoch, segment)
	s.value.CommandEpoch = c.Epoch
	child, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.mu.Unlock()
	defer cancel()

	for {
		s.mu.Lock()
		if s.curve != curve {
			s.mu.Unlock()
			return SegmentResult{}, errors.New("reflux curve was superseded by a new submission")
		}
		segment = s.curve.Segments[s.current]
		if reachedSegment(s.value.Ratio, segment) {
			now := time.Now()
			result = s.results[s.current]
			result.Status = segReached
			result.Reason = ""
			result.Actual = s.value.Ratio
			result.Deviation = s.value.Ratio - segment.Target
			result.ReachedAt = now
			result.ArrivalMS = now.Sub(result.StartedAt).Milliseconds()
			s.results[s.current] = result
			s.holdReason = ""
			s.mu.Unlock()
			return result, nil
		}
		// 只朝本段目标走一个控制步，步长封顶 stepSize，绝不越过本段目标。
		delta := segment.Target - s.value.Ratio
		step := math.Min(stepSize, math.Abs(delta))
		s.value.Ratio += math.Copysign(step, delta)
		result = s.results[s.current]
		result.Status = segTracking
		result.Steps++
		result.Actual = s.value.Ratio
		result.Deviation = s.value.Ratio - segment.Target
		s.results[s.current] = result
		s.mu.Unlock()

		select {
		case <-child.Done():
			s.holdCurrent("control loop interrupted before segment target was reached: " + child.Err().Error())
			return SegmentResult{}, child.Err()
		case <-time.After(tick):
		}
	}
}

// AdvanceSegment 是阶段切换的唯一入口：当前段未达标就留在该段并说明原因，不允许跳过控制步骤。
func (s *State) AdvanceSegment(c model.Campaign) (CurveSegment, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.curve == nil {
		return CurveSegment{}, false, errors.New("no reflux curve committed")
	}
	if s.current >= len(s.curve.Segments) {
		return CurveSegment{}, false, errors.New("reflux curve is already complete")
	}
	if s.current >= len(s.results) || s.results[s.current].Status != segReached {
		reason := s.segmentNotReachedReasonLocked()
		s.holdReason = reason
		return CurveSegment{}, false, errors.New(reason)
	}
	s.current++
	s.holdReason = ""
	s.value.CommandEpoch = c.Epoch
	if s.current >= len(s.curve.Segments) {
		return CurveSegment{}, false, nil
	}
	return s.curve.Segments[s.current], true, nil
}

// CurveStatus 返回曲线跟踪快照，含每段的实际到达时间、偏差与留段原因。
func (s *State) CurveStatus() CurveStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	status := CurveStatus{Ratio: s.value.Ratio, Reason: s.holdReason}
	if s.curve != nil {
		status.Active = s.current < len(s.curve.Segments)
		status.Complete = !status.Active
		status.Epoch = s.curve.Epoch
		status.Total = len(s.curve.Segments)
		status.Current = s.current
		if status.Active {
			status.CurrentPhase = s.curve.Segments[s.current].Phase
		}
	}
	status.Results = append([]SegmentResult(nil), s.results...)
	return status
}

func (s *State) ensureResultLocked(epoch uint64, segment CurveSegment) SegmentResult {
	if s.current < len(s.results) {
		result := s.results[s.current]
		if result.Status == segHeld {
			result.Status = segTracking
			result.Reason = ""
			s.results[s.current] = result
		}
		return result
	}
	result := SegmentResult{
		Epoch:     epoch,
		Phase:     segment.Phase,
		Target:    segment.Target,
		Tolerance: segment.Tolerance,
		StartedAt: time.Now(),
		Status:    segTracking,
	}
	s.results = append(s.results, result)
	return result
}

func (s *State) holdCurrent(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.curve != nil && s.current < len(s.results) {
		result := s.results[s.current]
		if result.Status != segReached {
			result.Status = segHeld
			result.Reason = reason
			s.results[s.current] = result
		}
	}
	s.holdReason = reason
}

func (s *State) segmentNotReachedReasonLocked() string {
	segment := s.curve.Segments[s.current]
	if s.current >= len(s.results) {
		return fmt.Sprintf("segment %s target %.3f not reached: no control step has run yet", segment.Phase, segment.Target)
	}
	result := s.results[s.current]
	return fmt.Sprintf("segment %s held: target %.3f not reached (actual %.3f, deviation %+.3f, steps %d); %s",
		segment.Phase, segment.Target, result.Actual, result.Deviation, result.Steps, result.Reason)
}

func reachedSegment(ratio float64, segment CurveSegment) bool {
	deviation := math.Abs(ratio - segment.Target)
	return deviation <= segment.Tolerance
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
