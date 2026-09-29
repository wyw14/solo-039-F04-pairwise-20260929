package reflux

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/wyw14/solo-039/internal/model"
)

const (
	defaultStep = 0.1
	defaultBand = 0.05
)

// 段结果状态。
const (
	SegmentPending     = "pending"     // 尚未执行
	SegmentReached     = "reached"     // 实际值进入带宽并完成保持
	SegmentBlocked     = "blocked"     // 超时仍未达目标，停留在本段
	SegmentInterrupted = "interrupted" // 控制被取消，停留在本段
)

// ErrSegmentTimeout 在某段超时未达目标时返回，曲线停留在该段。
var ErrSegmentTimeout = errors.New("reflux segment target not reached before timeout")

// Segment 是按批次阶段提交的回流目标曲线中的一段：
// 回流比只能沿曲线逐段逼近，不允许从当前值一下跳到最终目标。
type Segment struct {
	Phase   model.Phase   // 该段归属的批次阶段
	Target  float64       // 该段回流比目标
	Band    float64       // 到达判定带宽 |实际-目标| <= Band；<=0 用 defaultBand
	HoldFor time.Duration // 进入带宽后须连续保持的时长；0 表示进入带宽即到达
	Timeout time.Duration // 该段最长执行时间，超时则停留并说明原因
	Step    float64       // 每个控制周期的步长；<=0 用 defaultStep
}

// Curve 是一次提交的分阶段回流目标曲线。
type Curve struct {
	Segments []Segment
}

// DefaultCurve 是升温->稳定阶段的默认目标曲线：先在升温段建立初步回流，
// 进入稳定阶段后再继续逼近稳定操作值，避免直接跳到最终回流比。
func DefaultCurve() Curve {
	return Curve{Segments: []Segment{
		{Phase: model.Warming, Target: 0.35, Band: 0.02, HoldFor: 5 * time.Millisecond, Timeout: 5 * time.Second, Step: 0.1},
		{Phase: model.Stabilizing, Target: 0.7, Band: 0.02, HoldFor: 5 * time.Millisecond, Timeout: 5 * time.Second, Step: 0.1},
	}}
}

// Validate 校验曲线：至少一段、阶段非空、目标在量程内且只许单调递增、
// 超时为正、保持窗口不大于超时。
func (q Curve) Validate() error {
	if len(q.Segments) == 0 {
		return errors.New("reflux curve requires at least one segment")
	}
	previous := 0.0
	for i, segment := range q.Segments {
		if segment.Phase == "" {
			return fmt.Errorf("reflux curve segment %d is missing a batch phase", i)
		}
		if math.IsNaN(segment.Target) || segment.Target < 0 || segment.Target > 1 {
			return fmt.Errorf("reflux curve segment %d target %.3f is outside the reflux range [0,1]", i, segment.Target)
		}
		if i > 0 && segment.Target < previous {
			return fmt.Errorf("reflux curve segment %d target %.3f retreats from the previous target %.3f", i, segment.Target, previous)
		}
		if segment.Timeout <= 0 {
			return fmt.Errorf("reflux curve segment %d requires a positive timeout", i)
		}
		if segment.HoldFor < 0 {
			return fmt.Errorf("reflux curve segment %d hold time must be non-negative", i)
		}
		if segment.HoldFor >= segment.Timeout {
			return fmt.Errorf("reflux curve segment %d hold time must be shorter than its timeout", i)
		}
		if segment.Step < 0 {
			return fmt.Errorf("reflux curve segment %d step must be non-negative", i)
		}
		previous = segment.Target
	}
	return nil
}

// SegmentResult 是单段的反馈记录：实际到达时间、到达时回流比与偏差、
// 未到达时的状态和原因。
type SegmentResult struct {
	Phase     model.Phase   `json:"phase"`
	Index     int           `json:"index"`
	Target    float64       `json:"target"`
	Status    string        `json:"status"`
	ArrivedAt int64         `json:"arrived_at_nano,omitempty"`
	Elapsed   time.Duration `json:"elapsed_nano"`
	Ratio     float64       `json:"ratio"`
	Deviation float64       `json:"deviation"`
	Reason    string        `json:"reason,omitempty"`
}

// CurveProgress 是曲线执行进度的不可变快照。
type CurveProgress struct {
	Epoch    uint64          `json:"epoch"`
	Segments []SegmentResult `json:"segments"`
	Index    int             `json:"index"` // 当前停留段（未完成时）或已完成段数
	Done     bool            `json:"done"`
	Reason   string          `json:"reason,omitempty"` // 最近一次停留原因
}

// curveState 是 State 内部持有的执行态，受 State.mu 保护。
type curveState struct {
	epoch    uint64
	segments []SegmentResult
	index    int
	done     bool
	reason   string
}

func (s *State) snapshotCurve() CurveProgress {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshotCurveLocked()
}

func (s *State) snapshotCurveLocked() CurveProgress {
	if s.curve == nil {
		return CurveProgress{}
	}
	return CurveProgress{
		Epoch:    s.curve.epoch,
		Segments: append([]SegmentResult(nil), s.curve.segments...),
		Index:    s.curve.index,
		Done:     s.curve.done,
		Reason:   s.curve.reason,
	}
}

// Curve 返回最近一次提交的曲线执行进度；未提交过时 ok 为 false。
func (s *State) Curve() (CurveProgress, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.curve == nil {
		return CurveProgress{}, false
	}
	return s.snapshotCurveLocked(), true
}

// SegmentHook 在每段执行终态（到达/停留/中断）时被回调，供上层记录反馈。
type SegmentHook func(SegmentResult)

// RunCurve 提交整条目标曲线，但只执行归属当前批次阶段（campaign.Phase）的段：
// 遇到属于后续阶段的段时保留进度正常返回，等待阶段切换后续跑。
// 已完成段的反馈结果始终保留；当前段未达目标则停留在该段并给出原因，
// 段内每个周期只执行一个控制步骤，不能跳步。
func (s *State) RunCurve(ctx context.Context, c model.Campaign, q Curve, tick time.Duration, onSegment SegmentHook) (CurveProgress, error) {
	if err := q.Validate(); err != nil {
		return CurveProgress{}, err
	}
	child, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancel = cancel
	s.value.CommandEpoch = c.Epoch
	s.lastCurve = q
	state := &curveState{epoch: c.Epoch, segments: make([]SegmentResult, len(q.Segments))}
	for i, segment := range q.Segments {
		state.segments[i] = SegmentResult{Phase: segment.Phase, Index: i, Target: segment.Target, Status: SegmentPending}
	}
	s.curve = state
	s.mu.Unlock()
	defer cancel()
	return s.runSegments(child, c, q, 0, tick, onSegment)
}

// ResumeCurve 在批次阶段切换后续跑曲线：上一段结果原样保留，
// 从当前停留点继续；若停留段曾未达目标则在该段重新执行，不会跳过。
func (s *State) ResumeCurve(ctx context.Context, c model.Campaign, tick time.Duration, onSegment SegmentHook) (CurveProgress, error) {
	s.mu.RLock()
	q, state := s.lastCurve, s.curve
	s.mu.RUnlock()
	if state == nil {
		return CurveProgress{}, errors.New("no reflux curve has been committed")
	}
	if err := q.Validate(); err != nil {
		return CurveProgress{}, err
	}
	start := state.index
	if start > len(q.Segments) {
		start = len(q.Segments)
	}
	child, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancel = cancel
	s.value.CommandEpoch = c.Epoch
	s.curve.epoch = c.Epoch
	s.curve.reason = ""
	s.mu.Unlock()
	defer cancel()
	return s.runSegments(child, c, q, start, tick, onSegment)
}

func (s *State) runSegments(ctx context.Context, c model.Campaign, q Curve, start int, tick time.Duration, onSegment SegmentHook) (CurveProgress, error) {
	for i := start; i < len(q.Segments); i++ {
		segment := q.Segments[i]
		// 只认领当前批次阶段的段；后续阶段的段等阶段切换后再续跑。
		if segment.Phase != c.Phase {
			s.mu.Lock()
			s.curve.index = i
			progress := s.snapshotCurveLocked()
			s.mu.Unlock()
			return progress, nil
		}
		s.mu.Lock()
		s.curve.index = i
		s.curve.segments[i] = SegmentResult{Phase: segment.Phase, Index: i, Target: segment.Target, Status: SegmentPending}
		s.mu.Unlock()

		status, reason, arrivedAt, elapsed, ratio, err := s.runSegment(ctx, i, segment, tick)
		result := SegmentResult{
			Phase:     segment.Phase,
			Index:     i,
			Target:    segment.Target,
			Status:    status,
			ArrivedAt: arrivedAt,
			Elapsed:   elapsed,
			Ratio:     ratio,
			Deviation: ratio - segment.Target,
			Reason:    reason,
		}
		s.mu.Lock()
		s.curve.segments[i] = result
		if status != SegmentReached {
			// 未达目标：停留在本段，保留此前各段结果，后续段保持 pending。
			s.curve.reason = reason
			progress := s.snapshotCurveLocked()
			s.mu.Unlock()
			if onSegment != nil {
				onSegment(result)
			}
			return progress, err
		}
		s.mu.Unlock()
		if onSegment != nil {
			onSegment(result)
		}
	}
	s.mu.Lock()
	s.curve.index = len(q.Segments)
	s.curve.done = true
	progress := s.snapshotCurveLocked()
	s.mu.Unlock()
	return progress, nil
}

// runSegment 执行单段反馈控制，返回终态及反馈数据。
// 每个控制周期最多执行一个步进，然后等待一个 tick 再读取反馈，禁止跳步。
func (s *State) runSegment(ctx context.Context, index int, segment Segment, tick time.Duration) (status, reason string, arrivedAt int64, elapsed time.Duration, ratio float64, err error) {
	step := segment.Step
	if step <= 0 {
		step = defaultStep
	}
	band := segment.Band
	if band <= 0 {
		band = defaultBand
	}
	startedAt := time.Now()
	deadline := startedAt.Add(segment.Timeout)
	var holdStart time.Time
	for {
		s.mu.RLock()
		ratio = s.value.Ratio
		s.mu.RUnlock()
		if math.Abs(ratio-segment.Target) <= band {
			if holdStart.IsZero() {
				holdStart = time.Now()
			}
			if time.Since(holdStart) >= segment.HoldFor {
				return SegmentReached, "", time.Now().UnixNano(), time.Since(startedAt), ratio, nil
			}
		} else {
			holdStart = time.Time{}
			// 仍在带宽外：只走一个控制步骤，精确落位且绝不越过目标。
			s.advanceRatio(segment.Target, step)
		}
		if time.Now().After(deadline) {
			reason = fmt.Sprintf("segment %d (%s) target %.3f not reached within %s: ratio=%.3f deviation=%+.3f",
				index, segment.Phase, segment.Target, segment.Timeout, ratio, ratio-segment.Target)
			return SegmentBlocked, reason, 0, time.Since(startedAt), ratio, ErrSegmentTimeout
		}
		select {
		case <-ctx.Done():
			reason = fmt.Sprintf("segment %d (%s) control interrupted at ratio=%.3f: %s",
				index, segment.Phase, ratio, ctx.Err().Error())
			return SegmentInterrupted, reason, 0, time.Since(startedAt), ratio, ctx.Err()
		case <-time.After(tick):
		}
	}
}

// advanceRatio 朝目标执行一个步进；超过目标时精确落到目标，不产生回调/超调。
func (s *State) advanceRatio(target, step float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.value.Ratio >= target {
		return
	}
	next := s.value.Ratio + step
	if next > target {
		next = target
	}
	s.value.Ratio = next
}
