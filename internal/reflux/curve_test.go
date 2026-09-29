package reflux

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wyw14/solo-039/internal/model"
)

func warmingCampaign() model.Campaign {
	c := model.NewCampaign("test-grade")
	c.Phase = model.Warming
	return c
}

func TestCurveValidateRejectsBadCurves(t *testing.T) {
	cases := map[string]Curve{
		"empty": {Segments: nil},
		"missing phase": {Segments: []Segment{
			{Target: 0.3, Timeout: time.Second},
		}},
		"target out of range high": {Segments: []Segment{
			{Phase: model.Warming, Target: 1.2, Timeout: time.Second},
		}},
		"target out of range low": {Segments: []Segment{
			{Phase: model.Warming, Target: -0.1, Timeout: time.Second},
		}},
		"retreating targets": {Segments: []Segment{
			{Phase: model.Warming, Target: 0.7, Timeout: time.Second},
			{Phase: model.Stabilizing, Target: 0.3, Timeout: time.Second},
		}},
		"zero timeout": {Segments: []Segment{
			{Phase: model.Warming, Target: 0.3},
		}},
		"hold beyond timeout": {Segments: []Segment{
			{Phase: model.Warming, Target: 0.3, HoldFor: time.Second, Timeout: time.Second},
		}},
	}
	for name, curve := range cases {
		t.Run(name, func(t *testing.T) {
			if err := curve.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestRunCurveExecutesOnlyCurrentPhase(t *testing.T) {
	state := New()
	curve := Curve{Segments: []Segment{
		{Phase: model.Warming, Target: 0.35, Band: 0.02, HoldFor: 2 * time.Millisecond, Timeout: time.Second, Step: 0.1},
		{Phase: model.Stabilizing, Target: 0.7, Band: 0.02, HoldFor: 2 * time.Millisecond, Timeout: time.Second, Step: 0.1},
		{Phase: model.OnSpec, Target: 0.9, Band: 0.02, HoldFor: 2 * time.Millisecond, Timeout: time.Second, Step: 0.1},
	}}
	progress, err := state.RunCurve(context.Background(), warmingCampaign(), curve, time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	if progress.Done || progress.Index != 1 {
		t.Fatalf("warming run must stop before stabilizing: %+v", progress)
	}
	if progress.Segments[0].Status != SegmentReached {
		t.Fatalf("warming segment not reached: %+v", progress.Segments[0])
	}
	if got := state.Snapshot().Ratio; got > 0.35+0.02 {
		t.Fatalf("reflux jumped past the warming target: %.3f", got)
	}
	for i := 1; i < 3; i++ {
		if progress.Segments[i].Status != SegmentPending {
			t.Fatalf("segment %d must remain pending: %s", i, progress.Segments[i].Status)
		}
	}

	// 阶段切换后续跑：升温段结果保留，继续稳定段。
	stabilizing := warmingCampaign()
	stabilizing.Phase = model.Stabilizing
	progress, err = state.ResumeCurve(context.Background(), stabilizing, time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	if progress.Index != 2 || progress.Segments[0].Status != SegmentReached || progress.Segments[1].Status != SegmentReached {
		t.Fatalf("stabilizing resume lost previous results: %+v", progress)
	}
	if progress.Segments[2].Status != SegmentPending {
		t.Fatal("on-spec segment must remain pending until its phase")
	}

	onSpec := stabilizing
	onSpec.Phase = model.OnSpec
	progress, err = state.ResumeCurve(context.Background(), onSpec, time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !progress.Done {
		t.Fatalf("curve should be complete: %+v", progress)
	}
	for i, segment := range progress.Segments {
		if segment.Status != SegmentReached || segment.ArrivedAt == 0 {
			t.Fatalf("segment %d missing arrival record: %+v", i, segment)
		}
		if deviation := segment.Ratio - segment.Target; deviation > 0.02 || deviation < -0.02 {
			t.Fatalf("segment %d deviation out of band: %+.3f", i, deviation)
		}
	}
	if got := state.Snapshot().Ratio; got < 0.9-0.02 || got > 0.9+0.02 {
		t.Fatalf("final reflux ratio off target: %.3f", got)
	}
}

func TestRunCurveStaysInSegmentWhenTargetNotReached(t *testing.T) {
	state := New()
	curve := Curve{Segments: []Segment{
		{Phase: model.Warming, Target: 0.35, Band: 0.02, HoldFor: 2 * time.Millisecond, Timeout: time.Second, Step: 0.1},
		// 步长极小、超时极短：该段无法在超时内到达，必须停留并说明原因。
		{Phase: model.Stabilizing, Target: 0.9, Band: 0.02, HoldFor: 0, Timeout: 30 * time.Millisecond, Step: 0.01},
	}}
	if _, err := state.RunCurve(context.Background(), warmingCampaign(), curve, 2*time.Millisecond, nil); err != nil {
		t.Fatal(err)
	}
	stabilizing := warmingCampaign()
	stabilizing.Phase = model.Stabilizing
	progress, err := state.ResumeCurve(context.Background(), stabilizing, 2*time.Millisecond, nil)
	if !errors.Is(err, ErrSegmentTimeout) {
		t.Fatalf("expected segment timeout, got %v", err)
	}
	if progress.Done || progress.Index != 1 {
		t.Fatalf("must stay at the blocked segment: %+v", progress)
	}
	if progress.Segments[0].Status != SegmentReached {
		t.Fatal("previous segment result must be retained")
	}
	blocked := progress.Segments[1]
	if blocked.Status != SegmentBlocked || blocked.Reason == "" {
		t.Fatalf("blocked segment needs status and reason: %+v", blocked)
	}
	if state.Snapshot().Ratio >= 0.9-0.02 {
		t.Fatal("ratio must not have jumped to an unreached target")
	}

	// 再次续跑仍停留在同一段，不能跳过该控制步骤。
	progress, err = state.ResumeCurve(context.Background(), stabilizing, 2*time.Millisecond, nil)
	if !errors.Is(err, ErrSegmentTimeout) {
		t.Fatalf("re-resume must stay blocked, got %v", err)
	}
	if progress.Index != 1 || progress.Segments[1].Status != SegmentBlocked {
		t.Fatalf("blocked segment was skipped: %+v", progress)
	}
}

func TestRunCurveInterruptionStaysInSegment(t *testing.T) {
	state := New()
	curve := Curve{Segments: []Segment{
		{Phase: model.Warming, Target: 0.35, Band: 0.02, HoldFor: 100 * time.Millisecond, Timeout: 5 * time.Second, Step: 0.1},
		{Phase: model.Stabilizing, Target: 0.7, Band: 0.02, HoldFor: 2 * time.Millisecond, Timeout: time.Second, Step: 0.1},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	progress, err := state.RunCurve(ctx, warmingCampaign(), curve, time.Millisecond, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	if progress.Segments[0].Status != SegmentInterrupted || progress.Segments[0].Reason == "" {
		t.Fatalf("interrupted segment must record reason: %+v", progress.Segments[0])
	}
	if progress.Segments[1].Status != SegmentPending {
		t.Fatal("later segment must stay pending")
	}

	// 取消后用新上下文在同一段继续，最终可到达，且不跳过任何段。
	progress, err = state.ResumeCurve(context.Background(), warmingCampaign(), time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	if progress.Index != 1 || progress.Segments[0].Status != SegmentReached {
		t.Fatalf("resume after interruption should complete the segment: %+v", progress)
	}
}

func TestRunCurveReportsArrivalFeedbackViaHook(t *testing.T) {
	state := New()
	curve := DefaultCurve()
	var reached []SegmentResult
	_, err := state.RunCurve(context.Background(), warmingCampaign(), curve, time.Millisecond, func(result SegmentResult) {
		reached = append(reached, result)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(reached) != 1 || reached[0].Status != SegmentReached {
		t.Fatalf("hook should fire once for the reached warming segment: %+v", reached)
	}
	if reached[0].ArrivedAt == 0 || reached[0].Elapsed <= 0 {
		t.Fatal("arrival feedback missing actual arrival time")
	}
}

func TestRampLandsExactlyOnTargetWithoutOvershoot(t *testing.T) {
	state := New()
	c := warmingCampaign()
	if err := state.Ramp(context.Background(), c, 0.35, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got := state.Snapshot().Ratio; got != 0.35 {
		t.Fatalf("ramp overshot target: got %.3f want 0.35", got)
	}
}

func TestResumeWithoutCurveRejected(t *testing.T) {
	state := New()
	if _, err := state.ResumeCurve(context.Background(), warmingCampaign(), time.Millisecond, nil); err == nil {
		t.Fatal("resume without a committed curve must fail")
	}
}
