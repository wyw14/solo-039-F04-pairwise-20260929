package reflux

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/wyw14/solo-039/internal/model"
)

func testCampaign() model.Campaign { return model.NewCampaign("baseline") }

func within(actual, target, tolerance float64) bool {
	return math.Abs(actual-target) <= tolerance
}

// waitFor 轮询条件，避免依赖固定 sleep 在慢机器上产生竞态。
func waitFor(condition func() bool, t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met before deadline")
}

// 完整走两段：升温段 0→0.5，再稳定段 0.5→0.8，验证逐步逼近、
// 每段到达时间/偏差/步数被记录，且阶段切换不会让回流比跳变。
func TestCurveFollowsSegmentsInOrderWithoutJump(t *testing.T) {
	state := New()
	campaign := testCampaign()
	segments := []CurveSegment{
		{Phase: model.Warming, Target: 0.5},
		{Phase: model.Stabilizing, Target: 0.8},
	}
	if err := state.CommitCurve(campaign, segments, false); err != nil {
		t.Fatal(err)
	}

	warming, err := state.FollowCurve(context.Background(), campaign, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if warming.Steps != 5 {
		t.Fatalf("warming segment expected 5 steps, got %d", warming.Steps)
	}
	if !within(warming.Actual, 0.5, warming.Tolerance) || warming.ArrivalMS < 0 {
		t.Fatalf("warming segment did not arrive: %+v", warming)
	}
	if got := state.Snapshot().Ratio; !within(got, 0.5, 0.001) {
		t.Fatalf("ratio after warming = %v, want 0.5", got)
	}

	// 未达标段不允许推进；这里已达标，推进到稳定段。
	next, ok, err := state.AdvanceSegment(campaign)
	if err != nil || !ok || next.Phase != model.Stabilizing {
		t.Fatalf("advance to stabilizing failed: %v %v %v", next, ok, err)
	}

	// 阶段切换必须以上一段的到达值为起点，不能直接跳到最终值。
	if got := state.Snapshot().Ratio; !within(got, 0.5, 0.001) {
		t.Fatalf("ratio jumped on segment switch: %v", got)
	}

	stabilizing, err := state.FollowCurve(context.Background(), campaign, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if stabilizing.Steps != 3 {
		t.Fatalf("stabilizing segment expected 3 steps, got %d", stabilizing.Steps)
	}
	if !within(stabilizing.Actual, 0.8, stabilizing.Tolerance) {
		t.Fatalf("stabilizing segment did not arrive: %+v", stabilizing)
	}

	// 最后一段推进后曲线完成，且上一段的结果仍保留。
	if _, ok, err := state.AdvanceSegment(campaign); err != nil || ok {
		t.Fatalf("expected curve completion, got ok=%v err=%v", ok, err)
	}
	status := state.CurveStatus()
	if !status.Complete || len(status.Results) != 2 {
		t.Fatalf("expected complete curve with 2 results, got %+v", status)
	}
	if status.Results[0].Status != segReached || status.Results[1].Status != segReached {
		t.Fatalf("both segment results must be preserved as reached: %+v", status.Results)
	}
	if status.Results[0].ReachedAt.IsZero() || status.Results[1].ReachedAt.Before(status.Results[0].ReachedAt) {
		t.Fatalf("segment arrival ordering is wrong: %+v", status.Results)
	}
}

// 当前段未达标就推进阶段：必须拒绝、留在该段并给出原因，不能跳过控制步骤。
func TestCurveAdvanceRejectedBeforeTargetReached(t *testing.T) {
	state := New()
	campaign := testCampaign()
	if err := state.CommitCurve(campaign, []CurveSegment{{Phase: model.Warming, Target: 0.5}}, false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := state.AdvanceSegment(campaign); err == nil {
		t.Fatal("advance before any control step must be rejected")
	}

	// 中途取消控制循环：当前段保持 held，回流比停在中间值，原因被记录。
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := state.FollowCurve(ctx, campaign, time.Millisecond); done <- err }()
	waitFor(func() bool { r := state.Snapshot().Ratio; return r > 0 && r < 0.5 }, t)
	cancel()
	<-done

	status := state.CurveStatus()
	if status.Active != true || status.Current != 0 {
		t.Fatalf("expected to remain on first segment, got %+v", status)
	}
	if len(status.Results) != 1 || status.Results[0].Status != segHeld || status.Results[0].Reason == "" {
		t.Fatalf("interrupted segment must be held with a reason: %+v", status.Results)
	}
	ratioAfterInterrupt := state.Snapshot().Ratio
	if ratioAfterInterrupt >= 0.5 || ratioAfterInterrupt <= 0 {
		t.Fatalf("ratio should be held midway, got %v", ratioAfterInterrupt)
	}
	if _, _, err := state.AdvanceSegment(campaign); err == nil {
		t.Fatal("advance while segment held must be rejected")
	}

	// 恢复后接着完成本段；之前中断时已走的步数和起点时间保留。
	result, err := state.FollowCurve(context.Background(), campaign, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != segReached || result.Steps != 5 {
		t.Fatalf("expected resumed segment to reach target in 5 total steps, got %+v", result)
	}
}

// 中断第二段时，第一段的到达结果必须保留。
func TestCurveInterruptKeepsPreviousSegmentResult(t *testing.T) {
	state := New()
	campaign := testCampaign()
	if err := state.CommitCurve(campaign, []CurveSegment{
		{Phase: model.Warming, Target: 0.5},
		{Phase: model.Stabilizing, Target: 0.8},
	}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := state.FollowCurve(context.Background(), campaign, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	firstReachedAt := state.CurveStatus().Results[0].ReachedAt
	if _, _, err := state.AdvanceSegment(campaign); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _, _ = state.FollowCurve(ctx, campaign, time.Millisecond) }()
	waitFor(func() bool { r := state.Snapshot().Ratio; return r > 0.5 && r < 0.8 }, t)
	cancel()
	state.Stop()
	waitFor(func() bool { return state.CurveStatus().Results[1].Status == segHeld }, t)

	status := state.CurveStatus()
	if status.Results[0].Status != segReached || !status.Results[0].ReachedAt.Equal(firstReachedAt) {
		t.Fatalf("previous segment result must survive a later segment interruption: %+v", status.Results)
	}
	if status.Results[1].Status != segHeld || status.Reason == "" {
		t.Fatalf("current segment must be held with a reason: %+v", status.Results)
	}
}

func TestCurveCommitValidation(t *testing.T) {
	campaign := testCampaign()
	cases := []struct {
		name     string
		segments []CurveSegment
	}{
		{"empty", nil},
		{"target above limit", []CurveSegment{{Phase: model.Warming, Target: 1.5}}},
		{"negative target", []CurveSegment{{Phase: model.Warming, Target: -0.1}}},
		{"duplicate phase", []CurveSegment{{Phase: model.Warming, Target: 0.4}, {Phase: model.Warming, Target: 0.6}}},
		{"fallback target", []CurveSegment{{Phase: model.Warming, Target: 0.6}, {Phase: model.Stabilizing, Target: 0.4}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := New()
			if err := state.CommitCurve(campaign, tc.segments, false); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}

	state := New()
	if err := state.CommitCurve(campaign, []CurveSegment{{Phase: model.Warming, Target: 0.5}}, false); err != nil {
		t.Fatal(err)
	}
	// 活动曲线不能被新提交整体替换，只能按新阶段追加。
	if err := state.CommitCurve(campaign, []CurveSegment{{Phase: model.Stabilizing, Target: 0.8}}, false); err == nil {
		t.Fatal("replacing an active curve must be rejected")
	}
	if err := state.CommitCurve(campaign, []CurveSegment{{Phase: model.Warming, Target: 0.6}}, true); err == nil {
		t.Fatal("extending with an already submitted phase must be rejected")
	}
	if err := state.CommitCurve(campaign, []CurveSegment{{Phase: model.Stabilizing, Target: 0.4}}, true); err == nil {
		t.Fatal("extending with a lower target must be rejected")
	}
	if err := state.CommitCurve(campaign, []CurveSegment{{Phase: model.Stabilizing, Target: 0.8}}, true); err != nil {
		t.Fatalf("append next phase should succeed: %v", err)
	}
	if got := len(state.CurveStatus().Results); got != 0 {
		t.Fatalf("extending a fresh curve must not fabricate results, got %d", got)
	}
}
