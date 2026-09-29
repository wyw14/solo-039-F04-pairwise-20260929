package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/wyw14/solo-039/internal/model"
	"github.com/wyw14/solo-039/internal/reflux"
)

// defaultRefluxCurve 是批次启动时按阶段提交的回流目标曲线：
// 升温段先到中间值，稳定段再到最终值，避免回流比在阶段切换瞬间跳变。
func defaultRefluxCurve() []reflux.CurveSegment {
	return []reflux.CurveSegment{
		{Phase: model.Warming, Target: 0.5},
		{Phase: model.Stabilizing, Target: 0.8},
	}
}

// driveRefluxSegment 对曲线当前段执行逐步控制并记录反馈。
// 它只跟踪当前段：段目标到达后返回，阶段推进由调用方经 AdvanceSegment 把关。
func (p *Plant) driveRefluxSegment(ctx context.Context) error {
	current := p.Campaign.Snapshot()
	result, err := p.RefluxState.FollowCurve(ctx, current, refluxControlTick)
	p.RefluxTelemetry.Record(p.RefluxState.Snapshot())
	if err != nil {
		status := p.RefluxState.CurveStatus()
		reason := status.Reason
		if reason == "" {
			reason = err.Error()
		}
		p.Incidents.Open("reflux-segment", reason, current.Revision.String())
		return fmt.Errorf("reflux segment %s left unmet: %w", current.Phase, err)
	}
	if err := p.record("reflux.segment_reached", fmt.Sprintf("%s:target=%.3f:actual=%.3f:deviation=%+.3f:arrival_ms=%d:steps=%d",
		result.Phase, result.Target, result.Actual, result.Deviation, result.ArrivalMS, result.Steps)); err != nil {
		return err
	}
	return nil
}

// SubmitRefluxCurve 按当前批次代次提交（或追加）分段回流目标曲线，并立即逐步跟踪当前段。
// 当前段未达标时不会进入下一段，原因可在返回状态中看到。
func (p *Plant) SubmitRefluxCurve(segments []reflux.CurveSegment, extend bool) (reflux.CurveStatus, error) {
	current := p.Campaign.Snapshot()
	if current.Phase == model.Stopped {
		return reflux.CurveStatus{}, errors.New("campaign stopped")
	}
	if err := p.RefluxState.CommitCurve(current, segments, extend); err != nil {
		return reflux.CurveStatus{}, err
	}
	if err := p.record("reflux.curve_submitted", fmt.Sprintf("segments=%d:extend=%t", len(segments), extend)); err != nil {
		return reflux.CurveStatus{}, err
	}
	status := p.RefluxState.CurveStatus()
	return status, nil
}

// DriveRefluxSegment 驱动曲线当前段到达目标，供提交后继续控制使用。
func (p *Plant) DriveRefluxSegment(ctx context.Context) (reflux.CurveStatus, error) {
	if err := p.driveRefluxSegment(ctx); err != nil {
		return p.RefluxState.CurveStatus(), err
	}
	return p.RefluxState.CurveStatus(), nil
}

// AdvanceRefluxSegment 在阶段切换前调用：当前段未达标就拒绝切换并留在该段。
func (p *Plant) AdvanceRefluxSegment() (reflux.CurveStatus, error) {
	if _, _, err := p.RefluxState.AdvanceSegment(p.Campaign.Snapshot()); err != nil {
		return p.RefluxState.CurveStatus(), err
	}
	return p.RefluxState.CurveStatus(), nil
}

func (p *Plant) RefluxCurveStatus() reflux.CurveStatus { return p.RefluxState.CurveStatus() }
