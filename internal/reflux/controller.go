package reflux

import (
	"context"
	"github.com/wyw14/solo-039/internal/model"
	"time"
)

type Controller struct {
	State     *State
	Tick      time.Duration
	OnSegment SegmentHook
}

func NewController() *Controller { return &Controller{State: New(), Tick: time.Millisecond} }

func (c *Controller) Ramp(ctx context.Context, campaign model.Campaign, target float64) error {
	return c.State.Ramp(ctx, campaign, target, c.Tick)
}

// RunCurve 提交按批次阶段划分的回流目标曲线。
func (c *Controller) RunCurve(ctx context.Context, campaign model.Campaign, curve Curve) (CurveProgress, error) {
	return c.State.RunCurve(ctx, campaign, curve, c.Tick, c.OnSegment)
}

// ResumeCurve 在批次阶段切换后续跑曲线，保留上一段反馈结果。
func (c *Controller) ResumeCurve(ctx context.Context, campaign model.Campaign) (CurveProgress, error) {
	return c.State.ResumeCurve(ctx, campaign, c.Tick, c.OnSegment)
}

// CurveProgress 返回最近一次提交曲线的执行进度。
func (c *Controller) CurveProgress() (CurveProgress, bool) { return c.State.Curve() }

func (c *Controller) Stop() { c.State.Stop() }
