package reflux

import (
	"context"
	"github.com/wyw14/solo-039/internal/model"
	"time"
)

type Controller struct {
	State *State
	Tick  time.Duration
}

func NewController() *Controller { return &Controller{State: New(), Tick: time.Millisecond} }

func (c *Controller) Ramp(ctx context.Context, campaign model.Campaign, target float64) error {
	return c.State.Ramp(ctx, campaign, target, c.Tick)
}

// CommitCurve 按批次命令代次提交分段回流目标曲线。
func (c *Controller) CommitCurve(campaign model.Campaign, segments []CurveSegment, extend bool) error {
	return c.State.CommitCurve(campaign, segments, extend)
}

// FollowCurve 逐步跟踪当前段直到到达，不会自动进入下一段。
func (c *Controller) FollowCurve(ctx context.Context, campaign model.Campaign) (SegmentResult, error) {
	return c.State.FollowCurve(ctx, campaign, c.Tick)
}

// AdvanceSegment 在当前段达标后才允许阶段切换，否则留在当前段并返回原因。
func (c *Controller) AdvanceSegment(campaign model.Campaign) (CurveSegment, bool, error) {
	return c.State.AdvanceSegment(campaign)
}

func (c *Controller) CurveStatus() CurveStatus { return c.State.CurveStatus() }

func (c *Controller) Stop() { c.State.Stop() }
