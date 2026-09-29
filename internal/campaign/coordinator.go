package campaign

import (
	"context"
	"errors"
	"time"

	"github.com/wyw14/solo-039/internal/feed"
	"github.com/wyw14/solo-039/internal/model"
	"github.com/wyw14/solo-039/internal/reboiler"
	"github.com/wyw14/solo-039/internal/reflux"
)

type Coordinator struct {
	State    *State
	Feed     *feed.State
	Reflux   *reflux.State
	Reboiler *reboiler.State
}

func NewCoordinator(grade string) *Coordinator {
	return &Coordinator{State: New(grade), Feed: feed.New(), Reflux: reflux.New(), Reboiler: reboiler.New()}
}

func (c *Coordinator) Start(ctx context.Context) error {
	if err := c.Feed.Open(ctx, c.State.Snapshot()); err != nil {
		return err
	}
	c.State.Advance(model.Warming)
	if err := c.Reboiler.Ramp(ctx, c.State.Snapshot(), 0.45); err != nil {
		return err
	}
	// 升温阶段只提交并执行曲线的升温段；未达升温段目标前不切换阶段。
	curve := reflux.DefaultCurve()
	if _, err := c.Reflux.RunCurve(ctx, c.State.Snapshot(), curve, time.Millisecond, nil); err != nil {
		return err
	}
	c.State.Advance(model.Stabilizing)
	// 进入稳定阶段后续跑曲线，保留升温段结果，逐段逼近最终回流比。
	if _, err := c.Reflux.ResumeCurve(ctx, c.State.Snapshot(), time.Millisecond, nil); err != nil {
		return err
	}
	return nil
}

func (c *Coordinator) Cancel() { c.State.Stop(); c.Reflux.Stop(); c.Reboiler.Stop() }

func (c *Coordinator) EnsureRunning() error {
	if c.State.Snapshot().Phase == model.Stopped {
		return errors.New("campaign stopped")
	}
	return nil
}
