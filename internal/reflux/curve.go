package reflux

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/wyw14/solo-039/internal/model"
)

const (
	// stepSize 是每个控制周期允许的最大回流比变化，任何一段都只能按这个步长逼近目标。
	stepSize = 0.1
	// defaultTolerance 是段目标的默认到达判定带宽。
	defaultTolerance = 0.001
)

// 段状态：tracking 正在跟踪，reached 已达标，held 未达标被留在该段。
const (
	segTracking = "tracking"
	segReached  = "reached"
	segHeld     = "held"
)

// CurveSegment 是按批次阶段提交的一段回流目标曲线。
type CurveSegment struct {
	Phase     model.Phase `json:"phase"`
	Target    float64     `json:"target"`
	Tolerance float64     `json:"tolerance,omitempty"`
}

// SegmentResult 记录一段的实际执行结果，阶段切换后已完成段的结果继续保留。
type SegmentResult struct {
	Epoch     uint64      `json:"epoch"`
	Phase     model.Phase `json:"phase"`
	Target    float64     `json:"target"`
	Tolerance float64     `json:"tolerance"`
	StartedAt time.Time   `json:"started_at"`
	ReachedAt time.Time   `json:"reached_at,omitempty"`
	ArrivalMS int64       `json:"arrival_ms"`
	Actual    float64     `json:"actual"`
	Deviation float64     `json:"deviation"`
	Steps     int         `json:"steps"`
	Status    string      `json:"status"`
	Reason    string      `json:"reason,omitempty"`
}

// Curve 是一次提交生效的分段目标曲线。
type Curve struct {
	Epoch    uint64
	Segments []CurveSegment
}

// CurveStatus 是曲线跟踪的对外状态。
type CurveStatus struct {
	Active       bool            `json:"active"`
	Epoch        uint64          `json:"epoch"`
	Current      int             `json:"current"`
	CurrentPhase model.Phase     `json:"current_phase,omitempty"`
	Total        int             `json:"total"`
	Ratio        float64         `json:"ratio"`
	Complete     bool            `json:"complete"`
	Reason       string          `json:"reason,omitempty"`
	Results      []SegmentResult `json:"results"`
}

// ValidateCurve 校验分段曲线：必须有段、阶段有序不重复、目标在操作限值内、容差非负。
func ValidateCurve(segments []CurveSegment) error {
	if len(segments) == 0 {
		return errors.New("reflux curve requires at least one segment")
	}
	maxRatio := model.DefaultLimits().MaxReflux
	seen := make(map[model.Phase]bool, len(segments))
	previous := -1.0
	for i, segment := range segments {
		if segment.Phase == "" {
			return fmt.Errorf("reflux curve segment %d is missing a phase", i)
		}
		if seen[segment.Phase] {
			return fmt.Errorf("reflux curve phase %s is submitted twice", segment.Phase)
		}
		seen[segment.Phase] = true
		if math.IsNaN(segment.Target) || segment.Target < 0 || segment.Target > maxRatio {
			return fmt.Errorf("reflux curve segment %d target %.3f is outside operating limits", i, segment.Target)
		}
		if segment.Target < previous {
			return fmt.Errorf("reflux curve segment %d target %.3f falls back from %.3f", i, segment.Target, previous)
		}
		previous = segment.Target
		if segment.Tolerance < 0 || math.IsNaN(segment.Tolerance) {
			return fmt.Errorf("reflux curve segment %d tolerance is invalid", i)
		}
	}
	return nil
}

// withDefaults 填充默认容差并返回副本。
func withDefaults(segments []CurveSegment) []CurveSegment {
	out := make([]CurveSegment, len(segments))
	for i, segment := range segments {
		out[i] = segment
		if out[i].Tolerance == 0 {
			out[i].Tolerance = defaultTolerance
		}
	}
	return out
}
