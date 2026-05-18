package cronsched

import (
	"fmt"
	"strings"
	"time"

	"github.com/kranix-io/kranix-core/pkg/types"
	"github.com/robfig/cron/v3"
)

// Evaluator parses workload cron specs and evaluates whether a reconcile should trigger a deploy.
type Evaluator struct{}

// ShouldTriggerDeploy returns whether the workload cron is due for a scheduler run now.
func (e *Evaluator) ShouldTriggerDeploy(w *types.Workload, now time.Time) (bool, error) {
	if w == nil || w.Spec.CronSchedule == nil || strings.TrimSpace(w.Spec.CronSchedule.Schedule) == "" {
		return true, nil
	}
	spec := w.Spec.CronSchedule
	if spec.Suspended {
		return false, nil
	}

	sched, location, err := parseSchedule(spec)
	if err != nil {
		return false, err
	}
	now = now.In(location)

	anchor := time.Unix(0, 0).In(location)
	if w.Status.Cron != nil && w.Status.Cron.LastScheduleTime != nil {
		anchor = w.Status.Cron.LastScheduleTime.In(location)
	}
	next := sched.Next(anchor)

	if next.After(now) {
		return false, nil
	}

	cp := strings.ToLower(strings.TrimSpace(spec.ConcurrencyPolicy))
	if cp == "" {
		cp = "allow"
	}
	if cp == "forbid" && (w.Status.Phase == types.WorkloadPhaseRunning || w.Status.Phase == types.WorkloadPhaseDegraded) {
		return false, nil
	}
	return true, nil
}

func parseSchedule(spec *types.CronScheduleSpec) (cron.Schedule, *time.Location, error) {
	loc := time.UTC
	tz := strings.TrimSpace(spec.TimeZone)
	if tz != "" && strings.ToUpper(tz) != "UTC" && strings.ToUpper(tz) != "GMT" {
		l, err := time.LoadLocation(tz)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid cron time_zone %q: %w", tz, err)
		}
		loc = l
	}
	schedule, err := cron.ParseStandard(strings.TrimSpace(spec.Schedule))
	if err != nil {
		return nil, nil, fmt.Errorf("invalid cron schedule %q: %w", spec.Schedule, err)
	}
	return schedule, loc, nil
}

// Validate parses the cron schedule for admission checks.
func Validate(spec *types.CronScheduleSpec) error {
	if spec == nil {
		return nil
	}
	if strings.TrimSpace(spec.Schedule) == "" {
		return fmt.Errorf("cron schedule is empty")
	}
	_, _, err := parseSchedule(spec)
	return err
}
