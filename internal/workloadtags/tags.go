package workloadtags

import (
	"strings"

	"github.com/kranix-io/kranix-core/pkg/types"
)

// ListFilter selects workloads by structured tags (all non-empty fields must match).
type ListFilter struct {
	Team        string
	Environment string
	CostCenter  string
}

// Apply copies structured tags into workload labels for runtimes and billing exports.
func Apply(w *types.Workload) {
	if w == nil {
		return
	}
	if w.Tags == nil {
		SyncFromLabels(w)
		return
	}
	if w.Labels == nil {
		w.Labels = make(map[string]string)
	}
	if t := strings.TrimSpace(w.Tags.Team); t != "" {
		w.Labels[types.LabelKeyTeam] = t
	}
	if e := strings.TrimSpace(w.Tags.Environment); e != "" {
		w.Labels[types.LabelKeyEnvironment] = e
	}
	if c := strings.TrimSpace(w.Tags.CostCenter); c != "" {
		w.Labels[types.LabelKeyCostCenter] = c
	}
	for k, v := range w.Tags.Custom {
		k = strings.TrimSpace(k)
		if k == "" {
			continue
		}
		w.Labels[k] = strings.TrimSpace(v)
	}
}

// SyncFromLabels populates Tags from well-known label keys when Tags is nil or partially empty.
func SyncFromLabels(w *types.Workload) {
	if w == nil || w.Labels == nil {
		return
	}
	if w.Tags == nil {
		w.Tags = &types.WorkloadTags{}
	}
	if w.Tags.Team == "" {
		w.Tags.Team = strings.TrimSpace(w.Labels[types.LabelKeyTeam])
	}
	if w.Tags.Environment == "" {
		w.Tags.Environment = strings.TrimSpace(w.Labels[types.LabelKeyEnvironment])
	}
	if w.Tags.CostCenter == "" {
		w.Tags.CostCenter = strings.TrimSpace(w.Labels[types.LabelKeyCostCenter])
	}
}

// Team returns the team identifier from tags or labels.
func Team(w *types.Workload) string {
	if w == nil {
		return ""
	}
	if w.Tags != nil && strings.TrimSpace(w.Tags.Team) != "" {
		return strings.TrimSpace(w.Tags.Team)
	}
	if w.Labels != nil {
		return strings.TrimSpace(w.Labels[types.LabelKeyTeam])
	}
	return ""
}

// Matches reports whether the workload satisfies the filter.
func Matches(w *types.Workload, f ListFilter) bool {
	if w == nil {
		return false
	}
	SyncFromLabels(w)
	if t := strings.TrimSpace(f.Team); t != "" && !strings.EqualFold(Team(w), t) {
		return false
	}
	if e := strings.TrimSpace(f.Environment); e != "" {
		env := ""
		if w.Tags != nil {
			env = w.Tags.Environment
		}
		if env == "" && w.Labels != nil {
			env = w.Labels[types.LabelKeyEnvironment]
		}
		if !strings.EqualFold(strings.TrimSpace(env), e) {
			return false
		}
	}
	if c := strings.TrimSpace(f.CostCenter); c != "" {
		cc := ""
		if w.Tags != nil {
			cc = w.Tags.CostCenter
		}
		if cc == "" && w.Labels != nil {
			cc = w.Labels[types.LabelKeyCostCenter]
		}
		if !strings.EqualFold(strings.TrimSpace(cc), c) {
			return false
		}
	}
	return true
}

// Filter returns workloads matching all set filter fields.
func Filter(workloads []*types.Workload, f ListFilter) []*types.Workload {
	if strings.TrimSpace(f.Team) == "" && strings.TrimSpace(f.Environment) == "" && strings.TrimSpace(f.CostCenter) == "" {
		return workloads
	}
	out := make([]*types.Workload, 0, len(workloads))
	for _, w := range workloads {
		if Matches(w, f) {
			out = append(out, w)
		}
	}
	return out
}
