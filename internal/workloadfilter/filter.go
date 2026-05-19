package workloadfilter

import (
	"strings"

	"github.com/kranix-io/kranix-core/internal/quotaaggregate"
	"github.com/kranix-io/kranix-core/pkg/types"
)

// Query mirrors search query parameters.
type Query struct {
	Namespace   string
	Phase       string
	Status      string
	Image       string
	Team        string
	Environment string
	CostCenter  string
	LabelKey    string
	LabelValue  string
}

// Match returns true when the workload satisfies all non-empty query fields.
func Match(w *types.Workload, q Query) bool {
	if w == nil {
		return false
	}
	if ns := strings.TrimSpace(q.Namespace); ns != "" && !strings.EqualFold(w.Namespace, ns) {
		return false
	}
	phase := strings.TrimSpace(q.Phase)
	if phase == "" {
		phase = strings.TrimSpace(q.Status)
	}
	if phase != "" && !strings.EqualFold(string(w.Status.Phase), phase) {
		return false
	}
	if img := strings.TrimSpace(q.Image); img != "" && !strings.Contains(strings.ToLower(w.Spec.Image), strings.ToLower(img)) {
		return false
	}
	if team := strings.TrimSpace(q.Team); team != "" && !strings.EqualFold(quotaaggregate.TeamLabel(w), team) {
		return false
	}
	if env := strings.TrimSpace(q.Environment); env != "" {
		ev := environment(w)
		if !strings.EqualFold(ev, env) {
			return false
		}
	}
	if cc := strings.TrimSpace(q.CostCenter); cc != "" {
		c := costCenter(w)
		if !strings.EqualFold(c, cc) {
			return false
		}
	}
	if lk := strings.TrimSpace(q.LabelKey); lk != "" {
		if w.Labels == nil {
			return false
		}
		lv := strings.TrimSpace(q.LabelValue)
		if lv != "" {
			return strings.EqualFold(w.Labels[lk], lv)
		}
		_, ok := w.Labels[lk]
		return ok
	}
	return true
}

// Filter returns workloads matching the query.
func Filter(all []*types.Workload, q Query) []*types.Workload {
	out := make([]*types.Workload, 0, len(all))
	for _, w := range all {
		if Match(w, q) {
			out = append(out, w)
		}
	}
	return out
}

func environment(w *types.Workload) string {
	if w.Tags != nil && w.Tags.Environment != "" {
		return w.Tags.Environment
	}
	if w.Labels != nil {
		return w.Labels[types.LabelKeyEnvironment]
	}
	return ""
}

func costCenter(w *types.Workload) string {
	if w.Tags != nil && w.Tags.CostCenter != "" {
		return w.Tags.CostCenter
	}
	if w.Labels != nil {
		return w.Labels[types.LabelKeyCostCenter]
	}
	return ""
}
