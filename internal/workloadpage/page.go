package workloadpage

import (
	"github.com/kranix-io/kranix-core/pkg/types"
	"github.com/kranix-io/kranix-packages/pagination"
)

// workloadID adapts core Workload for generic pagination.
type workloadID struct{ *types.Workload }

func (w workloadID) GetID() string {
	if w.Workload == nil {
		return ""
	}
	return w.Workload.ID
}

// Paginate applies cursor pagination to workloads (sorted by id ascending).
func Paginate(all []*types.Workload, p pagination.Params) ([]*types.Workload, pagination.PageInfo) {
	wrapped := make([]pagination.IDProvider, len(all))
	for i, w := range all {
		wrapped[i] = workloadID{w}
	}
	page, info := pagination.SlicePage(wrapped, p)
	out := make([]*types.Workload, len(page))
	for i, w := range page {
		out[i] = w.(workloadID).Workload
	}
	return out, info
}
