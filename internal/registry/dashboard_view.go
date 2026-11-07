package registry

import (
	"sort"
	"strings"
)

// GetRegisteredModelInfos includes temporarily unavailable models so an
// observability view never loses a pinned model just because it is cooling.
// Returned metadata is detached from the registry and contains no client IDs.
func (r *ModelRegistry) GetRegisteredModelInfos() []*ModelInfo {
	if r == nil {
		return []*ModelInfo{}
	}
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	models := make([]*ModelInfo, 0, len(r.models))
	for _, registration := range r.models {
		if registration != nil && registration.Info != nil {
			models = append(models, cloneModelInfo(registration.Info))
		}
	}
	sort.Slice(models, func(i, j int) bool {
		return strings.TrimSpace(models[i].ID) < strings.TrimSpace(models[j].ID)
	})
	return models
}
