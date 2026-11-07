package auth

import (
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
)

// DashboardRoute is a public, credential-free projection of a scheduling tier.
type DashboardRoute struct {
	Provider         string     `json:"provider"`
	ProviderLabel    string     `json:"provider_label"`
	Priority         int        `json:"priority"`
	ReadyCredentials int        `json:"ready_credentials"`
	Status           string     `json:"status"`
	CooldownReason   string     `json:"cooldown_reason,omitempty"`
	RetryAt          *time.Time `json:"retry_at,omitempty"`
}

type DashboardModel struct {
	ID               string           `json:"id"`
	Label            string           `json:"label"`
	Routes           []DashboardRoute `json:"routes"`
	LastUsedProvider string           `json:"last_used_provider,omitempty"`
}

type DashboardRoutes struct {
	ObservedAt   time.Time        `json:"observed_at"`
	RuntimeKnown bool             `json:"runtime_known"`
	Models       []DashboardModel `json:"models"`
}

// DashboardProviderGroup exposes the configured provider group, never a
// credential label, account identity, endpoint, or authentication material.
// Request completions use the same grouping as the route snapshot.
func DashboardProviderGroup(auth *Auth, fallbackProvider string) (string, string) {
	provider := strings.ToLower(strings.TrimSpace(fallbackProvider))
	if auth != nil && strings.TrimSpace(auth.Provider) != "" {
		provider = strings.ToLower(strings.TrimSpace(auth.Provider))
		if compat := strings.TrimSpace(auth.Attributes["compat_name"]); compat != "" {
			return executorKeyFromAuth(auth), compat
		}
		if provider == "openai-compatibility" {
			// A compatibility credential's Label is not guaranteed to be public.
			// Config-created credentials carry compat_name above; otherwise use
			// the stable executor group without exposing the raw label.
			return "openai-compatibility", "OpenAI compatible"
		}
		kind := auth.AuthKind()
		switch {
		case provider == "claude" && kind == AuthKindOAuth:
			return "claude-oauth", "Claude OAuth"
		case provider == "claude" && kind == AuthKindAPIKey:
			return "claude-api", "Anthropic API"
		case provider == "codex" && kind == AuthKindOAuth:
			return "codex-oauth", "Codex OAuth"
		case provider == "codex" && kind == AuthKindAPIKey:
			return "codex-api", "OpenAI API"
		}
	}
	switch provider {
	case "claude":
		return provider, "Claude"
	case "claude-oauth":
		return provider, "Claude OAuth"
	case "claude-api":
		return provider, "Anthropic API"
	case "codex":
		return provider, "Codex"
	case "codex-oauth":
		return provider, "Codex OAuth"
	case "codex-api", "openai":
		return provider, "OpenAI API"
	case "gemini":
		return provider, "Gemini"
	case "xai":
		return provider, "xAI"
	case "":
		return "unknown", "Unknown provider"
	}
	label := []rune(provider)
	if len(label) > 0 {
		label[0] = unicode.ToUpper(label[0])
	}
	return provider, string(label)
}

// DashboardRoutesSnapshot reads detached auths and registry metadata. It does
// not invoke a selector, scheduler plugin, model pool, refresh, or Home dispatch.
// Eligibility follows the scheduler's default request policy; session affinity,
// request hints, custom schedulers, and remote Home state can change selection.
func (m *Manager) DashboardRoutesSnapshot(now time.Time) DashboardRoutes {
	result := DashboardRoutes{ObservedAt: now.UTC(), Models: []DashboardModel{}}
	reg := registry.GetGlobalRegistry()
	infos := reg.GetRegisteredModelInfos()
	var auths []*Auth
	executors := map[string]bool{}
	weighted := false
	if m != nil {
		m.mu.RLock()
		for _, auth := range m.auths {
			if auth != nil {
				auths = append(auths, auth.Clone())
				key := executorKeyFromAuth(auth)
				_, executors[key] = m.executorLocked(key)
			}
		}
		selector := m.selector
		if affinity, ok := selector.(*SessionAffinitySelector); ok && affinity != nil {
			selector = affinity.fallback
		}
		weighted = selectorStrategy(selector) == schedulerStrategyWeightedRoundRobin
		result.RuntimeKnown = isBuiltInSelector(selector)
		m.mu.RUnlock()
		result.RuntimeKnown = result.RuntimeKnown && !m.HomeEnabled() && !m.hasPluginScheduler()
	}
	sort.Slice(auths, func(i, j int) bool { return auths[i].ID < auths[j].ID })
	for _, info := range infos {
		id := strings.TrimSpace(info.ID)
		if id == "" {
			continue
		}
		model := DashboardModel{ID: id, Label: strings.TrimSpace(info.DisplayName), Routes: []DashboardRoute{}}
		if model.Label == "" {
			model.Label = id
		}
		tiers := map[string]*dashboardRouteTier{}
		bestPriority, hasReady := 0, false
		unknownPriority, hasUnknown := 0, false
		for _, auth := range auths {
			if !m.authSupportsRouteModel(reg, auth, id) {
				continue
			}
			provider, label := DashboardProviderGroup(auth, auth.Provider)
			priority := authPriority(auth)
			key := provider + "\x00" + strconv.Itoa(priority)
			tier := tiers[key]
			if tier == nil {
				tier = &dashboardRouteTier{route: DashboardRoute{Provider: provider, ProviderLabel: label, Priority: priority}}
				tiers[key] = tier
			}
			blocked, reason, retry, checkModel := m.dashboardAuthBlocked(auth, id, now)
			if blocked || !executors[executorKeyFromAuth(auth)] || (weighted && authWeight(auth) <= 0) {
				if reason != blockReasonDisabled && retry.After(now) {
					if tier.route.RetryAt == nil || retry.Before(*tier.route.RetryAt) {
						retryUTC := retry.UTC()
						tier.route.RetryAt = &retryUTC
						tier.route.CooldownReason = dashboardCooldownReason(auth, checkModel, retry, now)
					}
				}
				continue
			}
			if !result.RuntimeKnown || auth.Status == StatusUnknown || auth.Status == StatusPending {
				tier.unknown = true
				if !hasUnknown || priority > unknownPriority {
					unknownPriority, hasUnknown = priority, true
				}
				continue
			}
			tier.route.ReadyCredentials++
			if !hasReady || priority > bestPriority {
				bestPriority, hasReady = priority, true
			}
		}
		for _, tier := range tiers {
			row := tier.route
			switch {
			case row.ReadyCredentials > 0 && hasUnknown && (!hasReady || unknownPriority > bestPriority):
				// A higher-priority unknown candidate can change which tier the
				// default scheduler selects. Do not promote a fallback prematurely.
				row.Status = "unknown"
				row.ReadyCredentials = 0
			case row.ReadyCredentials > 0 && row.Priority == bestPriority:
				row.Status = "ready"
			case row.ReadyCredentials > 0:
				row.Status = "standby"
			case tier.unknown || !result.RuntimeKnown:
				row.Status = "unknown"
			case row.RetryAt != nil:
				row.Status = "cooling"
			default:
				row.Status = "unavailable"
			}
			// A ready tier may contain other cooling credentials; retain their
			// timer as context without labelling the entire route as blocked.
			model.Routes = append(model.Routes, row)
		}
		sort.Slice(model.Routes, func(i, j int) bool {
			a, b := model.Routes[i], model.Routes[j]
			if a.Priority != b.Priority {
				return a.Priority > b.Priority
			}
			return a.Provider < b.Provider
		})
		result.Models = append(result.Models, model)
	}
	return result
}

type dashboardRouteTier struct {
	route   DashboardRoute
	unknown bool
}

func dashboardCooldownReason(auth *Auth, model string, retry, now time.Time) string {
	for _, view := range CooldownSnapshotForAuth(auth, now) {
		if (view.Scope == "credential" || canonicalModelKey(view.ModelKey) == canonicalModelKey(model)) && view.RetryAt.Equal(retry) {
			return view.Reason
		}
	}
	return "unknown"
}

// dashboardAuthBlocked also applies execution's pool-member availability gate,
// but deliberately avoids executionModelCandidatesWithAlias: that method rotates
// the pool as part of dispatch. A read-only view needs only an eligible member.
func (m *Manager) dashboardAuthBlocked(auth *Auth, routeModel string, now time.Time) (bool, blockReason, time.Time, string) {
	checkModel := m.selectionModelForAuth(auth, routeModel)
	if blocked, reason, retry := isAuthBlockedForModel(auth, checkModel, now); blocked {
		return blocked, reason, retry, checkModel
	}
	routing := m.loadAPIKeyModelRouting()
	requested := rewriteModelForAuth(routeModel, auth)
	alias := m.resolveExecutionAliasResultForRequestedWithRouting(routing, auth, requested)
	upstream := executionAliasPoolModel(auth, requested, alias)
	candidates := resolveOpenAICompatUpstreamModelPool(routing.config, auth, upstream)
	if len(candidates) == 0 {
		resolved := m.applyAPIKeyModelAliasWithRouting(routing, auth, upstream)
		if strings.TrimSpace(resolved) == "" {
			resolved = upstream
		}
		candidates = []string{resolved}
	}
	pooled := len(candidates) > 1
	var earliest time.Time
	bestReason := blockReasonOther
	for _, candidate := range candidates {
		stateModel := m.stateModelForExecution(auth, routeModel, candidate, pooled)
		blocked, reason, retry := isAuthBlockedForModel(auth, stateModel, now)
		if !blocked {
			return false, blockReasonNone, time.Time{}, stateModel
		}
		if retry.After(now) && (earliest.IsZero() || retry.Before(earliest)) {
			earliest, bestReason, checkModel = retry, reason, stateModel
		}
	}
	return true, bestReason, earliest, checkModel
}
