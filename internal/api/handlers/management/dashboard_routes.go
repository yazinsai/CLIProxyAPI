package management

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// GetDashboardRoutes exposes only a sanitized read-only default-policy view.
func (h *Handler) GetDashboardRoutes(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	snapshot := h.authManager.DashboardRoutesSnapshot(time.Now())
	lastProviders := h.dashboardStore.LatestProviders()
	for index := range snapshot.Models {
		snapshot.Models[index].LastUsedProvider = lastProviders[snapshot.Models[index].ID]
	}
	c.JSON(http.StatusOK, snapshot)
}
