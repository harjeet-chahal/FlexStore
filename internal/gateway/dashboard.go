package gateway

import (
	_ "embed"
	"net/http"
	"time"
)

// The live dashboard is one self-contained HTML page embedded in the binary
// and served by the gateway itself. No build step, no CDN, no extra container:
// it polls the same read-only /admin/* endpoints an operator would curl, so it
// can never show a state the API would not also report — the same honesty rule
// the demo script follows.
//
//go:embed dashboard.html
var dashboardHTML []byte

// Dashboard implements GET /dashboard.
func (h *Handler) Dashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// The page is versioned with the binary; a stale cached copy after an
	// upgrade would poll endpoints it misunderstands.
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(dashboardHTML)
}

// AdminStats implements GET /admin/stats: the gateway's own traffic counters,
// read from the in-process Prometheus registry.
//
// It exists because the dashboard runs in a browser on the API origin
// (:8080), while the metrics listener is a different origin (:9101) that
// deliberately sends no CORS headers — the admin port is private surface.
// Rather than loosening that, the two counters the dashboard needs are
// re-exposed here, same-origin and read-only. Rates are computed client-side
// from successive samples.
func (h *Handler) AdminStats(w http.ResponseWriter, r *http.Request) {
	out := struct {
		Timestamp     string  `json:"timestamp"`
		UploadBytes   float64 `json:"upload_bytes_total"`
		DownloadBytes float64 `json:"download_bytes_total"`
		HTTPRequests  float64 `json:"http_requests_total"`
		InFlight      float64 `json:"http_requests_in_flight"`
	}{Timestamp: time.Now().UTC().Format(time.RFC3339Nano)}

	families, err := h.m.Registry.Gather()
	if err != nil {
		h.fail(w, r, err)
		return
	}
	for _, mf := range families {
		var sum float64
		for _, m := range mf.GetMetric() {
			switch {
			case m.GetCounter() != nil:
				sum += m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				sum += m.GetGauge().GetValue()
			}
		}
		switch mf.GetName() {
		case "flexstore_upload_bytes_total":
			out.UploadBytes = sum
		case "flexstore_download_bytes_total":
			out.DownloadBytes = sum
		case "flexstore_http_requests_total":
			out.HTTPRequests = sum
		case "flexstore_http_requests_in_flight":
			out.InFlight = sum
		}
	}
	writeJSON(w, http.StatusOK, out, h.log)
}
