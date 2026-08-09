package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/harjeetschahal/flexstore/internal/config"
	"github.com/harjeetschahal/flexstore/internal/observability"
)

func TestDashboardServesTheEmbeddedPage(t *testing.T) {
	h := NewHandler(config.Gateway{}, nil, nil, nil, testLogger(), observability.NewMetrics("test"))
	rec := httptest.NewRecorder()
	h.Dashboard(rec, httptest.NewRequest(http.MethodGet, "/dashboard", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store: a cached page after an upgrade polls endpoints it misunderstands", cc)
	}
	body := rec.Body.String()
	// The page must be self-contained: an offline laptop and a strict network
	// both have to render it, so any external fetch is a regression.
	for _, banned := range []string{"http://cdn", "https://cdn", "src=\"http", "href=\"https://fonts"} {
		if strings.Contains(body, banned) {
			t.Errorf("dashboard references an external asset (%q); it must be self-contained", banned)
		}
	}
	for _, want := range []string{"FlexStore", "/admin/nodes", "/admin/replication", "/admin/stats"} {
		if !strings.Contains(body, want) {
			t.Errorf("dashboard page is missing %q", want)
		}
	}
}

func TestAdminStatsExposesGatewayCounters(t *testing.T) {
	m := observability.NewMetrics("test")
	h := NewHandler(config.Gateway{}, nil, nil, nil, testLogger(), m)

	m.UploadBytesTotal.Add(1048576)
	m.DownloadBytesTotal.Add(2097152)

	rec := httptest.NewRecorder()
	h.AdminStats(rec, httptest.NewRequest(http.MethodGet, "/admin/stats", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var out struct {
		Timestamp     string  `json:"timestamp"`
		UploadBytes   float64 `json:"upload_bytes_total"`
		DownloadBytes float64 `json:"download_bytes_total"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decoding: %v\n%s", err, rec.Body.String())
	}
	if out.UploadBytes != 1048576 || out.DownloadBytes != 2097152 {
		t.Fatalf("counters = %v/%v, want 1048576/2097152", out.UploadBytes, out.DownloadBytes)
	}
	if out.Timestamp == "" {
		t.Error("timestamp missing")
	}
}
