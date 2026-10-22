package e2e

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/paulovillalobos/audio-jobs/internal/api"
)

// getJSON sends a GET to the API and decodes the JSON body into out (if non-nil).
func (h *harness) getJSON(path, tenant string, out any) int {
	h.t.Helper()
	req, _ := http.NewRequest(http.MethodGet, h.api.URL+path, nil)
	if tenant != "" {
		req.Header.Set("X-Tenant-ID", tenant)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			h.t.Fatalf("GET %s: decode: %v", path, err)
		}
	}
	return resp.StatusCode
}

type listBody struct {
	Jobs []struct {
		ID string `json:"id"`
	} `json:"jobs"`
	NextCursor string `json:"next_cursor"`
}

func TestGetJobIsScopedToTheTenant(t *testing.T) {
	h := newHarness(t)
	res := h.submit("acme", "", wav(20, 200))

	var job struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if code := h.getJSON("/v1/jobs/"+res.Job.ID, "acme", &job); code != http.StatusOK || job.ID != res.Job.ID {
		t.Fatalf("own job: %d %+v", code, job)
	}
	// Another tenant, an unknown id and a malformed id all look the same.
	for _, c := range []struct{ tenant, id string }{
		{"globex", res.Job.ID},
		{"acme", "00000000-0000-0000-0000-000000000000"},
		{"acme", "not-a-uuid"},
	} {
		if code := h.getJSON("/v1/jobs/"+c.id, c.tenant, nil); code != http.StatusNotFound {
			t.Fatalf("GET %s as %s: %d, want 404", c.id, c.tenant, code)
		}
	}
}

func TestSubmitReturnsLocationOfTheNewJob(t *testing.T) {
	h := newHarness(t)
	req, _ := http.NewRequest(http.MethodPost, h.api.URL+"/v1/jobs", bytes.NewReader(wav(21, 200)))
	req.Header.Set("X-Tenant-ID", "acme")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	loc := resp.Header.Get("Location")
	if resp.StatusCode != http.StatusAccepted || !strings.HasPrefix(loc, "/v1/jobs/") {
		t.Fatalf("status %d, Location %q", resp.StatusCode, loc)
	}
	if code := h.getJSON(loc, "acme", nil); code != http.StatusOK {
		t.Fatalf("GET Location: %d", code)
	}

	if r := h.submit("acme", strings.Repeat("k", 201), wav(21, 200)); r.Status != http.StatusBadRequest {
		t.Fatalf("long idempotency key: %+v", r)
	}
}

func TestListEndpointPagesWithCursor(t *testing.T) {
	h := newHarness(t)
	for i := range 3 {
		h.submit("acme", "", wav(uint64(30+i), 200))
	}

	var page1, page2 listBody
	if code := h.getJSON("/v1/jobs?limit=2", "acme", &page1); code != http.StatusOK {
		t.Fatalf("page 1: %d", code)
	}
	if len(page1.Jobs) != 2 || page1.NextCursor == "" {
		t.Fatalf("page 1: %+v", page1)
	}
	if code := h.getJSON("/v1/jobs?limit=2&cursor="+page1.NextCursor, "acme", &page2); code != http.StatusOK {
		t.Fatalf("page 2: %d", code)
	}
	if len(page2.Jobs) != 1 || page2.NextCursor != "" {
		t.Fatalf("page 2: %+v", page2)
	}
	for _, j := range page1.Jobs {
		if j.ID == page2.Jobs[0].ID {
			t.Fatalf("job %s on both pages", j.ID)
		}
	}

	// No jobs is an empty array, not null.
	var empty map[string]json.RawMessage
	if code := h.getJSON("/v1/jobs?status=succeeded", "acme", &empty); code != http.StatusOK || string(empty["jobs"]) != "[]" {
		t.Fatalf("empty list: %d %s", code, empty["jobs"])
	}
}

func TestListEndpointRejectsBadParameters(t *testing.T) {
	h := newHarness(t)
	noSeparator := base64.RawURLEncoding.EncodeToString([]byte("2026-01-01T00:00:00Z"))
	badTime := base64.RawURLEncoding.EncodeToString([]byte("yesterday|abc"))
	for _, c := range []struct{ query, tenant string }{
		{"", ""}, // tenant header is required
		{"?limit=0", "acme"},
		{"?limit=501", "acme"},
		{"?limit=ten", "acme"},
		{"?status=paused", "acme"},
		{"?cursor=%25%25", "acme"},
		{"?cursor=" + noSeparator, "acme"},
		{"?cursor=" + badTime, "acme"},
	} {
		if code := h.getJSON("/v1/jobs"+c.query, c.tenant, nil); code != http.StatusBadRequest {
			t.Fatalf("GET /v1/jobs%s: %d, want 400", c.query, code)
		}
	}
}

func TestReadyzReportsUnavailableDependencies(t *testing.T) {
	h := newHarness(t)
	if code := h.getJSON("/readyz", "", nil); code != http.StatusOK {
		t.Fatalf("readyz: %d", code)
	}

	srv := &api.Server{Store: h.store, Log: h.log,
		RedisPing: func(context.Context) error { return errors.New("connection refused") }}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "redis") {
		t.Fatalf("redis down: %d %s", rec.Code, rec.Body)
	}
}
