package httpapi

import (
	"context"
	"strings"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	observabilityapplication "github.com/acme/distributed-workflow-engine/internal/observability/application"
)

type fakeChecker struct {
	err error
}

func (c fakeChecker) Name() string                       { return "fake" }
func (c fakeChecker) Check(context.Context) error         { return c.err }

func TestHTTPRequestTimeoutCancellation(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(parent)
	rec := httptest.NewRecorder()
	server := &Server{}
	handler := server.withTimeout(time.Second)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Context().Err() == nil {
			t.Error("expected parent context cancellation to propagate")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	handler.ServeHTTP(rec, req)
}

func TestHTTPWriteJSONStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSON(rec, http.StatusCreated, map[string]string{"ok": "yes"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d", http.StatusCreated, rec.Code)
	}
}

func TestHTTPStatusRecorderWritesStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	recorder := &statusRecorder{ResponseWriter: rec, status: http.StatusOK}
	recorder.WriteHeader(http.StatusTeapot)
	if recorder.status != http.StatusTeapot {
		t.Fatalf("expected status %d, got %d", http.StatusTeapot, recorder.status)
	}
}

func TestHTTPReadyDownStatus(t *testing.T) {
	server := &Server{health: observabilityapplication.NewService(fakeChecker{err: errors.New("down")})}
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	server.handleReady(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d", http.StatusServiceUnavailable, rec.Code)
	}
}

func TestHTTPPaginationClampsNegativeOffset(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/?offset=-5", nil)
	_, offset := pagination(req)
	if offset != 0 {
		t.Fatalf("expected negative offset to clamp to 0, got %d", offset)
	}
}

func TestHTTPPaginationClampsZeroLimit(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/?limit=0", nil)
	limit, _ := pagination(req)
	if limit != 50 {
		t.Fatalf("expected zero limit to clamp to 50, got %d", limit)
	}
}

func TestHTTPDecodeJSONRejectsUnknownFields(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"known":true,"extra":1}`))
	var target struct {
		Known bool `json:"known"`
	}
	if err := decodeJSON(req, &target); err == nil {
		t.Fatal("expected unknown fields to be rejected")
	}
}
