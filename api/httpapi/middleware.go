package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	tenantdomain "github.com/acme/distributed-workflow-engine/internal/tenant/domain"
)

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (s *Server) withRequestContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := newRequestID()
		ctx := context.WithValue(r.Context(), requestIDKey{}, requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.log.Error(r.Context(), "http panic", "panic", recovered, "stack", string(debug.Stack()))
				writeError(w, http.StatusInternalServerError, fmt.Errorf("internal server error"))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) withTimeout(timeout time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func (s *Server) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		s.log.Info(r.Context(), "http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", recorder.status,
			"duration_ms", time.Since(start).Milliseconds(),
			"request_id", requestIDFrom(r.Context()),
		)
	})
}

func (s *Server) withMetrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		status := fmt.Sprintf("%d", recorder.status)
		s.metrics.HTTPRequests.WithLabelValues(r.Method, r.URL.Path, status).Inc()
		s.metrics.HTTPDuration.WithLabelValues(r.Method, r.URL.Path).Observe(time.Since(start).Seconds())
	})
}

func (s *Server) withAuth(resource, action string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := s.authenticate(r)
		if err != nil {
			writeError(w, http.StatusUnauthorized, err)
			return
		}
		if err := s.tenants.Authorize(principal, resource, action); err != nil {
			writeError(w, http.StatusForbidden, err)
			return
		}
		next.ServeHTTP(w, r.WithContext(withPrincipal(r.Context(), principal)))
	})
}

func (s *Server) authenticate(r *http.Request) (tenantdomain.Principal, error) {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		return tenantdomain.Principal{}, fmt.Errorf("authorization bearer token required")
	}
	token := strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
	if token == "" {
		return tenantdomain.Principal{}, fmt.Errorf("authorization bearer token required")
	}
	if token == s.cfg.Auth.BootstrapToken && s.bootstrapPrincipal.TenantID != "" {
		return s.bootstrapPrincipal, nil
	}
	principal, err := s.tenants.Authenticate(r.Context(), token)
	if err != nil {
		return tenantdomain.Principal{}, fmt.Errorf("authenticate api key: %w", err)
	}
	return principal, nil
}

func (s *Server) withGlobalRateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		allowed, err := s.policy.RateLimit(r.Context(), "workflow:rate:http", 120, time.Minute)
		if err != nil || !allowed {
			writeError(w, http.StatusTooManyRequests, fmt.Errorf("rate limit exceeded"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func newRequestID() string {
	raw := make([]byte, 12)
	if _, err := rand.Read(raw); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(raw)
}

type requestIDKey struct{}

func requestIDFrom(ctx context.Context) string {
	value, _ := ctx.Value(requestIDKey{}).(string)
	return value
}
