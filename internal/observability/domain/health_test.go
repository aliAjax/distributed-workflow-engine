package domain

import "testing"

func TestHealthReportWithCheck(t *testing.T) {
	report := HealthReport{Status: HealthOK}
	updated := report.WithCheck(Check{Name: "postgres", Status: HealthDown})
	if len(updated.Checks) != 1 {
		t.Fatalf("expected one check, got %d", len(updated.Checks))
	}
	if updated.Status != HealthDown {
		t.Fatalf("expected status down, got %s", updated.Status)
	}
}
