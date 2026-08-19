package application

import (
	"context"
	"time"

	"github.com/acme/distributed-workflow-engine/internal/observability/domain"
)

type Checker interface {
	Name() string
	Check(ctx context.Context) error
}

type Service struct {
	checks []Checker
}

func NewService(checks ...Checker) *Service {
	return &Service{checks: checks}
}

func (s *Service) Health(ctx context.Context) domain.HealthReport {
	report := domain.HealthReport{Status: domain.HealthOK, Now: time.Now().UTC()}
	for _, checker := range s.checks {
		err := checker.Check(ctx)
		check := domain.Check{Name: checker.Name(), Status: domain.HealthOK, CheckedAt: time.Now().UTC()}
		if err != nil {
			check.Status = domain.HealthDown
			check.Message = err.Error()
			report.Status = domain.HealthDown
		}
		report.Checks = append(report.Checks, check)
	}
	return report
}

func (s *Service) Ready(ctx context.Context) domain.HealthReport {
	return s.Health(context.Background())
}
