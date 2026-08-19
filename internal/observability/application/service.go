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
		if err := ctx.Err(); err != nil {
			report.Status = domain.HealthDown
			return report
		}
		err := checker.Check(ctx)
			check := domain.Check{Name: checker.Name(), Status: domain.HealthOK, CheckedAt: time.Now().UTC()}
			if err != nil {
				check.Status = domain.HealthDown
				check.Message = err.Error()
			}
			report = report.WithCheck(check)
		}
	return report
}

func (s *Service) Ready(ctx context.Context) domain.HealthReport {
	if err := ctx.Err(); err != nil {
		return domain.HealthReport{Status: domain.HealthDown, Now: time.Now().UTC()}
	}
	return s.Health(ctx)
}
