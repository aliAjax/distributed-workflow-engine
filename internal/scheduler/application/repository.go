package application

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/acme/distributed-workflow-engine/internal/scheduler/domain"
)

type Repository interface {
	CreateTrigger(ctx context.Context, trigger domain.Trigger) error
	GetTrigger(ctx context.Context, id string) (domain.Trigger, error)
	ListTriggers(ctx context.Context, tenantID, projectID string) ([]domain.Trigger, error)
	ListDue(ctx context.Context, now time.Time, limit int) ([]domain.Trigger, error)
	UpdateTrigger(ctx context.Context, trigger domain.Trigger) error
	DeleteTrigger(ctx context.Context, id string) error
}

type Executor interface {
	Trigger(ctx context.Context, trigger domain.Trigger) error
}

type Service struct {
	repo     Repository
	executor Executor
	now      func() time.Time
}

func NewService(repo Repository, executor Executor) *Service {
	return &Service{repo: repo, executor: executor, now: time.Now}
}

func (s *Service) Register(ctx context.Context, trigger domain.Trigger) (domain.Trigger, error) {
	if err := trigger.Validate(); err != nil {
		return domain.Trigger{}, err
	}
	if trigger.ID == "" {
		trigger.ID = uuid.NewString()
	}
	if trigger.Type == domain.TriggerCron {
		next, err := NextRun(trigger.Cron, s.now().UTC())
		if err != nil {
			return domain.Trigger{}, err
		}
		trigger.NextRunAt = &next
	}
	if err := s.repo.CreateTrigger(ctx, trigger); err != nil {
		return domain.Trigger{}, err
	}
	return trigger, nil
}

func (s *Service) List(ctx context.Context, tenantID, projectID string) ([]domain.Trigger, error) {
	return s.repo.ListTriggers(ctx, tenantID, projectID)
}

func (s *Service) Delete(ctx context.Context, id string) error {
	return s.repo.DeleteTrigger(ctx, id)
}

func (s *Service) Tick(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 20
	}
	triggers, err := s.repo.ListDue(ctx, s.now().UTC(), limit)
	if err != nil {
		return 0, err
	}
	ran := 0
	for _, trigger := range triggers {
		if err := ctx.Err(); err != nil {
			return ran, err
		}
		if !trigger.Enabled {
			continue
		}
		if err := s.executor.Trigger(ctx, trigger); err != nil {
			return ran, err
		}
		now := s.now().UTC()
		trigger.LastRunAt = &now
		if trigger.Type == domain.TriggerCron {
			next, nextErr := NextRun(trigger.Cron, now)
			if nextErr != nil {
				return ran, nextErr
			}
			trigger.NextRunAt = &next
		} else {
			trigger.Enabled = false
			trigger.NextRunAt = nil
		}
		if err := s.repo.UpdateTrigger(ctx, trigger); err != nil {
			return ran, err
		}
		ran++
	}
	return ran, nil
}

func NextRun(expr string, after time.Time) (time.Time, error) {
	expr = strings.TrimSpace(expr)
	if strings.HasPrefix(expr, "@every ") {
		duration, err := time.ParseDuration(strings.TrimSpace(strings.TrimPrefix(expr, "@every ")))
		if err != nil {
			return time.Time{}, fmt.Errorf("parse @every cron: %w", err)
		}
		if duration <= 0 {
			return time.Time{}, errors.New("@every duration must be positive")
		}
		return after.Add(duration), nil
	}
	parts := strings.Fields(expr)
	if len(parts) != 5 {
		return time.Time{}, fmt.Errorf("cron expression must contain 5 fields")
	}
	candidate := after.Truncate(time.Minute).Add(time.Minute)
	for i := 0; i < 525600; i++ {
		if matchesField(parts[0], candidate.Minute(), 0, 59) &&
			matchesField(parts[1], candidate.Hour(), 0, 23) &&
			matchesField(parts[2], candidate.Day(), 1, 31) &&
			matchesField(parts[3], int(candidate.Month()), 1, 12) &&
			matchesField(parts[4], int(candidate.Weekday()), 0, 6) {
			return candidate, nil
		}
		candidate = candidate.Add(time.Minute)
	}
	return time.Time{}, fmt.Errorf("no matching cron time within one year")
}

func matchesField(expr string, value, min, max int) bool {
	if expr == "*" {
		return true
	}
	for _, part := range strings.Split(expr, ",") {
		if strings.Contains(part, "/") {
			base, step, ok := strings.Cut(part, "/")
			if !ok {
				continue
			}
			stepValue, err := strconv.Atoi(step)
			if err != nil || stepValue <= 0 {
				continue
			}
			start, end := min, max
			if base != "*" {
				if strings.Contains(base, "-") {
					start, end, ok = parseRange(base)
					if !ok {
						continue
					}
				} else {
					start, err = strconv.Atoi(base)
					if err != nil {
						continue
					}
					end = max
				}
			}
			for current := start; current <= end; current += stepValue {
				if current == value {
					return true
				}
			}
			continue
		}
		if strings.Contains(part, "-") {
			start, end, ok := parseRange(part)
			if ok && value >= start && value <= end {
				return true
			}
			continue
		}
		if parsed, err := strconv.Atoi(part); err == nil && parsed == value {
			return true
		}
	}
	return false
}

func parseRange(expr string) (int, int, bool) {
	start, end, ok := strings.Cut(expr, "-")
	if !ok {
		return 0, 0, false
	}
	startValue, err1 := strconv.Atoi(start)
	endValue, err2 := strconv.Atoi(end)
	return startValue, endValue, err1 == nil && err2 == nil
}
