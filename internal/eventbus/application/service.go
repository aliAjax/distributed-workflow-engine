package application

import (
	"context"
	"fmt"

	"github.com/acme/distributed-workflow-engine/internal/eventbus/domain"
)

type Publisher interface {
	Publish(ctx context.Context, event domain.Event) error
}

type Persister interface {
	AppendEvent(ctx context.Context, event domain.Event) error
}

type SignalResolver interface {
	ResolveExternal(ctx context.Context, signal domain.ExternalSignal) error
}

type Service struct {
	publisher Publisher
	persister Persister
	resolver  SignalResolver
}

func NewService(publisher Publisher, persister Persister) *Service {
	return &Service{publisher: publisher, persister: persister}
}

func (s *Service) SetSignalResolver(resolver SignalResolver) {
	s.resolver = resolver
}

func (s *Service) Publish(ctx context.Context, event domain.Event) error {
	if err := event.Validate(); err != nil {
		return err
	}
	if s.persister != nil {
		if err := s.persister.AppendEvent(ctx, event); err != nil {
			return fmt.Errorf("persist event: %w", err)
		}
	}
	if s.publisher != nil {
		if err := s.publisher.Publish(ctx, event); err != nil {
			return fmt.Errorf("publish event: %w", err)
		}
	}
	return nil
}

func (s *Service) PublishExternalSignal(ctx context.Context, signal domain.ExternalSignal) error {
	if err := signal.Validate(); err != nil {
		return err
	}
	event := domain.Event{
		TenantID:  signal.TenantID,
		ProjectID: signal.ProjectID,
		Type:      "external.signal." + signal.Event,
		Source:    "external",
		SubjectID: signal.ExecutionID,
		Payload:   signal.Payload,
	}
	if err := s.Publish(ctx, event); err != nil {
		return err
	}
	if s.resolver != nil {
		if err := s.resolver.ResolveExternal(ctx, signal); err != nil {
			return fmt.Errorf("resolve external signal: %w", err)
		}
	}
	return nil
}
