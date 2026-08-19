package application

import (
	"context"
	"errors"
	"testing"

	"github.com/acme/distributed-workflow-engine/internal/eventbus/domain"
)

type eventPublisher struct {
	err error
}

func (p eventPublisher) Publish(context.Context, domain.Event) error { return p.err }

type eventPersister struct {
	err error
}

func (p eventPersister) AppendEvent(context.Context, domain.Event) error { return p.err }

type signalResolver struct {
	called bool
	err    error
}

func (r *signalResolver) ResolveExternal(context.Context, domain.ExternalSignal) error {
	r.called = true
	return r.err
}

func TestEventbusPublishPersistWraps(t *testing.T) {
	sentinel := errors.New("persist failed")
	svc := NewService(eventPublisher{}, eventPersister{err: sentinel})
	err := svc.Publish(context.Background(), domain.Event{TenantID: "tenant-a", Type: "workflow.started"})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected wrapped persist error, got %v", err)
	}
}

func TestEventbusExternalSignalResolver(t *testing.T) {
	sentinel := errors.New("resolve failed")
	resolver := &signalResolver{err: sentinel}
	svc := NewService(eventPublisher{}, eventPersister{})
	svc.SetSignalResolver(resolver)
	err := svc.PublishExternalSignal(context.Background(), domain.ExternalSignal{
		TenantID: "tenant-a",
		Event:    "approval",
	})
	if !resolver.called {
		t.Fatal("expected resolver to be called")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected wrapped resolver error, got %v", err)
	}
}
