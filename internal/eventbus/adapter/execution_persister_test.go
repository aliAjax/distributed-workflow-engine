package adapter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/acme/distributed-workflow-engine/internal/eventbus/domain"
	executiondomain "github.com/acme/distributed-workflow-engine/internal/execution/domain"
)

type executionEventRepo struct {
	event executiondomain.Event
	err error
}

func (r *executionEventRepo) AppendEvent(_ context.Context, event executiondomain.Event) error {
	r.event = event
	return r.err
}

func TestEventbusPersisterRepositoryWraps(t *testing.T) {
	sentinel := errors.New("append failed")
	repo := &executionEventRepo{err: sentinel}
	persister := NewExecutionPersister(repo)
	err := persister.AppendEvent(context.Background(), domain.Event{
		ID:        "event-a",
		TenantID:  "tenant-a",
		Type:      "node.failed",
		SubjectID: "execution-a",
		CreatedAt: time.Now().UTC(),
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected wrapped append error, got %v", err)
	}
}

func TestEventbusPersisterGeneratesID(t *testing.T) {
	repo := &executionEventRepo{}
	persister := NewExecutionPersister(repo)
	err := persister.AppendEvent(context.Background(), domain.Event{
		TenantID:  "tenant-a",
		Type:      "node.failed",
		SubjectID: "execution-a",
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("append event: %v", err)
	}
	if repo.event.ID == "" {
		t.Fatal("expected persister to generate event id")
	}
}
