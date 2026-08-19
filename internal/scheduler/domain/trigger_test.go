package domain

import "testing"

func TestSchedulerTriggerValidate(t *testing.T) {
	trigger := Trigger{Type: TriggerManual}
	if err := trigger.Validate(); err == nil {
		t.Fatal("expected empty workflow id to be rejected")
	}
	trigger.WorkflowID = "wf-a"
	if err := trigger.Validate(); err != nil {
		t.Fatalf("expected valid manual trigger, got %v", err)
	}
}
