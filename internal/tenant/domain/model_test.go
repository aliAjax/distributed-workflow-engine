package domain

import (
	"errors"
	"testing"
	"time"
)

func TestTenantAPIKeyValidateExpired(t *testing.T) {
	expired := time.Now().Add(-time.Hour)
	err := (APIKey{Name: "key-a", TenantID: "tenant-a", ExpiresAt: &expired}).Validate()
	if !errors.Is(err, ErrAPIKeyExpired) {
		t.Fatalf("expected expired sentinel, got %v", err)
	}
}

func TestTenantAdminPrincipalAlwaysAllowed(t *testing.T) {
	principal := Principal{TenantID: "tenant-a", Roles: []string{string(RoleAdmin)}}
	if !principal.Can("workflow", "delete") {
		t.Fatal("expected admin principal to be allowed")
	}
}

func TestTenantPrincipalScopeWildcard(t *testing.T) {
	principal := Principal{TenantID: "tenant-a", Scopes: []string{"workflow:*"}}
	if !principal.Can("workflow", "delete") {
		t.Fatal("expected wildcard scope to allow delete action")
	}
}
