package httpapi

import (
	"context"

	tenantdomain "github.com/acme/distributed-workflow-engine/internal/tenant/domain"
)

type principalKey struct{}

func withPrincipal(ctx context.Context, principal tenantdomain.Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, principal)
}

func principalFrom(ctx context.Context) tenantdomain.Principal {
	principal, _ := ctx.Value(principalKey{}).(tenantdomain.Principal)
	return principal
}
