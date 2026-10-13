package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/internal/identity"
	"github.com/lesomnus/shale/internal/sso"
)

// `auth.delegation.persons` is `reads` or `tenant`, nothing else, and only
// `tenant` lets an app view for a person whatever `auth.operators.reads`
// says (§33.8).
func TestDelegationPersons(t *testing.T) {
	a := AuthConfig{
		Oidc:      sso.Config{Issuer: "https://issuer.example"},
		Operators: identity.OperatorsConfig{Tenant: "acme", Reads: identity.ReadsGranted},
	}
	c := DelegationConfig{Audience: "urn:test:api:shale", Clients: map[string]string{"portal-client": "portal"}}
	for _, v := range []string{"", DelegationPersonsReads, DelegationPersonsTenant} {
		c.Persons = v
		require.NoError(t, c.check(a), v)
		require.Equal(t, v == DelegationPersonsTenant, c.core().EveryPerson, v)
	}
	c.Persons = "everybody"
	require.ErrorContains(t, c.check(a), "auth.delegation.persons")
	require.ErrorContains(t, DelegationConfig{Persons: "all"}.check(a), "auth.delegation.persons", "refused when nothing is delegated too")
}
