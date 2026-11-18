package kubereads

import (
	"errors"
	"testing"

	"sdo.dev/operator/sdk"
)

func TestDatabaseOperationErrorClass(t *testing.T) {
	cases := []struct {
		step   DatabaseOperationStep
		reason DatabaseFailureReason
		want   string
	}{
		{DatabaseAuthentication, DatabaseRejected, sdk.ReadClassRefused},
		{DatabaseCatalogRead, DatabaseRejected, sdk.ReadClassRefused},
		{DatabaseMutation, DatabaseRejected, sdk.ReadClassUnavailable},
		{DatabaseExecTransport, DatabaseTimeout, sdk.ReadClassUnavailable},
		{DatabaseExecTransport, DatabaseClientFailed, sdk.ReadClassUnavailable},
		{DatabaseClientBinary, DatabaseNotFound, sdk.ReadClassUnavailable},
		{DatabaseContainerLookup, DatabaseIdentityChanged, sdk.ReadClassUnavailable},
		{DatabaseCatalogRead, DatabaseForbidden, sdk.ReadClassUnavailable},
		{DatabaseCatalogParse, DatabaseInvalidProjection, sdk.ReadClassUnavailable},
		{DatabaseAuthentication, DatabaseAmbiguousAuth, sdk.ReadClassInvalid},
		{DatabaseAuthentication, DatabaseUnavailable, sdk.ReadClassUnavailable},
	}
	for _, c := range cases {
		err := error(&DatabaseOperationError{Step: c.step, Reason: c.reason})
		if got := sdk.ReadErrorClass(err); got != c.want {
			t.Errorf("%s/%s = %q, want %q", c.step, c.reason, got, c.want)
		}
	}
}

func TestDatabaseOperationErrorKeepsTextAndWrapsOnlyItsClass(t *testing.T) {
	err := error(&DatabaseOperationError{Step: DatabaseAuthentication, Reason: DatabaseRejected})
	if err.Error() != "database authentication: rejected" {
		t.Fatalf("text = %q", err)
	}
	if !errors.Is(err, sdk.ErrRefused) || errors.Is(err, sdk.ErrUnavailable) {
		t.Fatalf("a rejected login is refused only: %v", err)
	}
	var typed *DatabaseOperationError
	if !errors.As(err, &typed) {
		t.Fatal("typed error must stay reachable")
	}
}

func TestCredentialAndTargetErrorClasses(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{&CredentialReferenceError{Reason: credentialEnvMissing}, sdk.ReadClassRefused},
		{&CredentialReferenceError{Reason: credentialKeyMissing}, sdk.ReadClassRefused},
		{&CredentialReferenceError{Reason: credentialInvalidRef}, sdk.ReadClassInvalid},
		{&CredentialReferenceError{Reason: credentialNamespace}, sdk.ReadClassInvalid},
		{&CredentialReferenceError{Reason: "ConfigMap transport unavailable"}, sdk.ReadClassUnavailable},
		{&DatabaseTargetError{Container: "c", Containers: []string{"d"}}, sdk.ReadClassInvalid},
	}
	for _, c := range cases {
		if got := sdk.ReadErrorClass(c.err); got != c.want {
			t.Errorf("%v = %q, want %q", c.err, got, c.want)
		}
	}
}
