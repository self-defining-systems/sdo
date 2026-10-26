package runtime

import (
	"errors"
	"strings"
	"testing"
)

func TestDatabaseSafeErrorRetainsNamedFailureStep(t *testing.T) {
	for _, step := range []DatabaseOperationStep{DatabaseContainerLookup, DatabaseExecTransport, DatabaseClientBinary, DatabaseAuthentication, DatabaseCatalogRead, DatabaseCatalogParse} {
		original := &DatabaseOperationError{Step: step, Reason: DatabaseUnavailable}
		err := databaseSafeError(original, "database catalog observation unavailable")
		var diagnostic *DatabaseOperationError
		if !errors.As(err, &diagnostic) || diagnostic.Step != step || !strings.Contains(err.Error(), string(step)) {
			t.Fatalf("database step lost: %v", err)
		}
	}
	const canary = "SYNTHETIC_UPSTREAM_CREDENTIAL_DO_NOT_EMIT"
	if strings.Contains(databaseSafeError(errors.New(canary), "database catalog observation unavailable").Error(), canary) {
		t.Fatal("untrusted upstream message escaped")
	}
}
