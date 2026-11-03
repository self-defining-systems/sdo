package runtime

import (
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"testing"
)

func TestDatabaseFailureReasonsBelongToBothReceiptContracts(t *testing.T) {
	allowed := func(value string, values []string) bool {
		for _, v := range values {
			if v == value {
				return true
			}
		}
		return false
	}
	steps := []DatabaseOperationStep{DatabaseContainerLookup, DatabaseExecTransport, DatabaseClientBinary, DatabaseAuthentication, DatabaseCatalogRead, DatabaseCatalogParse, DatabaseMutation, "future-step"}
	reasons := []DatabaseFailureReason{DatabaseUnavailable, databaseForbidden, databaseTimeout, databaseNotFound, databaseClientFailed, databaseRejected, databaseIdentityChanged, databaseInvalidProjection, databaseAmbiguousAuth, "future-reason"}
	for _, step := range steps {
		for _, reason := range reasons {
			code := databaseFailureCode(&DatabaseOperationError{Step: step, Reason: reason})
			if !allowed(code, wire.StringIn(&contracts.ActionReport{}, "failure_reason")) || !allowed(code, wire.StringIn(&contracts.RepairActionReceipt{}, "failure_reason")) {
				t.Errorf("%s/%s produces unpersistable reason %q", step, reason, code)
			}
		}
	}
}
