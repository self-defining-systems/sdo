package runtime

import (
	"errors"

	"sdo.dev/operator/kubereads"
)

type DatabaseOperationStep = kubereads.DatabaseOperationStep

const (
	DatabaseContainerLookup = kubereads.DatabaseContainerLookup
	DatabaseExecTransport   = kubereads.DatabaseExecTransport
	DatabaseClientBinary    = kubereads.DatabaseClientBinary
	DatabaseAuthentication  = kubereads.DatabaseAuthentication
	DatabaseCatalogRead     = kubereads.DatabaseCatalogRead
	DatabaseCatalogParse    = kubereads.DatabaseCatalogParse
	DatabaseMutation        = kubereads.DatabaseMutation
)

type DatabaseFailureReason = kubereads.DatabaseFailureReason

const (
	DatabaseUnavailable       = kubereads.DatabaseUnavailable
	databaseForbidden         = kubereads.DatabaseForbidden
	databaseTimeout           = kubereads.DatabaseTimeout
	databaseNotFound          = kubereads.DatabaseNotFound
	databaseClientFailed      = kubereads.DatabaseClientFailed
	databaseRejected          = kubereads.DatabaseRejected
	databaseIdentityChanged   = kubereads.DatabaseIdentityChanged
	databaseInvalidProjection = kubereads.DatabaseInvalidProjection
	databaseAmbiguousAuth     = kubereads.DatabaseAmbiguousAuth
)

// DatabaseOperationError deliberately has no wrapped upstream error: API bodies,
// exec status messages and database exceptions may contain credential values.
type DatabaseOperationError = kubereads.DatabaseOperationError

var (
	databaseOperationFailure = kubereads.DatabaseOperationFailure
	databaseProcessFailure   = kubereads.DatabaseProcessFailure
	databaseSafeError        = kubereads.DatabaseSafeError
)

// databaseFailureCode is a stable, value-free reason for a failed database
// operation. Upstream error text is never copied: it may carry credentials.
func databaseFailureCode(err error) string {
	var operation *DatabaseOperationError
	if errors.As(err, &operation) && operation.Reason == databaseForbidden {
		return "authorization_denied"
	}
	if databaseFailedBeforeWrite(err) {
		return "execution_failed"
	}
	return "outcome_unknown"
}

// databaseFailedBeforeWrite reports failures that provably precede the catalog
// mutation: unresolved references, a missing container or client binary, and
// rejected authentication or catalog preparation. Transport loss and identity
// changes may follow a write and stay uncertain.
func databaseFailedBeforeWrite(err error) bool {
	var reference *CredentialReferenceError
	var target *DatabaseTargetError
	if errors.As(err, &reference) || errors.As(err, &target) {
		return true
	}
	var operation *DatabaseOperationError
	if !errors.As(err, &operation) {
		return false
	}
	switch operation.Step {
	case DatabaseAuthentication, DatabaseClientBinary, DatabaseCatalogRead:
		return true
	case DatabaseContainerLookup:
		return operation.Reason != databaseIdentityChanged
	}
	return false
}
