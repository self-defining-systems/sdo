package runtime

import (
	"context"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	utilexec "k8s.io/client-go/util/exec"
	"sdo.dev/operator/sdk/program"
)

// DatabaseOperationStep is a value-free execution boundary, not upstream output.
type DatabaseOperationStep string

const (
	DatabaseContainerLookup DatabaseOperationStep = "container-lookup"
	DatabaseExecTransport   DatabaseOperationStep = "exec-transport"
	DatabaseClientBinary    DatabaseOperationStep = "client-binary"
	DatabaseAuthentication  DatabaseOperationStep = "authentication"
	DatabaseCatalogRead     DatabaseOperationStep = "catalog-read"
	DatabaseCatalogParse    DatabaseOperationStep = "catalog-parse"
	DatabaseMutation        DatabaseOperationStep = "catalog-write"
)

type DatabaseFailureReason string

const (
	DatabaseUnavailable       DatabaseFailureReason = "unavailable"
	databaseForbidden         DatabaseFailureReason = "RBAC denied"
	databaseTimeout           DatabaseFailureReason = "deadline exceeded"
	databaseNotFound          DatabaseFailureReason = "not found"
	databaseClientFailed      DatabaseFailureReason = "client startup or connection failed"
	databaseRejected          DatabaseFailureReason = "rejected"
	databaseIdentityChanged   DatabaseFailureReason = "Pod identity changed"
	databaseInvalidProjection DatabaseFailureReason = "invalid nonsecret projection"
)

// DatabaseOperationError deliberately has no wrapped upstream error: API bodies,
// exec status messages and database exceptions may contain credential values.
type DatabaseOperationError struct {
	Step   DatabaseOperationStep
	Reason DatabaseFailureReason
}

func (e *DatabaseOperationError) Error() string {
	return fmt.Sprintf("database %s: %s", e.Step, e.Reason)
}
func databaseOperationFailure(step DatabaseOperationStep, err error) error {
	reason := DatabaseUnavailable
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		reason = databaseTimeout
	case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
		reason = databaseForbidden
	case apierrors.IsNotFound(err):
		reason = databaseNotFound
	}
	return &DatabaseOperationError{Step: step, Reason: reason}
}
func databaseProcessFailure(t program.DatabaseTarget, a *program.EnsureDatabaseUser, code int32) error {
	if ref := databaseCredentialExit(t, a, code); ref != nil {
		return ref
	}
	step, reason := DatabaseExecTransport, databaseClientFailed
	switch code {
	case 126, 127:
		step, reason = DatabaseClientBinary, databaseNotFound
	case 60:
		step, reason = DatabaseAuthentication, databaseRejected
	case 61:
		step, reason = DatabaseCatalogRead, databaseRejected
	case 62:
		step, reason = DatabaseMutation, databaseRejected
	}
	return &DatabaseOperationError{Step: step, Reason: reason}
}
func databaseStreamFailure(ctx context.Context, t program.DatabaseTarget, err error) error {
	var exited utilexec.ExitError
	if errors.As(err, &exited) {
		return databaseProcessFailure(t, nil, int32(exited.ExitStatus()))
	}
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	return databaseOperationFailure(DatabaseExecTransport, err)
}
