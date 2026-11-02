package runtime

import (
	"context"
	"fmt"
	"time"

	"sdo.dev/operator/kubereads"
	"sdo.dev/operator/sdk/program"
)

// DatabaseCatalogClient is the narrow nonsecret catalog boundary. Implementations
// resolve credential references only at execution time. Catalog interfaces and
// observations never carry resolved credential bytes.
type DatabaseCatalogClient interface {
	DatabaseUsers(context.Context, program.DatabaseTarget) ([]program.DatabaseUser, error)
	EnsureDatabaseUser(context.Context, program.EnsureDatabaseUser) error
}

func (v LiveReadView) DatabaseUsers(t program.DatabaseTarget) ([]program.DatabaseUser, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	if t.Pod.Namespace != v.Namespace() {
		return nil, fmt.Errorf("database catalog outside snapshot namespace")
	}
	captured, err := v.Object(t.Pod)
	if err != nil || captured == nil || captured.GetUID() == "" {
		return nil, fmt.Errorf("database Pod absent from snapshot")
	}
	ctx, cancel := context.WithTimeout(v.context(), 5*time.Second)
	defer cancel()
	client := TypedPlanClient{Client: v.Client, ExecConfig: v.DatabaseExecConfig, ProtectedNamespace: v.ProtectedNamespace}
	current, err := client.Get(ctx, t.Pod)
	if err != nil || current == nil || current.GetUID() != captured.GetUID() {
		return nil, fmt.Errorf("database Pod identity differs from snapshot")
	}
	users, err := client.DatabaseUsers(ctx, t)
	if err != nil {
		return nil, err
	}
	current, err = client.Get(ctx, t.Pod)
	if err != nil || current == nil || current.GetUID() != captured.GetUID() {
		return nil, fmt.Errorf("database Pod identity differs from snapshot")
	}
	return users, nil
}

func (v *cachedReadView) DatabaseUsers(t program.DatabaseTarget) ([]program.DatabaseUser, error) {
	var r []program.DatabaseUser
	err := v.read("DatabaseUsers", []any{t}, &r, func() (any, error) { return v.ReadView.DatabaseUsers(t) })
	return r, err
}

var (
	mongoScript               = kubereads.MongoScript
	mongoCommandArgs          = kubereads.MongoCommandArgs
	databaseCredentialRefs    = kubereads.DatabaseCredentialRefs
	databaseCredentialCode    = kubereads.DatabaseCredentialCode
	databaseCredentialExit    = kubereads.DatabaseCredentialExit
	validateDatabaseContainer = kubereads.ValidateDatabaseContainer
)

// DatabaseTargetError contains only target identity and observed container names.
// It is safe to return across the credential-error sanitization boundary.
type DatabaseTargetError = kubereads.DatabaseTargetError

func (c TypedPlanClient) databaseReader() kubereads.DatabaseReader {
	return kubereads.DatabaseReader{
		Get:                c.Get,
		Exec:               kubereads.SPDYExec{Client: c.Client, Config: c.ExecConfig},
		Client:             c.Client,
		ProtectedNamespace: c.ProtectedNamespace,
	}
}

func (c TypedPlanClient) DatabaseUsers(ctx context.Context, t program.DatabaseTarget) ([]program.DatabaseUser, error) {
	return c.databaseReader().Users(ctx, t)
}
func (c TypedPlanClient) EnsureDatabaseUser(ctx context.Context, a program.EnsureDatabaseUser) error {
	if err := a.Validate(); err != nil {
		return err
	}
	t := a.Target
	before, err := c.Get(ctx, t.Pod)
	if err != nil || before == nil || before.GetUID() == "" {
		return kubereads.DatabaseOperationFailure(DatabaseContainerLookup, err)
	}
	if err := kubereads.ValidateDatabaseContainer(before, t.Container); err != nil {
		return err
	}
	shell := t.Shell
	if shell == "" {
		shell = "mongo"
	}
	input, err := kubereads.MongoInput(ctx, c.Client, c.ProtectedNamespace, t, &a)
	if err != nil {
		return err
	}
	// This command is executor-local: only the reference-bearing action is recorded.
	command := program.Command{Pod: t.Pod, Container: t.Container, Args: kubereads.MongoCommandArgs(shell, kubereads.DatabaseCredentialRefs(t, &a)...), Stdin: input}
	code, err := c.runCommand(ctx, command)
	if err != nil {
		return kubereads.DatabaseOperationFailure(DatabaseExecTransport, err)
	}
	if code != 0 {
		return kubereads.DatabaseProcessFailure(t, &a, code)
	}
	after, err := c.Get(ctx, t.Pod)
	if err != nil || after == nil || after.GetUID() != before.GetUID() {
		return &DatabaseOperationError{Step: DatabaseContainerLookup, Reason: kubereads.DatabaseIdentityChanged}
	}
	return nil
}
