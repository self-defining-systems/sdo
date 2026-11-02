package kubereads

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"
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
	DatabaseForbidden         DatabaseFailureReason = "RBAC denied"
	DatabaseTimeout           DatabaseFailureReason = "deadline exceeded"
	DatabaseNotFound          DatabaseFailureReason = "not found"
	DatabaseClientFailed      DatabaseFailureReason = "client startup or connection failed"
	DatabaseRejected          DatabaseFailureReason = "rejected"
	DatabaseIdentityChanged   DatabaseFailureReason = "Pod identity changed"
	DatabaseInvalidProjection DatabaseFailureReason = "invalid nonsecret projection"
	DatabaseAmbiguousAuth     DatabaseFailureReason = "ambiguous source references; provide explicit target.auth"
)

// DatabaseOperationError deliberately has no wrapped upstream error: API bodies,
// exec status messages and database exceptions may contain credential values.
type DatabaseOperationError struct {
	Step   DatabaseOperationStep
	Reason DatabaseFailureReason
}

func (s DatabaseOperationStep) Safe() DatabaseOperationStep {
	switch s {
	case DatabaseContainerLookup, DatabaseExecTransport, DatabaseClientBinary, DatabaseAuthentication, DatabaseCatalogRead, DatabaseCatalogParse, DatabaseMutation:
		return s
	default:
		return "operation"
	}
}

func (e *DatabaseOperationError) Error() string {
	step, reason := e.Step.Safe(), e.Reason
	switch reason {
	case DatabaseUnavailable, DatabaseForbidden, DatabaseTimeout, DatabaseNotFound, DatabaseClientFailed, DatabaseRejected, DatabaseIdentityChanged, DatabaseInvalidProjection, DatabaseAmbiguousAuth:
	default:
		reason = DatabaseUnavailable
	}
	return fmt.Sprintf("database %s: %s", step, reason)
}
func DatabaseOperationFailure(step DatabaseOperationStep, err error) error {
	reason := DatabaseUnavailable
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		reason = DatabaseTimeout
	case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
		reason = DatabaseForbidden
	case apierrors.IsNotFound(err):
		reason = DatabaseNotFound
	}
	return &DatabaseOperationError{Step: step, Reason: reason}
}
func DatabaseProcessFailure(t program.DatabaseTarget, a *program.EnsureDatabaseUser, code int32) error {
	if ref := DatabaseCredentialExit(t, a, code); ref != nil {
		return ref
	}
	step, reason := DatabaseExecTransport, DatabaseClientFailed
	switch code {
	case 126, 127:
		step, reason = DatabaseClientBinary, DatabaseNotFound
	case 60:
		step, reason = DatabaseAuthentication, DatabaseRejected
	case 61:
		step, reason = DatabaseCatalogRead, DatabaseRejected
	case 62:
		step, reason = DatabaseMutation, DatabaseRejected
	}
	return &DatabaseOperationError{Step: step, Reason: reason}
}
func DatabaseStreamFailure(ctx context.Context, t program.DatabaseTarget, err error) error {
	var exited utilexec.ExitError
	if errors.As(err, &exited) {
		return DatabaseProcessFailure(t, nil, int32(exited.ExitStatus()))
	}
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	return DatabaseOperationFailure(DatabaseExecTransport, err)
}

// MongoScript constructs fixed operations, encoding all identifiers as JSON.
// Env references are evaluated by the Mongo shell. This function never resolves values.
func MongoScript(t program.DatabaseTarget, a *program.EnsureDatabaseUser) string {
	return MongoScriptWithCredentials(t, a, func(r program.CredentialRef) string {
		b, _ := json.Marshal(r.Env)
		return "credential(" + string(b) + fmt.Sprintf(",%d)", DatabaseCredentialCode(t, a, r))
	})
}

func MongoScriptWithCredentials(t program.DatabaseTarget, a *program.EnsureDatabaseUser, env func(program.CredentialRef) string) string {
	q := func(v any) string { b, _ := json.Marshal(v); return string(b) }
	script := `try { var stage=61; function credential(n,c){var v=(typeof process!=="undefined" && process.env)?process.env[n]:_getEnv(n);if(typeof v!=="string" || v.length===0){quit(c);}return v;}`
	if t.Auth != nil {
		script += "stage=60;var authDB=db.getSiblingDB(" + q(t.Auth.Database) + ");if(!authDB.auth(" + env(t.Auth.Username) + "," + env(t.Auth.Password) + ")){throw new Error('auth');}"
	}
	script += "stage=61;var catalog=db.getSiblingDB(" + q(t.Database) + ");"
	if a == nil {
		script += `var result=catalog.runCommand({usersInfo:1,showCredentials:false,showPrivileges:false});if(result.ok!==1){quit(result.code===13 || result.code===18 ? 60 : 61);} print(JSON.stringify(result.users.map(function(u){return {name:u.user,roles:u.roles.map(function(r){return {database:r.db,name:r.role};})};})));`
	} else {
		roles := make([]map[string]string, 0, len(a.Roles))
		for _, r := range a.Roles {
			roles = append(roles, map[string]string{"db": r.Database, "role": r.Name})
		}
		script += "stage=62;var name=" + q(a.User) + ",roles=" + q(roles) + ";if(catalog.getUser(name)===null){catalog.createUser({user:name,pwd:" + env(a.Password) + ",roles:roles});}else{catalog.updateUser(name,{roles:roles});}"
	}
	return script + `} catch(e) { quit(stage); }` + "\n"
}

type catalogOutput struct{ bytes.Buffer }

func (b *catalogOutput) Write(p []byte) (int, error) {
	if b.Len()+len(p) > 64*1024 {
		return 0, fmt.Errorf("catalog output exceeds cap")
	}
	return b.Buffer.Write(p)
}

// DatabaseTargetError contains only target identity and observed container names.
// It is safe to return across the credential-error sanitization boundary.
type DatabaseTargetError struct {
	Container  string
	Containers []string
}

func (e *DatabaseTargetError) Error() string {
	return fmt.Sprintf("container %s not found; containers: %v", e.Container, e.Containers)
}

func ValidateDatabaseContainer(object *unstructured.Unstructured, container string) error {
	names, _, _ := unstructuredContainers(object.Object)
	for _, name := range names {
		if name == container {
			return nil
		}
	}
	return &DatabaseTargetError{Container: container, Containers: names}
}

func unstructuredContainers(object map[string]any) ([]string, bool, error) {
	list, found, err := unstructured.NestedSlice(object, "spec", "containers")
	var names []string
	for _, item := range list {
		m, _ := item.(map[string]any)
		name, _ := m["name"].(string)
		names = append(names, name)
	}
	return names, found, err
}

// MongoCommandArgs keeps credential bytes off process argv and exec URLs.
// Legacy mongo execFile seeks its input, so /dev/stdin is not a script file.
// A fixed POSIX read/export bootstrap transfers the one-line script from stdin
// into exec's private environment; --eval sees only the fixed loader. It never
// starts the legacy REPL or writes credential-bearing shell history. Mongosh
// reads a noninteractive script directly from stdin through Node's file API.
func MongoCommandArgs(shell string, refs ...program.CredentialRef) []string {
	args := []string{shell, "--quiet", "--norc", "--host", "127.0.0.1", "--port", "27017"}
	if shell == "mongosh" {
		return append(args, "--file", "/dev/stdin")
	}
	privateEnv := "_SDO_DATABASE_SCRIPT"
	for {
		collision := false
		for _, ref := range refs {
			collision = collision || ref.Env == privateEnv
		}
		if !collision {
			break
		}
		privateEnv += "_"
	}
	args = append(args, "--eval", `eval(_getEnv("`+privateEnv+`"))`)
	return append([]string{"sh", "-c", `IFS= read -r ` + privateEnv + ` || exit 41; export ` + privateEnv + `; exec "$@"`, "sdo-database-stdin"}, args...)
}

func MongoInput(ctx context.Context, client kubernetes.Interface, protectedNamespace string, t program.DatabaseTarget, a *program.EnsureDatabaseUser) (string, error) {
	var resolutionErr error
	script := MongoScriptWithCredentials(t, a, func(ref program.CredentialRef) string {
		if ref.ConfigMap == nil {
			b, _ := json.Marshal(ref.Env)
			return "credential(" + string(b) + fmt.Sprintf(",%d)", DatabaseCredentialCode(t, a, ref))
		}
		value, err := ResolveCredential(ctx, client, protectedNamespace, t.Pod.Namespace, ref)
		if err != nil {
			resolutionErr = err
			return "null"
		}
		b, _ := json.Marshal(value)
		return string(b)
	})
	if resolutionErr != nil {
		return "", resolutionErr
	}
	return script, nil
}

func DatabaseCredentialRefs(t program.DatabaseTarget, a *program.EnsureDatabaseUser) []program.CredentialRef {
	var refs []program.CredentialRef
	if t.Auth != nil {
		refs = append(refs, t.Auth.Username, t.Auth.Password)
	}
	if a != nil {
		refs = append(refs, a.Password)
	}
	return refs
}
func DatabaseCredentialCode(t program.DatabaseTarget, a *program.EnsureDatabaseUser, ref program.CredentialRef) int {
	for i, r := range DatabaseCredentialRefs(t, a) {
		if r.ConfigMap == nil && r.Env == ref.Env {
			return 42 + i
		}
	}
	return 41
}
func DatabaseCredentialExit(t program.DatabaseTarget, a *program.EnsureDatabaseUser, code int32) error {
	refs := DatabaseCredentialRefs(t, a)
	i := int(code) - 42
	if i >= 0 && i < len(refs) && refs[i].ConfigMap == nil {
		return &CredentialReferenceError{Reference: refs[i], Reason: "container environment variable missing or empty"}
	}
	return nil
}
