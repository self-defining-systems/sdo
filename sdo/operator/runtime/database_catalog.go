package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	spdytransport "k8s.io/client-go/transport/spdy"
	"sdo.dev/operator/sdk/program"
)

// DatabaseCatalogClient is the narrow nonsecret catalog boundary. Implementations
// resolve credential references only inside the target application container.
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
	client := TypedPlanClient{Client: v.Client, ExecConfig: v.DatabaseExecConfig}
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

// mongoScript constructs fixed operations, encoding all identifiers as JSON.
// Env references are evaluated by the Mongo shell, never the controller.
func mongoScript(t program.DatabaseTarget, a *program.EnsureDatabaseUser) string {
	q := func(v any) string { b, _ := json.Marshal(v); return string(b) }
	env := func(r program.CredentialRef) string { return "credential(" + q(r.Env) + ")" }
	script := `try { function credential(n){var v=(typeof process!=="undefined" && process.env)?process.env[n]:_getEnv(n);if(typeof v!=="string" || v.length===0){throw new Error("unavailable");}return v;}`
	if t.Auth != nil {
		script += "var authDB=db.getSiblingDB(" + q(t.Auth.Database) + ");if(!authDB.auth(" + env(t.Auth.Username) + "," + env(t.Auth.Password) + ")){throw new Error('auth');}"
	}
	script += "var catalog=db.getSiblingDB(" + q(t.Database) + ");"
	if a == nil {
		script += `var result=catalog.runCommand({usersInfo:1,showCredentials:false,showPrivileges:false});if(result.ok!==1){throw new Error("catalog");} print(JSON.stringify(result.users.map(function(u){return {name:u.user,roles:u.roles.map(function(r){return {database:r.db,name:r.role};})};})));`
	} else {
		roles := make([]map[string]string, 0, len(a.Roles))
		for _, r := range a.Roles {
			roles = append(roles, map[string]string{"db": r.Database, "role": r.Name})
		}
		script += "var name=" + q(a.User) + ",roles=" + q(roles) + ";if(catalog.getUser(name)===null){catalog.createUser({user:name,pwd:" + env(a.Password) + ",roles:roles});}else{catalog.updateUser(name,{roles:roles});}"
	}
	return script + `} catch(e) { quit(41); }` + "\n"
}

type catalogOutput struct{ bytes.Buffer }

func (b *catalogOutput) Write(p []byte) (int, error) {
	if b.Len()+len(p) > 64*1024 {
		return 0, fmt.Errorf("catalog output exceeds cap")
	}
	return b.Buffer.Write(p)
}

func (c TypedPlanClient) mongo(ctx context.Context, t program.DatabaseTarget) ([]byte, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	if c.Client == nil || c.ExecConfig == nil || c.Client.CoreV1().RESTClient() == nil {
		return nil, fmt.Errorf("database exec transport unavailable")
	}
	object, err := c.Get(ctx, t.Pod)
	if err != nil || object == nil || object.GetUID() == "" {
		return nil, fmt.Errorf("database Pod unavailable")
	}
	containers, _, _ := unstructuredContainers(object.Object)
	found := false
	for _, name := range containers {
		found = found || name == t.Container
	}
	if !found {
		return nil, fmt.Errorf("database container unavailable")
	}
	shell := t.Shell
	if shell == "" {
		shell = "mongo"
	}
	command := []string{shell, "--quiet", "--norc", "--host", "127.0.0.1", "--port", "27017", "--eval", mongoScript(t, nil)}
	request := c.Client.CoreV1().RESTClient().Post().Resource("pods").Name(t.Pod.Name).Namespace(t.Pod.Namespace).SubResource("exec").VersionedParams(&corev1.PodExecOptions{Container: t.Container, Command: command, Stdin: true, Stdout: true, Stderr: false, TTY: false}, scheme.ParameterCodec)
	config := rest.CopyConfig(c.ExecConfig)
	transport, upgrader, err := spdytransport.RoundTripperFor(config)
	if err != nil {
		return nil, fmt.Errorf("database exec transport unavailable")
	}
	executor, err := remotecommand.NewSPDYExecutorRejectRedirects(transport, upgrader, "POST", request.URL())
	if err != nil {
		return nil, fmt.Errorf("database exec transport unavailable")
	}
	output := &catalogOutput{}
	var stdout io.Writer = output
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdin: strings.NewReader(""), Stdout: stdout})
	if err != nil {
		return nil, fmt.Errorf("database operation failed or outcome unknown")
	}
	current, readErr := c.Get(ctx, t.Pod)
	if readErr != nil || current == nil || current.GetUID() != object.GetUID() {
		return nil, fmt.Errorf("database operation Pod identity changed")
	}
	return output.Bytes(), nil
}
func (c TypedPlanClient) DatabaseUsers(ctx context.Context, t program.DatabaseTarget) ([]program.DatabaseUser, error) {
	raw, err := c.mongo(ctx, t)
	if err != nil {
		return nil, err
	}
	var users []program.DatabaseUser
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err = d.Decode(&users); err != nil || d.Decode(new(any)) != io.EOF {
		return nil, fmt.Errorf("database catalog returned invalid nonsecret projection")
	}
	return program.NormalizeDatabaseUsers(users)
}
func (c TypedPlanClient) EnsureDatabaseUser(ctx context.Context, a program.EnsureDatabaseUser) error {
	if err := a.Validate(); err != nil {
		return err
	}
	t := a.Target
	before, err := c.Get(ctx, t.Pod)
	if err != nil || before == nil || before.GetUID() == "" {
		return fmt.Errorf("database Pod unavailable")
	}
	names, _, _ := unstructuredContainers(before.Object)
	found := false
	for _, name := range names {
		found = found || name == t.Container
	}
	if !found {
		return fmt.Errorf("database container unavailable")
	}
	shell := t.Shell
	if shell == "" {
		shell = "mongo"
	}
	command := program.Command{Pod: t.Pod, Container: t.Container, Args: []string{shell, "--quiet", "--norc", "--host", "127.0.0.1", "--port", "27017", "--eval", mongoScript(t, &a)}}
	code, err := c.runCommand(ctx, command)
	if err != nil || code != 0 {
		return fmt.Errorf("database operation failed or outcome unknown")
	}
	after, err := c.Get(ctx, t.Pod)
	if err != nil || after == nil || after.GetUID() != before.GetUID() {
		return fmt.Errorf("database operation Pod identity changed")
	}
	return nil
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
