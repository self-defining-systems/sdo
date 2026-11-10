package runtime

import (
	"context"
	"fmt"
	"strings"

	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/storeops"
	"sdo.dev/operator/sdk/storestate"
)

// StorePayloadSource holds the restore text the controller captured with
// each store Pod's promoted healthy baseline.
type StorePayloadSource interface {
	Payload(component, pod string) (storestate.Payload, bool)
}

// storeStatePort is the target's port, or its engine reader's default.
func (r storeRun) storeStatePort() int {
	if r.target.Port != 0 {
		return r.target.Port
	}
	if adapter, ok := storestate.AdapterFor(storestate.Engine(r.target.Engine)); ok {
		return adapter.DefaultPort()
	}
	return 0
}

func sameItem(a, b *storestate.Item) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Value == b.Value
}

func lookupItem(f *storestate.Facts, key storestate.Key) *storestate.Item {
	if f == nil {
		return nil
	}
	if item, ok := f.Lookup(key); ok {
		return &item
	}
	return nil
}

func renderItem(i *storestate.Item) string {
	if i == nil {
		return "(absent)"
	}
	return fmt.Sprintf("%q", i.Value)
}

// restoreItem runs a RestoreStoreItem: the baseline value comes from the
// controller's promoted healthy readings of the target Pod, the prior value
// from a live read, and a second read verifies the restore.
func (r storeRun) restoreItem(ctx context.Context, a program.RestoreStoreItem) (program.StoreRestoreEffect, bool, error) {
	key := a.Change.Key()
	effect := program.StoreRestoreEffect{Component: a.Target.Component, Key: key}
	refuse := func(format string, args ...any) (program.StoreRestoreEffect, bool, error) {
		return effect, false, fmt.Errorf("RestoreStoreItem refused: "+format, args...)
	}
	if r.access.Snapshot == nil {
		return refuse("no cluster snapshot carries the store's healthy baseline")
	}
	view, err := r.access.Snapshot.Snapshot(ctx)
	if err != nil {
		return refuse("cluster snapshot unavailable: %v", err)
	}
	baseline, ok := storestate.Baseline(view, a.Target.Component, a.Target.Pod)
	if !ok {
		return refuse("the controller holds no promoted healthy baseline of store %s Pod %s; it is captured once health has settled", a.Target.Component, a.Target.Pod.Name)
	}
	effect.Baseline = lookupItem(&baseline, key)
	current, factsErr := r.facts(ctx)
	if current == nil {
		return refuse("the store's current facts are unreadable: %s", factsErr)
	}
	effect.Prior = lookupItem(current, key)
	if sameItem(effect.Baseline, effect.Prior) {
		effect.Already, effect.After, effect.Facts = true, effect.Prior, current
		return effect, false, nil
	}
	options := storestate.RestoreOptions{EndSessions: a.EndSessions}
	if r.access.Payloads != nil {
		if payload, ok := r.access.Payloads.Payload(a.Target.Component, a.Target.Pod.Name); ok {
			options.Payload = payload.Lookup
		}
	}
	change := storestate.Change{Before: effect.Baseline, After: effect.Prior}
	commands, err := storestate.RestoreCommands(storestate.Engine(a.Target.Engine), storestate.Endpoint{Port: r.storeStatePort()}, change, baseline, options)
	if err != nil {
		return refuse("%v", err)
	}
	for i, args := range commands {
		stdout, _, err := r.command(ctx, storeops.Command{Args: args})
		if err != nil {
			return effect, i > 0, fmt.Errorf("RestoreStoreItem outcome unknown: %w", err)
		}
		if !strings.Contains(stdout, storestate.RestoredMarker) {
			answer := firstStoreLine(stdout)
			if i == 0 {
				return refuse("the store did not accept the restore of %s: %s", key, answer)
			}
			return effect, true, fmt.Errorf("RestoreStoreItem applied in part: step %d of %d of %s was not accepted: %s", i+1, len(commands), key, answer)
		}
	}
	effect.SessionsEnded = a.EndSessions && a.Target.Engine == string(storestate.EnginePostgres) && key.Kind == storestate.KindSetting && key.Scope != ""
	after, factsErr := r.facts(ctx)
	if after == nil {
		return effect, true, fmt.Errorf("RestoreStoreItem applied; the verifying read failed: %s", factsErr)
	}
	effect.After, effect.Facts = lookupItem(after, key), after
	if !sameItem(effect.Baseline, effect.After) {
		return effect, true, fmt.Errorf("RestoreStoreItem applied but %s is %s, not the baseline %s (a setting that needs a restart, or a client that set it again)", key, renderItem(effect.After), renderItem(effect.Baseline))
	}
	return effect, true, nil
}

// firstStoreLine is the first non-empty line of a store's answer, bounded.
func firstStoreLine(stdout string) string {
	for _, line := range strings.Split(stdout, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			if len(line) > 200 {
				line = line[:200]
			}
			return line
		}
	}
	return "no answer"
}

// StoreCredentialProbe is one credential check: the client container whose
// env and envFrom references the check Pod copies, and the store Pod and
// container whose image and IP it uses.
type StoreCredentialProbe struct {
	Namespace       string
	StorePod        string
	StoreContainer  string
	ClientPod       string
	ClientContainer string
	Engine          storestate.Engine
	Port            int
	Inputs          storestate.CredentialInputs
}

// StoreCredentialClient runs a credential check Pod and returns its output.
type StoreCredentialClient interface {
	StoreCredentialCheck(ctx context.Context, probe StoreCredentialProbe) (string, error)
}

// checkCredential runs a CheckStoreCredential.
func (r storeRun) checkCredential(ctx context.Context, a program.CheckStoreCredential) (program.StoreCredentialEffect, error) {
	effect := program.StoreCredentialEffect{Component: a.Target.Component, Client: a.Client, Container: a.Container}
	client, ok := r.executor.Client.(StoreCredentialClient)
	if !ok {
		return effect, fmt.Errorf("CheckStoreCredential refused: the controller cannot run a check Pod")
	}
	stdout, err := client.StoreCredentialCheck(ctx, StoreCredentialProbe{Namespace: a.Target.Pod.Namespace, StorePod: a.Target.Pod.Name, StoreContainer: a.Target.Container,
		ClientPod: a.Client.Name, ClientContainer: a.Container, Engine: storestate.Engine(a.Target.Engine), Port: r.storeStatePort(), Inputs: a.Credential})
	if err != nil {
		return effect, fmt.Errorf("CheckStoreCredential failed: %w", err)
	}
	check, err := storestate.ParseCredentialCheck(stdout)
	if err != nil {
		return effect, fmt.Errorf("CheckStoreCredential failed: %w", err)
	}
	effect.CredentialCheck = check
	return effect, nil
}
