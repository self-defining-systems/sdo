package traffic

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"time"
)

// identityLocks holds, per synthetic identity, the writing sample that is
// acting on it. A write-read sample writes its identity's data and reads back
// exactly what it wrote; a second sample writing the same data in between
// (another workload running a copy of the journey, or the same scenario's
// next iteration) makes the first read the second one's value. Samples that
// share an identity therefore run one at a time, in every engine that
// ShareSetup joins.
type identityLocks struct {
	mu   sync.Mutex
	held map[string]chan struct{}
}

func (l *identityLocks) slot(key string) chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.held == nil {
		l.held = map[string]chan struct{}{}
	}
	slot := l.held[key]
	if slot == nil {
		slot = make(chan struct{}, 1)
		l.held[key] = slot
	}
	return slot
}

// lock waits until no other sample holds any of keys (sorted, so two samples
// never wait for each other) and returns the release function.
func (l *identityLocks) lock(ctx context.Context, keys []string) (func(), error) {
	if l == nil || len(keys) == 0 {
		return func() {}, nil
	}
	acquired := make([]chan struct{}, 0, len(keys))
	release := func() {
		for index := len(acquired) - 1; index >= 0; index-- {
			<-acquired[index]
		}
	}
	for _, key := range keys {
		slot := l.slot(key)
		select {
		case slot <- struct{}{}:
			acquired = append(acquired, slot)
		case <-ctx.Done():
			release()
			return nil, ctx.Err()
		}
	}
	return release, nil
}

// writeIdentity is the set of synthetic identities a scenario's samples act
// on, as lock keys; a read scenario has none. Identities are not declared in
// the catalog (a generator names its synthetic users in its requests), so the
// key is the scenario's requests themselves, built with one fixed random
// source and time: two scenarios that send the same requests (a journey the
// designer copied into a second workload) and every iteration of one scenario
// share it. Each probe user the scenario signs in as is a key of its own.
func writeIdentity(scenario Scenario) []string {
	if scenario.SideEffect == SideEffectRead {
		return nil
	}
	keys := []string{"requests:" + requestTemplate(scenario)}
	for _, user := range scenario.Users {
		keys = append(keys, "probe-user:"+user.Name)
	}
	sort.Strings(keys)
	return keys
}

// requestTemplate digests the requests scenario's steps send. A step that
// needs a value an earlier step or setup saves contributes its build error,
// which names the missing value.
func requestTemplate(scenario Scenario) (digest string) {
	defer func() {
		if recover() != nil {
			// A generator that cannot be built offline still serializes its own
			// iterations.
			digest = "scenario:" + scenario.ID
		}
	}()
	rng := rand.New(rand.NewPCG(0, 0))
	state := State{StateNow: time.Unix(0, 0).UTC().Format(time.RFC3339)}
	for _, user := range scenario.Users {
		state[user.As] = passwordPlaceholder
		if user.SSHKey != "" {
			state[user.SSHKey] = publicKeyPlaceholder
		}
	}
	var text strings.Builder
	for _, step := range scenario.Steps {
		target := scenario.stepTarget(step)
		request, err := step.Endpoint.Build(context.Background(), rng, state)
		if err != nil {
			fmt.Fprintf(&text, "%s|unbuilt %s\n", target.key(), err)
			continue
		}
		headers := make([]string, 0, len(request.Headers))
		for name, value := range request.Headers {
			headers = append(headers, name+": "+value)
		}
		sort.Strings(headers)
		fmt.Fprintf(&text, "%s|%s|%s|%s\n", target.key(), requestLine(request, nil), strings.Join(headers, "\n"), request.Body)
	}
	sum := sha256.Sum256([]byte(text.String()))
	return hex.EncodeToString(sum[:])
}

func (t Target) key() string {
	return fmt.Sprintf("%s://%s@%s:%d", t.Scheme, t.User, t.Service, t.Port)
}
