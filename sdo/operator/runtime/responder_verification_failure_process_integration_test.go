//go:build integration

package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
)

func TestResponderVerificationFailureIsSavedBeforeRollback(t *testing.T) {
	for _, rollback := range []string{"success", "failure", "interrupted"} {
		t.Run(rollback, func(t *testing.T) {
			ctx := context.Background()
			loop, kube := trustedPortLoop(t, false, false, false)
			root := nativeRepository(t)
			file := filepath.Join(root, ".sdo/playbooks/service-targetport-mismatch/program.go")
			if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(file, []byte("package toy\nconst Port = 8080\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			nativeGit(t, root, "add", ".")
			nativeGit(t, root, "commit", "-qm", "synthetic program")
			loop.Request.RepositoryWorktree = root
			loop.Request.SourceCommit = nativeGit(t, root, "rev-parse", "HEAD")
			revision, err := programDependencyRevision(ctx, root, loop.Request.SourceCommit, "service-targetport-mismatch")
			if err != nil {
				t.Fatal(err)
			}
			// Mutable checkout bytes and stale prior trust must not bind the failure.
			if err := os.WriteFile(file, []byte("package toy\nconst Port = 9999\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			loop.Trust = map[string]ProgramTrust{"service-targetport-mismatch": {LastChange: "stale-revision"}}
			dispatcher := KubernetesJobDispatcher{Client: kube, Namespace: "app"}
			attachRecoveryStore(t, dispatcher, loop)
			signal := "application_verification_failure:service-targetport-mismatch@" + revision
			checked := false
			loop.Executor.Client = &verificationRollbackClient{PlanObjectClient: loop.Executor.Client, beforeRollback: func() error {
				checked = true
				restored := restartPortLoop(loop)
				attachRecoveryStore(t, dispatcher, restored)
				if signals := restored.FailureSignals(); len(signals) != 1 || signals[0] != signal {
					t.Fatalf("failure was not durably revision-bound before rollback: %v", signals)
				}
				if len(restored.PlanApplications()) != 0 {
					t.Fatal("restart restored application state")
				}
				if rollback == "failure" {
					return errors.New("synthetic rollback outage")
				}
				if rollback == "interrupted" {
					panic("synthetic rollback interruption")
				}
				return nil
			}}
			planned := loop.Handle(ctx, ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: program.Bindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}})
			if planned.Error != "" {
				t.Fatal(planned.Error)
			}
			func() {
				defer func() {
					if value := recover(); value != nil {
						if rollback != "interrupted" || value != "synthetic rollback interruption" {
							panic(value)
						}
					} else if rollback == "interrupted" {
						t.Error("rollback interruption did not fire")
					}
				}()
				applied := loop.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
				if applied.Error == "" || (rollback == "failure" && !strings.Contains(applied.Error, "synthetic rollback outage")) {
					t.Fatalf("missing verification/rollback error: %+v", applied)
				}
			}()
			if !checked {
				t.Fatal("rollback was not attempted")
			}
			restored := restartPortLoop(loop)
			attachRecoveryStore(t, dispatcher, restored)
			if !contains(restored.FailureSignals(), signal) {
				t.Fatalf("rollback erased failure: %v", restored.FailureSignals())
			}
			t.Logf("rollback=%s durable signal=%s", rollback, signal)
		})
	}
}
