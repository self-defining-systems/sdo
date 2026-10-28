package broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// TestUpstreamCrossHostLane is one lane of a scripted multi-host check against
// a real remote; it is skipped unless SDO_UPSTREAM_E2E_URL is set. Modes:
//   - seed: push the synthetic accepted fixture to the branch
//   - lane: publish outcomes, a lane-owned playbook and a shared playbook
//     concurrently with a peer lane, resolving a memory conflict by rebase
//   - sync: sync and print the accepted head and tree
func TestUpstreamCrossHostLane(t *testing.T) {
	url, branch, lane, mode := os.Getenv("SDO_UPSTREAM_E2E_URL"), os.Getenv("SDO_UPSTREAM_E2E_BRANCH"), os.Getenv("SDO_UPSTREAM_E2E_LANE"), os.Getenv("SDO_UPSTREAM_E2E_MODE")
	if url == "" {
		t.Skip("SDO_UPSTREAM_E2E_URL is not set")
	}
	ctx := context.Background()
	if mode == "seed" {
		seed, _, _ := sharedReflectionFixture(t)
		git(t, seed, "push", "--quiet", url, "HEAD:refs/heads/"+branch)
		fmt.Printf("SEED %s\n", git(t, seed, "rev-parse", "HEAD"))
		return
	}
	root := filepath.Join(t.TempDir(), "accepted")
	if err := cloneGit(ctx, url, root, "--branch", branch); err != nil {
		t.Fatal(err)
	}
	b := sharedMemoryBroker(t, root, func(context.Context, string, []string) error { return nil })
	if err := b.SetUpstream(Upstream{URL: url, Branch: branch}); err != nil {
		t.Fatal(err)
	}
	if mode == "lane" {
		if start, err := strconv.ParseInt(os.Getenv("SDO_UPSTREAM_E2E_START"), 10, 64); err == nil {
			time.Sleep(time.Until(time.Unix(start, 0)))
		}
		conflicts := 0
		for round := range 3 {
			id := fmt.Sprintf("%s-%d", lane, round)
			if _, err := b.RecordOutcome(ctx, id, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: id, Classification: "success"}); err != nil {
				t.Fatal(err)
			}
			book := "shared"
			if round != 1 {
				book = fmt.Sprintf("%s-own-%d", lane, round)
			}
			p := reflectionProposal(t, b, id)
			writeLearning(t, p, book, "Learning from "+id)
			for attempt := 1; ; attempt++ {
				_, err := publishLearning(b, id, book)
				if err == nil {
					break
				}
				var conflict *MemoryConflict
				if !errors.As(err, &conflict) || attempt > 3 {
					t.Fatalf("%s: %v", id, err)
				}
				conflicts++
				rebased, err := b.RebaseProposal(ctx, id, ReflectionPhase)
				if err != nil {
					t.Fatal(err)
				}
				if resolved := resolveCollisionsByUnion(t, rebased.Path); resolved == 0 {
					t.Fatalf("%s: rebased checkout carries no collision markers", id)
				}
				writeLearning(t, rebased, book, "Learning from every lane including "+id)
			}
		}
		fmt.Printf("CONFLICTS %s %d\n", lane, conflicts)
	}
	if err := b.SyncUpstream(ctx); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("HEAD %s %s TREE %s\n", lane, git(t, root, "rev-parse", "HEAD"), git(t, root, "rev-parse", "HEAD^{tree}"))
}

// resolveCollisionsByUnion stands in for the reflection agent: it keeps both
// sides of every diff3 collision and drops the common ancestor.
func resolveCollisionsByUnion(t *testing.T, root string) int {
	t.Helper()
	resolved := 0
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			if entry != nil && entry.IsDir() && entry.Name() == ".git" {
				return filepath.SkipDir
			}
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil || !strings.Contains(string(data), "<<<<<<< ") {
			return err
		}
		var kept []string
		base := false
		for _, line := range strings.SplitAfter(string(data), "\n") {
			switch {
			case strings.HasPrefix(line, "<<<<<<< "), strings.HasPrefix(line, ">>>>>>> "):
				base = false
			case strings.HasPrefix(line, "||||||| "):
				base = true
			case line == "=======\n":
				base = false
			case !base:
				kept = append(kept, line)
			}
		}
		resolved++
		return os.WriteFile(path, []byte(strings.Join(kept, "")), 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}
