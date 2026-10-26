package runtime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sdo.dev/operator/internal/testutil"
	"strconv"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/memory"
)

// TestRealSeedHermeticReplay is the real-seed hermetic replay: a hand-written
// replay slot's source edits, applied to a copy of a refreshed benchmark seed,
// driven through the real controller, broker, outcome store and strict receipt
// path with a kill/restart at each durable boundary. The seed is read only.
//
//	RT1_SEED_ROOT=<seed repository> RT1_SEED_SLOT=0,6 [RT1_SEED_COMMIT=<sha>] [RT1_SEED_CUTS=a,b]
func TestRealSeedHermeticReplay(t *testing.T) {
	seed := os.Getenv(realSeedRootEnv)
	if seed == "" {
		t.Skip("set " + realSeedRootEnv + " and " + realSeedSlotEnv + " to replay a real seed")
	}
	slots := []int{}
	for _, raw := range strings.Split(os.Getenv(realSeedSlotEnv), ",") {
		slot, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil {
			t.Fatalf("%s must list replay slots: %v", realSeedSlotEnv, err)
		}
		slots = append(slots, slot)
	}
	cuts := restartClassCuts
	if raw := os.Getenv("RT1_SEED_CUTS"); raw != "" {
		cuts = strings.Split(raw, ",")
	}
	for _, slot := range slots {
		for _, cut := range cuts {
			t.Run(fmt.Sprintf("slot=%d/%s", slot, cut), func(t *testing.T) {
				started := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
				defer cancel()
				t.Setenv(realSeedSlotEnv, strconv.Itoa(slot))
				directory := t.TempDir()
				root := filepath.Join(directory, "accepted")
				testutil.CloneGit(t, seed, root, "--no-hardlinks")
				if commit := os.Getenv(realSeedCommitEnv); commit != "" {
					nativeGit(t, root, "checkout", "-q", commit)
				}
				nativeGit(t, root, "config", "user.name", "Test")
				nativeGit(t, root, "config", "user.email", "test@example.com")
				revision, err := memory.AcceptedHealthRevision(ctx, root)
				if err != nil || revision.NeedsRefresh {
					t.Fatalf("seed health is not accepted: %+v %v", revision, err)
				}
				world := restartClassSeedWorld(t, slot, root)
				restartClassBoundary(t, ctx, world, cut, directory, root, revision.Commit)
				t.Logf("slot=%d cut=%s wall=%s", slot, cut, time.Since(started))
			})
		}
	}
}
