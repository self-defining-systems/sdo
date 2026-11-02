package validate

import (
	"context"
	"sdo.dev/operator/memory/layout"
	"strings"
	"testing"
)

func TestRV4RenderedOwnershipMatrix(t *testing.T) {
	for _, identity := range []struct {
		actor Actor
		phase Phase
	}{{Responder, ProposalPhase}, {Responder, ReflectionPhase}, {Deployer, InitialPhase}, {HealthJudge, InitialPhase}, {Human, InitialPhase}, {Upkeep, InitialPhase}} {
		for _, file := range []string{layout.RenderedLockFile, layout.RenderedDir + "/" + strings.Repeat("a", 64) + ".yaml", layout.RenderedDir + "/arbitrary.txt"} {
			t.Run(string(identity.actor)+"/"+string(identity.phase)+"/"+file, func(t *testing.T) {
				root := goodTree(t)
				write(t, root, ".gitignore", "/.sdo/rendered/\n")
				commit(t, root, "base")
				base := gitIn(t, root, "rev-parse", "HEAD")
				write(t, root, file, "{}\n")
				diagnostics, err := Validate(context.Background(), Request{Root: root, Base: base, Actor: identity.actor, Phase: identity.phase, Layers: Ownership})
				if err != nil {
					t.Fatal(err)
				}
				denied := false
				for _, d := range diagnostics {
					if d.Rule == RuleOwnershipDenied && d.Path == file {
						denied = true
					}
				}
				if !denied {
					t.Fatalf("ignored rendered edit bypassed ownership: %+v", diagnostics)
				}
			})
		}
	}
}
