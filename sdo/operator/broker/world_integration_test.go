//go:build integration

package broker

import (
	"path/filepath"
	"testing"

	"sdo.dev/operator/internal/testutil"
)

// patchWorld is an onboarded world with a scratch clone of the accepted
// repository for tests that author commits outside the broker.
type patchWorld struct {
	*advanceWorld
	scratch string
}

func newPatchWorld(t *testing.T, editedByDeployer bool) *patchWorld {
	t.Helper()
	w := &patchWorld{advanceWorld: newOnboardedWorld(t, editedByDeployer)}
	w.scratch = filepath.Join(t.TempDir(), "author")
	testutil.Git(t, filepath.Dir(w.scratch), "clone", "-q", w.accepted, w.scratch)
	testutil.ConfigureGit(t, w.scratch)
	return w
}

// commitDeploymentFile commits a deployment file in a checkout, as the
// deployer agent does.
func commitDeploymentFile(t *testing.T, checkout, name string) {
	t.Helper()
	writeFiles(t, checkout, map[string]string{"deploy/" + name + ".yaml": "replicas: 2\n"})
	commitInCheckout(t, checkout, "deploy "+name, "deploy/"+name+".yaml")
}
