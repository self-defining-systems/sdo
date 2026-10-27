package runtime

import (
	"regexp"
	"sdo.dev/operator/rendered"
)

var gitRevision = regexp.MustCompile(`^[a-fA-F0-9]{40,64}$`)

// GitPlanSource renders accepted source as it is written; raw revision reads are
// retained for explicit historical source inspection.
type GitPlanSource = rendered.Source

var ErrAcceptedManifestAbsent = rendered.ErrAcceptedManifestAbsent
