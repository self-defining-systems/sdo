package runtime

import (
	"regexp"

	"sdo.dev/operator/rendered"
)

var gitRevision = regexp.MustCompile(`^[a-fA-F0-9]{40,64}$`)

// GitPlanSource reads durable accepted deployment locks; raw revision reads are
// retained for explicit historical source inspection.
type GitPlanSource = rendered.Source
type SourceImages = rendered.SourceImages

var ErrAcceptedManifestAbsent = rendered.ErrAcceptedManifestAbsent
