package rendered

import "sdo.dev/operator/sdk"

// DeployedImage is captured by the trusted deployer from its applied document
// and verified rollout, never reconstructed from later healthy observations.
type DeployedImage struct {
	Ref     sdk.ObjectRef `json:"ref"`
	Name    string        `json:"name"`
	Init    bool          `json:"init"`
	Applied string        `json:"applied"`
	Digest  string        `json:"digest"`
}

// DeploymentEvidence comes from the broker's private source-deployment ledger.
// It is deliberately separate from the untrusted snapshot being validated.
type DeploymentEvidence struct {
	SourceTree string
	Images     []DeployedImage
}

func (e DeploymentEvidence) Image(ref sdk.ObjectRef, name string, init bool) (DeployedImage, bool) {
	for _, image := range e.Images {
		if image.Ref == ref && image.Name == name && image.Init == init {
			return image, true
		}
	}
	return DeployedImage{}, false
}

// IsImmutableImage recognizes the digest identities required by locks.
func IsImmutableImage(image string) bool { return immutableImage.MatchString(image) }
