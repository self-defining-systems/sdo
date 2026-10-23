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
	Inputs  ImageInputs   `json:"inputs,omitzero"`
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

// ImageInputs is copied from the trusted deployment plan, never from a lock or
// live Pod. SourceImage with empty paths declares an image with no local build.
// The zero value denotes unknown inputs and retains the all-source fallback.
type ImageInputs struct {
	SourceImage    string `json:"source_image"`
	ContextPath    string `json:"context_path,omitempty"`
	DockerfilePath string `json:"dockerfile_path,omitempty"`
}
