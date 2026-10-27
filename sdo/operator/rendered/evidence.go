package rendered

import "sdo.dev/operator/sdk"

// DeployedImage is captured by the trusted deployer from its applied document
// and verified rollout. It is information about a deployment; no diff,
// reconcile or closure depends on it.
type DeployedImage struct {
	Ref     sdk.ObjectRef `json:"ref"`
	Name    string        `json:"name"`
	Init    bool          `json:"init"`
	Applied string        `json:"applied"`
	Digest  string        `json:"digest"`
	Inputs  ImageInputs   `json:"inputs,omitzero"`
}

// ImageInputs is copied from the trusted deployment plan, never from a lock or
// live Pod. SourceImage with empty paths declares an image with no local build.
// The zero value denotes unknown inputs and retains the all-source fallback.
type ImageInputs struct {
	SourceImage    string `json:"source_image"`
	ContextPath    string `json:"context_path,omitempty"`
	DockerfilePath string `json:"dockerfile_path,omitempty"`
}
