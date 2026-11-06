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

// Recreation records an object the trusted deployer deleted and created again
// because the API server refused to update an immutable field in place (a
// changed Job template, a changed workload selector). It is information about
// a deployment, like DeployedImage; no identity comparison depends on it.
type Recreation struct {
	Ref sdk.ObjectRef `json:"ref"`
	// Fields are the immutable fields the API server named.
	Fields []string `json:"fields"`
	// Propagation is the delete propagation: Background for a finished Job,
	// Orphan for a workload whose pods the recreated workload replaces.
	Propagation string `json:"propagation"`
	// Reason is the recreate policy's typed reason (deployment.RecreateReason).
	Reason string `json:"reason"`
	// Replaced lists the orphaned dependents (Kind/namespace/name) the
	// deployer removed once the recreated workload was ready; dependents the
	// recreated workload adopted are not listed.
	Replaced []string `json:"replaced,omitempty"`
}
