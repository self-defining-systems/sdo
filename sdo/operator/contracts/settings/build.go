package settings

import "time"

// DefaultBuildTimeout is the deadline of one source image build. Real source
// builds compile their dependencies: openresty-thrift (social-network) spent
// 393 s in its apt step alone on a loaded host and was still in the OpenResty
// compile at the old 10 minute limit. 30 minutes leaves about 3x the observed
// cold-build time of the heaviest known image.
const DefaultBuildTimeout = 30 * time.Minute

// DefaultBuildBudget bounds all image builds of one deployment attempt: the
// three heavy social-network images plus the light ones fit in an hour on a
// loaded host, and a single wedged build cannot hold the attempt longer.
const DefaultBuildBudget = 60 * time.Minute

// DefaultRolloutTimeout is how long the deployment verifier waits for one
// applied workload to finish its rollout. A real app's pod is not ready at
// image pull: social-network's media-frontend runs an alpine/git init
// container that clones sources for 2.5-3 minutes before the main container
// starts, and a node under a heavy parallel build is slower still. 10 minutes
// is about 3x that observed cold start, and a pod that is failing rather than
// slow is reported as failing (typed), not as a time limit.
const DefaultRolloutTimeout = 10 * time.Minute
