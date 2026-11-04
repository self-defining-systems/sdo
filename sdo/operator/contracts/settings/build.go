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
