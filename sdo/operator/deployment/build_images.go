package deployment

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/rendered"
)

// BuildReport is what a turn-time build did: the images Go's deploy would
// build from the committed tree that were selected, the digest of each one
// built when every build passed, and the joined build failures otherwise.
type BuildReport struct {
	Tree   string
	Images []string
	Built  map[string]string
	Err    error
}

// Text is the report a person or the deployer agent reads: one line per
// image, then each failure (a BuildKit failure carries its step and log tail).
func (r BuildReport) Text() string {
	var b strings.Builder
	tree := r.Tree
	if len(tree) > 12 {
		tree = tree[:12]
	}
	fmt.Fprintf(&b, "Built from tree %s (your last commit; uncommitted files are not built):\n", tree)
	failed := failedImages(r.Err)
	for _, image := range r.Images {
		switch {
		case failed[image]:
			fmt.Fprintf(&b, "  failed  %s\n", image)
		case r.Built[image] != "":
			fmt.Fprintf(&b, "  ok      %s  %s\n", image, r.Built[image])
		case r.Err == nil || len(failed) > 0:
			fmt.Fprintf(&b, "  ok      %s\n", image)
		default:
			fmt.Fprintf(&b, "  stopped %s\n", image)
		}
	}
	if r.Err != nil {
		fmt.Fprintf(&b, "\n%v\n", r.Err)
	} else {
		b.WriteString("Every selected image built and was pushed; Go's deploy of this commit reuses the layer cache.\n")
	}
	return b.String()
}

func failedImages(err error) map[string]bool {
	out := map[string]bool{}
	var walk func(error)
	walk = func(err error) {
		if err == nil {
			return
		}
		if joined, ok := err.(interface{ Unwrap() []error }); ok {
			for _, e := range joined.Unwrap() {
				walk(e)
			}
			return
		}
		var attributed *imagesrc.Error
		if errors.As(err, &attributed) {
			out[attributed.Image] = true
		}
	}
	walk(err)
	return out
}

// BuildImages builds images of the checkout's committed tree through the
// deploy's own path (render, image rule, BuildKit, registry layer cache), but
// applies nothing and pins no prebuilt image. names selects images by rendered
// reference, repository, or the repository's last path segment; none selects
// every image the deploy would build. A build that passes here builds again
// from cache when Go deploys the same commit. The error is for a request that
// cannot build at all (render, policy, an unknown name); build failures are in
// the report.
func (d Deployer) BuildImages(ctx context.Context, checkout string, plan *contracts.DeploymentPlan, names []string) (BuildReport, error) {
	if err := d.validate(); err != nil {
		return BuildReport{}, err
	}
	in, err := d.prepare(ctx, checkout, plan)
	if err != nil {
		return BuildReport{}, err
	}
	defer in.release()
	var buildable []string
	seen := map[string]bool{}
	for _, image := range rendered.ImageSites(in.set) {
		declared, ok := imagesrc.Match(in.declared, image)
		if ok && declared.SourceID != d.Images.Default() && !seen[image] {
			seen[image] = true
			buildable = append(buildable, image)
		}
	}
	if len(buildable) == 0 {
		return BuildReport{}, errors.New("the committed tree renders no image Go builds: every image is prebuilt (only images under the trusted registry with a Dockerfile, or an images entry in the override, are built)")
	}
	selected := buildable
	if len(names) > 0 {
		selected = nil
		for _, name := range names {
			matched := false
			for _, image := range buildable {
				repository := imagesrc.Repository(image)
				if name == image || name == repository || name == path.Base(repository) {
					matched = true
					if !slices.Contains(selected, image) {
						selected = append(selected, image)
					}
				}
			}
			if !matched {
				return BuildReport{}, fmt.Errorf("%q is not an image this commit builds; it builds: %s", name, strings.Join(buildable, ", "))
			}
		}
	}
	var declared []imagesrc.Declared
	for _, image := range selected {
		d, _ := imagesrc.Match(in.declared, image)
		if !slices.ContainsFunc(declared, func(o imagesrc.Declared) bool { return o.Image == d.Image }) {
			declared = append(declared, d)
		}
	}
	report := BuildReport{Tree: in.effective.ID, Images: selected}
	resolved, err := d.Images.ResolveAll(ctx, selected, declared, in.request)
	if err != nil {
		if ctx.Err() != nil {
			return BuildReport{}, err
		}
		report.Err = err
		return report, nil
	}
	report.Built = make(map[string]string, len(resolved))
	for image, r := range resolved {
		report.Built[image] = r.Ref
	}
	return report, nil
}
