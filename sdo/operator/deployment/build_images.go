package deployment

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment/internal/deploymentio"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/rendered"
)

// BuildReport is what a turn-time build did: the images Go's deploy would
// build from the committed tree that were selected and why, the digest of each
// one built when every build passed, and the joined build failures otherwise.
// Unchanged are the images not rebuilt because nothing they read changed since
// their last good build; NotNamed are images a named build left out although
// their inputs changed.
type BuildReport struct {
	Tree      string
	Images    []string
	Reasons   map[string]string
	Unchanged []string
	NotNamed  map[string]string
	Built     map[string]string
	Err       error
}

// BuildOptions are the optional parts of a turn-time build. Record selects,
// when no image is named, the images whose inputs changed since their last
// good build (nil: every image) and remembers each good build. Progress hears
// one line as the build starts and one as each image finishes.
type BuildOptions struct {
	Record   BuildRecord
	Progress func(line string)
	// Now times the progress lines; nil is the real clock.
	Now func() time.Time
}

// Text is the report a person or the deployer agent reads: one line per
// image with why it was built, then each failure (a BuildKit failure carries
// its step and log tail).
func (r BuildReport) Text() string {
	var b strings.Builder
	tree := shortTree(r.Tree)
	if len(r.Images) == 0 {
		fmt.Fprintf(&b, "Nothing to build in tree %s (your last commit): every image is unchanged since its last good build: %s.\nName images to build them anyway.\n", tree, strings.Join(r.Unchanged, ", "))
		return b.String()
	}
	fmt.Fprintf(&b, "Built from tree %s (your last commit; uncommitted files are not built):\n", tree)
	failed := failedImages(r.Err)
	for _, image := range r.Images {
		why := ""
		if reason := r.Reasons[image]; reason != "" {
			why = "  (" + reason + ")"
		}
		switch {
		case failed[image]:
			fmt.Fprintf(&b, "  failed  %s%s\n", image, why)
		case r.Built[image] != "":
			fmt.Fprintf(&b, "  ok      %s  %s%s\n", image, r.Built[image], why)
		case r.Err == nil || len(failed) > 0:
			fmt.Fprintf(&b, "  ok      %s%s\n", image, why)
		default:
			fmt.Fprintf(&b, "  stopped %s%s\n", image, why)
		}
	}
	if len(r.Unchanged) > 0 {
		fmt.Fprintf(&b, "Not rebuilt, unchanged since its last good build: %s\n", strings.Join(r.Unchanged, ", "))
	}
	if len(r.NotNamed) > 0 {
		names := make([]string, 0, len(r.NotNamed))
		for image := range r.NotNamed {
			names = append(names, image)
		}
		slices.Sort(names)
		b.WriteString("Changed since their last good build but not built (not named; run with no arguments to build every changed image):\n")
		for _, image := range names {
			fmt.Fprintf(&b, "  %s  (%s)\n", image, r.NotNamed[image])
		}
	}
	if r.Err != nil {
		fmt.Fprintf(&b, "\n%v\n", r.Err)
	} else {
		b.WriteString("Every selected image built and was pushed; Go's deploy of this commit reuses the layer cache.\n")
	}
	return b.String()
}

func shortTree(tree string) string {
	if len(tree) > 12 {
		return tree[:12]
	}
	return tree
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
// reference, repository, or the repository's last path segment. With no names
// it selects every image the deploy would build whose inputs (its source's
// BuildInputs: for BuildKit the Dockerfile, ignore files and COPY sources)
// changed since its last good build in opts.Record; an image with no good
// build, or without a Record, is always selected. A build that passes here
// builds again from cache when Go deploys the same commit. The error is for a
// request that cannot build at all (render, policy, an unknown name); build
// failures are in the report.
func (d Deployer) BuildImages(ctx context.Context, checkout string, plan *contracts.DeploymentPlan, names []string, opts BuildOptions) (BuildReport, error) {
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
	changes := buildChanges{deployer: d, in: in, record: opts.Record, diffs: map[string][]string{}}
	report := BuildReport{Tree: in.effective.ID, Reasons: map[string]string{}}
	if len(names) > 0 {
		for _, name := range names {
			matched := false
			for _, image := range buildable {
				repository := imagesrc.Repository(image)
				if name == image || name == repository || name == path.Base(repository) {
					matched = true
					if !slices.Contains(report.Images, image) {
						report.Images = append(report.Images, image)
						report.Reasons[image] = "named"
					}
				}
			}
			if !matched {
				return BuildReport{}, fmt.Errorf("%q is not an image this commit builds; it builds: %s", name, strings.Join(buildable, ", "))
			}
		}
		if opts.Record != nil {
			for _, image := range buildable {
				if slices.Contains(report.Images, image) {
					continue
				}
				if reason, changed := changes.reason(ctx, image); changed {
					if report.NotNamed == nil {
						report.NotNamed = map[string]string{}
					}
					report.NotNamed[image] = reason
				}
			}
		}
	} else {
		for _, image := range buildable {
			reason, changed := changes.reason(ctx, image)
			if changed {
				report.Images = append(report.Images, image)
				report.Reasons[image] = reason
			} else {
				report.Unchanged = append(report.Unchanged, image)
			}
		}
	}
	if len(report.Images) == 0 {
		return report, nil
	}
	var declared []imagesrc.Declared
	for _, image := range report.Images {
		d, _ := imagesrc.Match(in.declared, image)
		if !slices.ContainsFunc(declared, func(o imagesrc.Declared) bool { return o.Image == d.Image }) {
			declared = append(declared, d)
		}
	}
	request := in.request
	request.Finished = progressReporter(report, opts)
	if opts.Progress != nil {
		opts.Progress(fmt.Sprintf("Building %d images from tree %s; one line as each finishes:", len(report.Images), shortTree(report.Tree)))
	}
	resolved, err := d.Images.ResolveAll(ctx, report.Images, declared, request)
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

// progressReporter records each good build in opts.Record and reports each
// finished image to opts.Progress.
func progressReporter(report BuildReport, opts BuildOptions) func(string, error) {
	var mu sync.Mutex
	done := 0
	now := opts.Now
	if now == nil {
		now = deploymentio.Now
	}
	started := now()
	return func(image string, err error) {
		mu.Lock()
		defer mu.Unlock()
		done++
		status := "ok     "
		if err != nil {
			status = "failed "
		} else if opts.Record != nil {
			if recordErr := opts.Record.RecordGood(image, report.Tree); recordErr != nil {
				slog.Warn("image_build_record_failed", "image", image, "error", recordErr)
			}
		}
		if opts.Progress != nil {
			opts.Progress(fmt.Sprintf("  %s %s  (%d of %d, %s)", status, image, done, len(report.Images), now().Sub(started).Round(time.Second)))
		}
	}
}

// buildChanges decides whether an image's inputs changed since its last good
// build, diffing each recorded tree against the current one once.
type buildChanges struct {
	deployer Deployer
	in       prepared
	record   BuildRecord
	diffs    map[string][]string
}

// reason says why image must be built, or changed=false when nothing it reads
// changed since its last good build.
func (c buildChanges) reason(ctx context.Context, image string) (string, bool) {
	if c.record == nil {
		return "", true
	}
	tree, ok, err := c.record.LastGood(image)
	if err != nil {
		return "its build record is unreadable: " + err.Error(), true
	}
	if !ok {
		return "no earlier good build", true
	}
	if tree == c.in.effective.ID {
		return "", false
	}
	declared, _ := imagesrc.Match(c.in.declared, image)
	inputs, known, err := c.deployer.Images.BuildInputs(declared, c.in.request.FS)
	if err != nil || !known {
		return "its build inputs cannot be listed, so it is always built", true
	}
	changed, cached := c.diffs[tree]
	if !cached {
		changed, err = c.in.effective.ChangedSince(ctx, tree)
		if err != nil {
			return "the tree of its last good build is not available", true
		}
		c.diffs[tree] = changed
	}
	var hits []string
	for _, p := range changed {
		if inputs.Covers(p) {
			hits = append(hits, p)
		}
	}
	switch {
	case len(hits) == 0:
		return "", false
	case len(hits) > 3:
		return fmt.Sprintf("changed since its last good build: %s and %d more", strings.Join(hits[:3], ", "), len(hits)-3), true
	default:
		return "changed since its last good build: " + strings.Join(hits, ", "), true
	}
}
