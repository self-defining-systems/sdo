package runtime

import (
	"context"
	"os/exec"
	"regexp"
	"strings"
)

var programIDPath = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

func fillProgramRevisions(ctx context.Context, runs []ProgramRun, repository, commit string) {
	cache := map[string]string{}
	for index := range runs {
		id := runs[index].ProgramID
		revision, ok := cache[id]
		if !ok {
			revision = commit
			if programIDPath.MatchString(id) && gitRevision.MatchString(commit) {
				output, err := exec.CommandContext(ctx, "git", "-C", repository, "rev-parse", commit+":.sdo/playbooks/"+id).Output()
				if err == nil {
					revision = strings.TrimSpace(string(output))
				}
			}
			cache[id] = revision
		}
		runs[index].ProgramRevision = revision
	}
}
