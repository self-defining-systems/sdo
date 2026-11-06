package operatorcli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/operatorcli/internal/processio"
)

// TerminationMessagePath is where Kubernetes reads a container's termination
// message; the operate Job's stop reason is visible there without log scraping.
const TerminationMessagePath = "/dev/termination-log"

// terminationMessageLimit is Kubernetes' cap on a termination message.
const terminationMessageLimit = 4096

// terminalStop is the machine-readable form of a lifecycle that ended on an
// operability decision no agent can act on. Report is omitted from the
// termination message (it does not fit) and present on the log line.
type terminalStop struct {
	Summary  operability.Summary `json:"summary"`
	Decision string              `json:"decision"`
	Owner    string              `json:"owner,omitempty"`
	Report   json.RawMessage     `json:"report,omitempty"`
}

// terminalOperability finds the operability report an error ends on: an
// *lifecycle.OperabilityError (the decision it recorded) or a
// *lifecycle.ProvidedPlanError that carries one (decided for a provided plan).
func terminalOperability(err error) (operability.Report, operability.Decision, bool) {
	var opErr *lifecycle.OperabilityError
	if errors.As(err, &opErr) {
		return opErr.Report, opErr.Decision, true
	}
	var provided *lifecycle.ProvidedPlanError
	if errors.As(err, &provided) && provided.Report != nil {
		return *provided.Report, provided.Report.DecideIn(operability.ModeProvided, operability.StageItems(provided.Report.Stage)...), true
	}
	return operability.Report{}, operability.Decision{}, false
}

// ReportTerminalOperability writes the operability report behind err, if it has
// one, to stderr as one `SDO-OPERABILITY <json>` line and its summary to the
// termination message file, and reports whether it did. A report that cannot be
// encoded is reported on stderr instead; the original error is never replaced.
func ReportTerminalOperability(err error, stderr io.Writer, terminationPath string) bool {
	return reportTerminalOperability(err, stderr, terminationPath, processio.Now())
}

func reportTerminalOperability(err error, stderr io.Writer, terminationPath string, now time.Time) bool {
	report, decision, ok := terminalOperability(err)
	if !ok {
		return false
	}
	summary, serr := operability.Summarize(report, now)
	if serr != nil {
		fmt.Fprintf(stderr, "operability report is not encodable: %v\n", serr)
		return true
	}
	stop := terminalStop{Summary: summary, Decision: string(decision.Kind), Owner: string(decision.Owner)}
	short, serr := json.Marshal(stop)
	if serr != nil || len(short) > terminationMessageLimit {
		fmt.Fprintf(stderr, "operability summary does not fit the termination message: %v\n", serr)
		return true
	}
	if terminationPath != "" {
		if werr := os.WriteFile(terminationPath, short, 0o644); werr != nil {
			fmt.Fprintf(stderr, "write termination message: %v\n", werr)
		}
	}
	if full, ferr := report.Encode(); ferr == nil {
		stop.Report = full
		if line, merr := json.Marshal(stop); merr == nil {
			fmt.Fprintf(stderr, "SDO-OPERABILITY %s\n", line)
		}
	}
	return true
}
