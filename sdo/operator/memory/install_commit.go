package memory

// InstallCommitMessage is the message of a commit Go writes into the
// application repository while installing (the standard goal, the deployment
// plan, the generic detectors). Go produced and checked the content itself, so
// the commit carries the same SDO-Validation trailer a broker commit does:
// a reader that admits only validated setup commits then admits exactly
// SDO's own writes, and rejects an engineer's or an agent's unstamped commit.
func InstallCommitMessage(subject string) string {
	return subject + "\n\nSDO-Actor: human\nSDO-Phase: initial\nSDO-Origin: install-existing\nSDO-Validation: passed"
}
