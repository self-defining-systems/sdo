package memory

// InstallCommitMessage is the message of a commit Go writes into the
// application repository while installing (the standard goal, the deployment
// plan, the generic detectors). It carries no SDO-Validation trailer: no
// acceptance check runs at install (the check lives in memory/validate, which
// imports this package, and these commits land on trees that are not yet a
// complete memory). Readers recognise it by SDO-Origin and the paths it
// touches, never as a validated commit.
func InstallCommitMessage(subject string) string {
	return subject + "\n\nSDO-Actor: human\nSDO-Phase: initial\nSDO-Origin: install-existing"
}
