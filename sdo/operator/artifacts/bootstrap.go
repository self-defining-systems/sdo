// Package artifacts adapts lifecycle's logical bootstrap contract to memory.
package artifacts

import "sdo.dev/operator/memory"

type BootstrapStore struct{}

func (BootstrapStore) ReadArchitecture(checkout string) (string, error) {
	return memory.ReadArchitecture(checkout)
}

func (BootstrapStore) WriteGoal(checkout, application, objective string) error {
	return memory.WriteGoal(checkout, application, objective)
}

func (BootstrapStore) WriteSchema(checkout string) error {
	return memory.WriteSchema(checkout)
}

func (BootstrapStore) WriteArchitecture(checkout, application, acceptedSourceCommit, fingerprint, summary string) error {
	return memory.WriteArchitecture(checkout, application, acceptedSourceCommit, fingerprint, summary)
}
