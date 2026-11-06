// Package artifacts adapts lifecycle's logical bootstrap contract to memory.
package artifacts

import (
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
)

type BootstrapStore struct{}

func (BootstrapStore) ReadArchitecture(checkout string) (string, error) {
	return memory.ReadArchitecture(checkout)
}

func (BootstrapStore) WriteGoal(checkout, application, objective string, sources []layout.Source) error {
	return memory.WriteGoal(checkout, application, objective, sources)
}

func (BootstrapStore) WriteSchema(checkout string) error {
	return memory.WriteSchema(checkout)
}

func (BootstrapStore) WriteArchitecture(checkout, application, acceptedSourceCommit, summary string) error {
	return memory.WriteArchitecture(checkout, application, acceptedSourceCommit, summary)
}
