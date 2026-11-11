// Package artifacts adapts lifecycle's logical bootstrap contract to memory.
package artifacts

import (
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
)

type BootstrapStore struct{}

// ReadArchitecture is the architecture text of the detection designer's
// brief: the file's text, then the probe coverage obligations Go computes from
// its application map.
func (BootstrapStore) ReadArchitecture(checkout string) (string, error) {
	text, err := memory.ReadArchitecture(checkout)
	if err != nil {
		return "", err
	}
	if obligations := memory.CoverageObligations(checkout); obligations != "" {
		text += "\n" + obligations
	}
	return text, nil
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
