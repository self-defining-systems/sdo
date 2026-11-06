package buildio

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func DefaultOperatorRoot() string {
	root := os.Getenv("SDO_OPERATOR_ROOT")
	if root == "" {
		return "/opt/sdo/sdo/operator"
	}
	return root
}

// OperatorModules lists the nested Go modules of an operator source tree: every
// direct subdirectory holding a go.mod. The tree is the single source, so a
// module added there is replaced in every synthesized go.mod and go.work
// without a second list to update.
func OperatorModules(operatorRoot string) ([]string, error) {
	entries, err := os.ReadDir(operatorRoot)
	if err != nil {
		return nil, err
	}
	var modules []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join(operatorRoot, entry.Name(), "go.mod")); err == nil {
			modules = append(modules, entry.Name())
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	if len(modules) == 0 {
		return nil, fmt.Errorf("no operator modules under %s", operatorRoot)
	}
	return modules, nil
}
