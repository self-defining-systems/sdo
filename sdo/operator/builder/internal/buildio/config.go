package buildio

import "os"

func DefaultOperatorRoot() string {
	root := os.Getenv("SDO_OPERATOR_ROOT")
	if root == "" {
		return "/opt/sdo/sdo/operator"
	}
	return root
}
