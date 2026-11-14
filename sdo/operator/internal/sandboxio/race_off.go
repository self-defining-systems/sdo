//go:build !race

package sandboxio

// dataHeadroom is zero outside race builds; see race_on.go.
const dataHeadroom = 0
