package validate

import (
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// Proto is the one conversion from a Diagnostic to its wire form. The CLI's
// JSON output and any other transport of validation findings go through it.
func (d Diagnostic) Proto() *contracts.MemoryDiagnostic {
	return &contracts.MemoryDiagnostic{Rule: d.Rule, Path: d.Path, Line: int32(d.Line), Message: d.Message, Hint: d.Hint}
}

// ProtoDiagnostics converts a diagnostic list, preserving order. The result is
// never nil.
func ProtoDiagnostics(diagnostics []Diagnostic) []*contracts.MemoryDiagnostic {
	converted := make([]*contracts.MemoryDiagnostic, 0, len(diagnostics))
	for _, d := range diagnostics {
		converted = append(converted, d.Proto())
	}
	return converted
}
