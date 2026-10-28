package sdktest

import (
	"encoding/json"
	"fmt"
	"sdo.dev/operator/sdk/program"
)

// ScriptOrigin checks linkage to an original successful timed receipt of the
// case's incident. Every recorded mutation path qualifies (RunScript, sdo act,
// observed API writes); when the receipt recorded script bytes, they must be
// these bytes.
func ScriptOrigin(script program.RunScript, c CorpusCase) (program.ScriptOriginReceipt, error) {
	id := c.ScriptOrigins[program.ScriptDigest(script.Script)]
	if id == "" || c.Provenance.IncidentID == "" {
		return program.ScriptOriginReceipt{}, fmt.Errorf("RunScript requires digest linkage to its original successful receipt")
	}
	for _, call := range c.Recordings {
		if call.Method != "RepairReceipt" || call.Error != "" {
			continue
		}
		var receipt program.ScriptOriginReceipt
		if json.Unmarshal(call.Result, &receipt) != nil || receipt.ID != id {
			continue
		}
		if receipt.IncidentID != c.Provenance.IncidentID || !receipt.Success || receipt.StartedAt.IsZero() || receipt.CompletedAt.IsZero() || receipt.CompletedAt.Before(receipt.StartedAt) {
			return receipt, fmt.Errorf("RunScript origin receipt lacks a successful timed incident mutation")
		}
		if receipt.ScriptDigest != "" && receipt.ScriptDigest != program.ScriptDigest(script.Script) {
			return receipt, fmt.Errorf("RunScript origin receipt records different script bytes")
		}
		return receipt, nil
	}
	return program.ScriptOriginReceipt{}, fmt.Errorf("RunScript original successful receipt %q unavailable", id)
}
