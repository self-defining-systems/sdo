package runtime

import (
	"strings"

	"sdo.dev/operator/apihistory"
)

// goIssuedReceiptPrefixes name the receipt IDs only Go mints: writes derived
// from the recording of the agent's Kubernetes calls, and plan and program
// applications.
var goIssuedReceiptPrefixes = []string{apihistory.WriteIDPrefix, "sdo-program-"}

// goIssuedReceipts drops the receipts a worker result carries that Go did not
// issue. The responder no longer has a command that records its own actions,
// so a receipt of its own making is not evidence of a write.
func goIssuedReceipts(receipts []RepairActionReceipt) []RepairActionReceipt {
	var kept []RepairActionReceipt
	for _, receipt := range receipts {
		for _, prefix := range goIssuedReceiptPrefixes {
			if strings.HasPrefix(receipt.ActionID, prefix) && receipt.ActionID != prefix {
				kept = append(kept, receipt)
				break
			}
		}
	}
	return kept
}
