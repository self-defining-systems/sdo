// Package outcome names the values of OutcomeRecord.classification. The
// permitted set is stated once, as the `in` rule in outcome.proto; these
// constants exist so Go code can compare against a value by name, and
// TestClassificationsMatchProto fails if the two ever drift. The durable string
// encoding uses these stable lowercase strings.
package outcome

const (
	Success              = "success"
	ClearedWithoutAction = "cleared_without_action"
	Failed               = "failed"
	// Unconfirmed is a verified repair whose closure-time recheck never saw a
	// complete observation window. It is neither success nor failure evidence.
	Unconfirmed = "unconfirmed"
	// Escalated is a closure whose responder ended (or timed out) before
	// Recovered held; the incident stayed open until its opening alerts cleared.
	// It routes the fault to a human, is never success and grants no repair
	// learning. A confirmed out-of-reach cause (OutOfReachConfirmed) additionally
	// asks reflection for a Diagnose-only playbook.
	Escalated = "escalated"
)
