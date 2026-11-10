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
	// Escalated is a closure whose health never returned but whose responder
	// confirmed an out-of-reach cause from controller observations taken
	// before any repair (OutOfReachConfirmed). It routes the cause to a human;
	// it is never success and grants no repair learning.
	Escalated = "escalated"
)
