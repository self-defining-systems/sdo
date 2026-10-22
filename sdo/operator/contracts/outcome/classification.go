// Package outcome names the values of OutcomeRecord.classification. The
// permitted set is stated once, as the `in` rule in outcome.proto; these
// constants exist so Go code can compare against a value by name, and
// TestClassificationsMatchProto fails if the two ever drift. The durable string
// encoding is unchanged.
package outcome

const (
	Success              = "success"
	ClearedWithoutAction = "cleared_without_action"
	Failed               = "failed"
)
