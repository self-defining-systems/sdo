package operatorcli

import (
	"errors"
	"flag"
)

// registerDriftTokenBudget keeps the launch entry points on the same flag name,
// default, and help text as the controller that consumes the budget.
func registerDriftTokenBudget(flags *flag.FlagSet) *int64 {
	return flags.Int64("drift-tokens-per-hour", 1000000, "repeat drift responder token budget per hour")
}

func validateDriftTokenBudget(budget int64) error {
	if budget <= 0 {
		return errors.New("drift responder token budget must be positive")
	}
	return nil
}
