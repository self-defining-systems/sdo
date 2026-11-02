//go:build integration && !race

package runtime

import "time"

const rehearsalBudget = 110 * time.Second
