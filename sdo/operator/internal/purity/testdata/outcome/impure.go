// This schema-free fixture must fail depguard and forbidigo in architecture CI.
package outcome

import (
	random "math/rand/v2"
	"net/http"
	clock "time"
)

func Impure() (string, int64, int) { return http.MethodGet, clock.Now().Unix(), random.Int() }
