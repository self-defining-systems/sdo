// The shell controller is built into the image once, independently of accepted
// detector scripts. Go code paths continue using their generated controller.
package main

import (
	"context"
	"fmt"
	"os"
	runtime "sdo.dev/operator/runtime"
)

func main() {
	if err := runtime.RunWithOptions(context.Background(), nil, runtime.RuntimeOptions{}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
