package main

import (
	"context"
	"fmt"
	"os"
	"sdo.dev/operator/runtime"
)

func main() {
	if err := runtime.RunObservationCheck(context.Background(), os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
