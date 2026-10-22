// sdo-native-receipt evaluates one benchmark receipt per invocation. A rejected
// incident is a successful transport exchange with a typed failure result.
package main

import (
	"fmt"
	"io"
	"os"

	"google.golang.org/protobuf/encoding/protojson"
	receipt "sdo.dev/benchmarks/sregym/receipt"
	c "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func main() {
	build := false
	if len(os.Args) != 2 || (os.Args[1] != "build" && os.Args[1] != "validate") {
		fmt.Fprintln(os.Stderr, "usage: sdo-native-receipt build|validate < receipt.json")
		os.Exit(2)
	}
	build = os.Args[1] == "build"
	const limit = 16 << 20
	data, err := io.ReadAll(io.LimitReader(os.Stdin, limit+1))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var result *c.NativeReceiptResult
	if len(data) > limit {
		result = &c.NativeReceiptResult{Failure: &c.ReceiptFailure{Code: c.ReceiptFailure_CODE_INVALID_WIRE, Message: "native receipt exceeds 16 MiB"}}
	} else {
		result = receipt.Evaluate(data, build)
	}
	encoded, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(result)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if _, err := fmt.Fprintln(os.Stdout, string(encoded)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
