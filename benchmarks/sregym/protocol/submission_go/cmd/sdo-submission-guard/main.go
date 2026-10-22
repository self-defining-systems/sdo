package main

import (
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"
	"io"
	"os"
	submission "sdo.dev/benchmarks/sregym/submission"
	c "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func main() {
	const limit = 1 << 20
	data, err := io.ReadAll(io.LimitReader(os.Stdin, limit+1))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var result *c.SubmissionGuardDecision
	if len(data) > limit {
		result = &c.SubmissionGuardDecision{Failure: &c.SubmissionGuardFailure{Code: c.SubmissionGuardFailure_CODE_INVALID_WIRE, Message: "submission guard input exceeds 1 MiB"}}
	} else {
		result = submission.Decide(data)
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
