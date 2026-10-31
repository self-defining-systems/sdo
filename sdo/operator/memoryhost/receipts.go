package memoryhost

import (
	"encoding/json"
	"errors"
	"google.golang.org/protobuf/types/known/structpb"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/sdktest"
)

func EncodeReceipts(calls []sdktest.PrimitiveCall) ([]*pb.MemoryReadReceipt, error) {
	var out []*pb.MemoryReadReceipt
	for _, call := range calls {
		args, result := &structpb.Value{}, &structpb.Value{}
		if err := args.UnmarshalJSON(call.Arguments); err != nil {
			return nil, err
		}
		if err := result.UnmarshalJSON(call.Result); err != nil {
			return nil, err
		}
		out = append(out, &pb.MemoryReadReceipt{Operation: call.Method, Arguments: args, Result: result, Error: call.Error})
	}
	return out, nil
}
func DecodeReceipts(receipts []*pb.MemoryReadReceipt) ([]sdktest.PrimitiveCall, error) {
	var out []sdktest.PrimitiveCall
	for _, r := range receipts {
		if r == nil || r.Arguments == nil || r.Result == nil {
			return nil, errors.New("incomplete read receipt")
		}
		args, err := r.Arguments.MarshalJSON()
		if err != nil {
			return nil, err
		}
		result, err := r.Result.MarshalJSON()
		if err != nil {
			return nil, err
		}
		var arguments []json.RawMessage
		if err = json.Unmarshal(args, &arguments); err != nil {
			return nil, err
		}
		out = append(out, sdktest.PrimitiveCall{Method: r.Operation, Arguments: args, Result: result, Error: r.Error})
	}
	return out, nil
}
