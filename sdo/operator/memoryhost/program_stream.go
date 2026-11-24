package memoryhost

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"google.golang.org/protobuf/proto"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/programread"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
)

// MaxMessageBytes caps each memory-host gRPC frame.
const MaxMessageBytes = 16 << 20

func (s *Server) RunProgram(stream pb.MemoryHostService_RunProgramServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	invocation := first.GetInvocation()
	if invocation == nil {
		return errors.New("expected a program invocation")
	}

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	var exchangeMu sync.Mutex
	result, err := s.executeProgram(ctx, invocation, func(call programread.Message) (programread.Message, error) {
		exchangeMu.Lock()
		defer exchangeMu.Unlock()
		if err := ctx.Err(); err != nil {
			return programread.Message{}, err
		}
		request := &pb.ProgramReadCall{Method: call.Method, Arguments: asBytes(call.Arguments)}
		frame := &pb.RunProgramResponse{Frame: &pb.RunProgramResponse_Read{Read: request}}
		if proto.Size(frame) > MaxMessageBytes {
			return programread.Message{}, sdk.Classified("program read request exceeds the memory-host message limit", sdk.ErrInvalidRead)
		}
		if err := stream.Send(frame); err != nil {
			return programread.Message{}, err
		}
		incoming, err := stream.Recv()
		if err != nil {
			return programread.Message{}, err
		}
		reply := incoming.GetRead()
		if reply == nil {
			return programread.Message{}, errors.New("expected a program read reply")
		}
		answer := programread.Message{Values: asJSON(reply.Values), Error: reply.Error, ErrorClass: reply.ErrorClass}
		if reply.UsageUnavailable != nil {
			answer.UsageUnavailable = &metrics.UsageUnavailableError{Reason: *reply.UsageUnavailable}
		}
		return answer, nil
	})
	cancel()
	if err != nil {
		return err
	}
	if err := stream.Context().Err(); err != nil {
		return err
	}
	exchangeMu.Lock()
	defer exchangeMu.Unlock()
	return stream.Send(result)
}

// The SDK JSON wire remains raw bytes in protobuf.
func asBytes(values []json.RawMessage) [][]byte {
	out := make([][]byte, len(values))
	for i, value := range values {
		out[i] = value
	}
	return out
}

func asJSON(values [][]byte) []json.RawMessage {
	out := make([]json.RawMessage, len(values))
	for i, value := range values {
		out[i] = value
	}
	return out
}

// CallProgram keeps the read callback local to this invocation. Closing the
// stream releases both peers on an early error, including an invalid frame.
func CallProgram(ctx context.Context, client pb.MemoryHostServiceClient, invocation *pb.MemoryProgramInvocation, read func(programread.Message) programread.Message) (*pb.RunProgramResponse, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.RunProgram(ctx)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&pb.RunProgramRequest{Frame: &pb.RunProgramRequest_Invocation{Invocation: invocation}}); err != nil {
		return nil, err
	}
	for {
		frame, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		switch payload := frame.GetFrame().(type) {
		case *pb.RunProgramResponse_Read:
			if payload.Read == nil {
				return nil, errors.New("empty program read call")
			}
			if err := replyToProgram(ctx, stream, payload.Read, read); err != nil {
				return nil, err
			}
		case *pb.RunProgramResponse_Verdict, *pb.RunProgramResponse_Plan, *pb.RunProgramResponse_Error:
			return frame, nil
		default:
			return nil, errors.New("invalid program response")
		}
	}
}

func replyToProgram(ctx context.Context, stream pb.MemoryHostService_RunProgramClient, call *pb.ProgramReadCall, read func(programread.Message) programread.Message) error {
	// The read view owns its own source bounds. A slow read must not keep the
	// program stream or its host lease alive after this invocation expires.
	done := make(chan programread.Message, 1)
	go func() { done <- read(programread.Message{Method: call.Method, Arguments: asJSON(call.Arguments)}) }()
	var answer programread.Message
	select {
	case answer = <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	reply := &pb.ProgramReadReply{Values: asBytes(answer.Values), Error: answer.Error, ErrorClass: answer.ErrorClass}
	if answer.UsageUnavailable != nil {
		reply.UsageUnavailable = &answer.UsageUnavailable.Reason
	}
	frame := &pb.RunProgramRequest{Frame: &pb.RunProgramRequest_Read{Read: reply}}
	if proto.Size(frame) > MaxMessageBytes {
		frame.Frame = &pb.RunProgramRequest_Read{Read: &pb.ProgramReadReply{
			Error: "program read result exceeds the memory-host message limit", ErrorClass: sdk.ReadClassUnavailable,
		}}
	}
	return stream.Send(frame)
}
