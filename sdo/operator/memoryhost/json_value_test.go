package memoryhost

import (
	"encoding/json"
	"fmt"
	"google.golang.org/protobuf/types/known/structpb"
	"math"
	"reflect"
	"sdo.dev/operator/sdk/traffic"
	"testing"
	"time"
)

func TestObservationJSONMatchesProtobufJSONSemantics(t *testing.T) {
	cases := []*structpb.Struct{
		{},
		{Fields: map[string]*structpb.Value{"nested": structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"list": structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{structpb.NewNullValue(), structpb.NewBoolValue(true), structpb.NewStringValue("<>&\n\u2028"), structpb.NewNumberValue(1.25)}})}})}},
		{Fields: map[string]*structpb.Value{"nan": structpb.NewNumberValue(math.NaN())}},
		{Fields: map[string]*structpb.Value{"infinity": structpb.NewNumberValue(math.Inf(1))}},
		{Fields: map[string]*structpb.Value{"unset": {}}},
		{Fields: map[string]*structpb.Value{"nil": nil}},
		{Fields: map[string]*structpb.Value{"bad": structpb.NewStringValue(string([]byte{0xff}))}},
		{Fields: map[string]*structpb.Value{string([]byte{0xff}): structpb.NewNullValue()}},
		{Fields: map[string]*structpb.Value{"empty-object": structpb.NewStructValue(nil), "empty-list": structpb.NewListValue(nil)}},
	}
	for index, value := range cases {
		raw, wantErr := value.MarshalJSON()
		var want map[string]any
		if wantErr == nil {
			wantErr = json.Unmarshal(raw, &want)
		}
		got, gotErr := decode[map[string]any](value)
		if (gotErr != nil) != (wantErr != nil) {
			t.Errorf("case %d: errors differ: protobuf=%v native=%v", index, wantErr, gotErr)
			continue
		}
		if gotErr == nil && !reflect.DeepEqual(*got, want) {
			t.Errorf("case %d: JSON values differ: got=%v want=%v", index, *got, want)
		}
	}
}

// Traffic observations carry nested arrays in live RPCs; a tiny empty Struct
// misses the JSON/protobuf bridge cost entirely.
func BenchmarkObservationDecode(b *testing.B) {
	window := traffic.Window{}
	for i := 0; i < 3; i++ {
		scenario := traffic.ScenarioObservations{Scenario: traffic.Descriptor{ID: fmt.Sprintf("synthetic-%d", i)}, Qualified: true}
		for j := 0; j < 200; j++ {
			scenario.Samples = append(scenario.Samples, traffic.Sample{At: time.Unix(1000+int64(j), 0), Iteration: uint64(j), Status: 200, Outcome: traffic.OutcomeOK, Requests: 1, Request: "GET /synthetic"})
		}
		window.Scenarios = append(window.Scenarios, scenario)
	}
	raw, err := json.Marshal(window)
	if err != nil {
		b.Fatal(err)
	}
	payload := &structpb.Struct{}
	if err := payload.UnmarshalJSON(raw); err != nil {
		b.Fatal(err)
	}
	for _, native := range []bool{false, true} {
		name := "protobuf-json"
		if native {
			name = "native-json"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if native {
					if _, err := decode[traffic.Window](payload); err != nil {
						b.Fatal(err)
					}
				} else {
					raw, err := payload.MarshalJSON()
					if err != nil {
						b.Fatal(err)
					}
					var result traffic.Window
					if err := json.Unmarshal(raw, &result); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
