package memoryhost

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/appstate"
	"sdo.dev/operator/sdk/podread"
)

// podReadReader requires a pod read the way an authored reader does.
type podReadReader struct{ stateReader }

func (podReadReader) ReadState(_ context.Context, view appstate.View) (appstate.Values, error) {
	var out appstate.Values
	result, err := podread.Require(view, "appstate-settings")
	if err != nil {
		return out, err
	}
	out.Set("out", result.Readings[0].Stdout)
	return out, nil
}

func TestReadStateCarriesTheReadersPodReads(t *testing.T) {
	server := readStateServer(t, podReadReader{stateReader{id: "settings"}})
	wire, err := encodeStruct(podread.Result{
		Query:    podread.Query{ID: "appstate-settings"},
		Readings: []podread.Reading{{Pod: "store-0", Container: "store", Stdout: "limit=5"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.ReadState(context.Background(), &pb.ReadStateRequest{ReaderId: "settings", PodReads: map[string]*structpb.Struct{"appstate-settings": wire}})
	if err != nil || response.Error != "" || response.Values["out"] != "limit=5" {
		t.Fatalf("with pod reads: response=%v err=%v", response, err)
	}
	response, err = server.ReadState(context.Background(), &pb.ReadStateRequest{ReaderId: "settings"})
	if err != nil || !strings.Contains(response.Error, "no collected result") {
		t.Fatalf("without pod reads: response=%v err=%v, want an error naming the missing result", response, err)
	}
}
