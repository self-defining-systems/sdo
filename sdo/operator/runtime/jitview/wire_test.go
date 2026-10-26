package jitview

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"sync"
	"testing"

	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestReadViewEveryMethodRoundTripsAndCannotWrite(t *testing.T) {
	source := &sdktest.ReplayReadView{}
	contract := reflect.TypeFor[program.ReadView]()
	for i := 0; i < contract.NumMethod(); i++ {
		method := contract.Method(i)
		t.Run(method.Name, func(t *testing.T) {
			var args []reflect.Value
			request := Message{Method: method.Name}
			for j := 0; j < method.Type.NumIn(); j++ {
				arg := reflect.Zero(method.Type.In(j))
				args = append(args, arg)
				raw, err := json.Marshal(arg.Interface())
				if err != nil {
					t.Fatal(err)
				}
				request.Arguments = append(request.Arguments, raw)
			}
			response := read(source, request)
			var returned, called bytes.Buffer
			if err := json.NewEncoder(&returned).Encode(response); err != nil {
				t.Fatal(err)
			}
			proxy := &View{Input: json.NewDecoder(&returned), Output: json.NewEncoder(&called)}
			got := reflect.ValueOf(proxy).MethodByName(method.Name).Call(args)
			want := reflect.ValueOf(source).MethodByName(method.Name).Call(args)
			for j := range got {
				if got[j].Type() == reflect.TypeFor[error]() {
					errorText := func(value reflect.Value) string {
						if value.IsNil() {
							return ""
						}
						return value.Interface().(error).Error()
					}
					if errorText(got[j]) != errorText(want[j]) {
						t.Fatalf("read error changed: %v / %v", got[j], want[j])
					}
				} else if !reflect.DeepEqual(got[j].Interface(), want[j].Interface()) {
					t.Fatalf("read result changed: %v / %v", got[j], want[j])
				}
			}
			var sent Message
			if err := json.Unmarshal(called.Bytes(), &sent); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(sent, request) {
				t.Fatalf("read arguments changed: %+v / %+v", sent, request)
			}
		})
	}
	for _, method := range []string{"Apply", "Patch", "Delete", "Exec", "DoesNotExist"} {
		if response := read(source, Message{Method: method}); response.Error == "" {
			t.Fatalf("non-read method accepted: %s", method)
		}
	}
}

func TestReadViewConcurrentCallsKeepTheirResponses(t *testing.T) {
	requestsR, requestsW := io.Pipe()
	responsesR, responsesW := io.Pipe()
	defer requestsR.Close()
	defer requestsW.Close()
	defer responsesR.Close()
	defer responsesW.Close()
	proxy := &View{Input: json.NewDecoder(responsesR), Output: json.NewEncoder(requestsW)}
	source := &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app"}}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		decoder := json.NewDecoder(requestsR)
		encoder := json.NewEncoder(responsesW)
		for {
			var message Message
			if decoder.Decode(&message) != nil {
				return
			}
			if encoder.Encode(read(source, message)) != nil {
				return
			}
		}
	}()
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Go(func() {
			for j := 0; j < 10; j++ {
				if got := proxy.Namespace(); got != "app" {
					t.Errorf("concurrent response mixed: %q", got)
				}
				_ = proxy.Services()
			}
		})
	}
	workers.Wait()
	_ = requestsW.Close()
	<-finished
}
