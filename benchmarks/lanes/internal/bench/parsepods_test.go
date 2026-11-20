package bench

import "testing"

func TestParsePodsReadsEveryContainersRestarts(t *testing.T) {
	m, err := parsePods([]byte(`{"items":[{"metadata":{"namespace":"ns","name":"a"},"status":{"containerStatuses":[{"name":"c","restartCount":5}],"initContainerStatuses":[{"name":"i","restartCount":1}]}},{"metadata":{"namespace":"ns","name":"b"},"status":{}}]}`))
	if err != nil || len(m) != 2 || m["ns/a/c"] != 5 || m["ns/a/i"] != 1 {
		t.Fatalf("%v %v", m, err)
	}
	if _, err := parsePods([]byte("not json")); err == nil {
		t.Fatal("garbage is an error, which is no reading")
	}
}
