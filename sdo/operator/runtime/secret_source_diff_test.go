package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sdo.dev/operator/rendered"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"sdo.dev/operator/internal/programread"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/sdktest"
)

type secretDiffSource struct {
	declaringSource
	data map[string][]byte
	err  error
}

func (s secretDiffSource) AcceptedSecretData(context.Context, string, string, sdk.ObjectRef) (map[string][]byte, error) {
	return s.data, s.err
}

func TestSecretSourceDiffSerializesOnlyKeyNames(t *testing.T) {
	const accepted = "canary-accepted-secret"
	const live = "canary-observed-secret"
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "credentials"}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: ref.Namespace, Name: ref.Name, Annotations: map[string]string{"ignored": live}},
		Data:       map[string][]byte{"changed": []byte(live), "added": []byte(live), "unchanged": []byte(accepted)},
	}
	source := secretDiffSource{data: map[string][]byte{"changed": []byte(accepted), "removed": []byte(accepted), "unchanged": []byte(accepted)}}
	view := LiveReadView{Carrier: inputs.New(absentWorld()), Client: fake.NewSimpleClientset(secret), Source: source, SourceCommit: liveViewCommit}
	cycle := NewCycleReadView(view)
	diff, err := cycle.DiffAgainstSource(ref)
	if err != nil || !diff.Changed || diff.SourceAbsent || diff.LiveAbsent {
		t.Fatalf("diff=%+v err=%v", diff, err)
	}
	field, err := diff.Field("data.changed")
	if err != nil || !field.Differs() {
		t.Errorf("changed declared key: %+v, %v", field, err)
	}
	restored := secret.DeepCopy()
	restored.Data["changed"] = []byte(accepted)
	restored.Data["removed"] = []byte(accepted)
	view.Client = fake.NewSimpleClientset(restored)
	if got, err := view.DiffAgainstSource(ref); err != nil || got.Changed || !strings.Contains(got.Text, "added") {
		t.Errorf("restored declared keys with an extra live key: %+v, %v", got, err)
	}
	empty := view
	empty.Source = secretDiffSource{err: rendered.ErrSecretSourceNoLiteral}
	if got, err := empty.DiffAgainstSource(ref); err == nil || got.Changed || got.SourceAbsent {
		t.Errorf("no accepted literals: %+v, %v", got, err)
	}
	left, right, err := diff.Sides()
	if err != nil {
		t.Fatal(err)
	}
	for side, fields := range map[string]struct {
		data     any
		expected string
	}{
		"accepted": {left["data"], `{"changed":true,"removed":true}`},
		"observed": {right["data"], `{"changed":false}`},
	} {
		got, _ := json.Marshal(fields.data)
		if string(got) != fields.expected {
			t.Fatalf("%s data = %s, want %s", side, got, fields.expected)
		}
	}
	source.err = fmt.Errorf("read failure with %s", accepted)
	view.Source = source
	failed, readErr := view.DiffAgainstSource(ref)
	if readErr == nil || failed.Changed {
		t.Fatalf("failed read returned %+v, %v", failed, readErr)
	}
	hostReads, err := frozenMemoryReads(cycle, []sdk.ObjectRef{ref})
	if err != nil || len(hostReads) == 0 {
		t.Fatalf("memory host projection = %v, %v", hostReads, err)
	}
	recorder := &sdktest.RecordingReadView{ReadView: cycle}
	refJSON, _ := json.Marshal(ref)
	call := programread.Message{Method: "DiffAgainstSource", Arguments: []json.RawMessage{refJSON}}
	reply := readForRemoteProgram(recorder, call)
	if reply.Error != "" || len(reply.Values) != 1 || reply.OnReply == nil {
		t.Fatalf("remote comparison reply = %+v", reply)
	}
	reply.OnReply(reply)
	failedReply := readForRemoteProgram(view, call)
	encoded, err := json.Marshal([]any{diff, cycle.SnapshotCalls(), hostReads, recorder.SnapshotCalls(), reply, failedReply})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{accepted, live} {
		for _, canary := range []string{value, base64.StdEncoding.EncodeToString([]byte(value)), fmt.Sprintf("%x", sha256.Sum256([]byte(value)))} {
			if strings.Contains(string(encoded), canary) || strings.Contains(readErr.Error(), canary) {
				t.Fatal("comparison disclosed a canary")
			}
		}
	}
}

func TestSecretSourceDiffReportsTypedReadFailures(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "credentials"}
	client := fake.NewSimpleClientset()
	liveErr := apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, ref.Name, errors.New("private text"))
	client.PrependReactor("get", "secrets", func(k8stesting.Action) (bool, kruntime.Object, error) {
		return true, nil, liveErr
	})
	view := LiveReadView{Carrier: inputs.New(absentWorld()), Client: client, SourceCommit: liveViewCommit}
	check := func(want error, text string) {
		t.Helper()
		diff, err := view.DiffAgainstSource(ref)
		if !errors.Is(err, want) || err.Error() != text || diff.Changed || diff.SourceAbsent {
			t.Fatalf("diff=%+v err=%v, want %q", diff, err, text)
		}
	}
	view.Source = secretDiffSource{data: map[string][]byte{"key": []byte("synthetic")}}
	check(sdk.ErrRefused, "Secret comparison forbidden")
	liveErr = apierrors.NewTimeoutError("private text", 1)
	check(context.DeadlineExceeded, "Secret comparison timed out")
	view.Source = secretDiffSource{err: errors.New("private renderer output")}
	check(errSecretDiffRender, "accepted Secret render failed")
	view.Source = secretDiffSource{err: rendered.ErrSecretSourceHook}
	check(sdk.ErrUnavailable, "accepted Secret has no comparable literal data")
}
