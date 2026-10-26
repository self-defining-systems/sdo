package operatorcli

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	"sdo.dev/operator/sdk/program"
)

// scriptCaptureStore keeps complete object images out of the bounded result
// envelope. Binary chunks preserve arbitrary UTF-8 boundaries and stay below the
// ConfigMap limit. First-write preimages are immutable; postimages are settled.
type scriptCaptureStore struct {
	maps                           typedcore.ConfigMapInterface
	namespace, application, jobUID string
}

const scriptImageChunkBytes = 384 << 10

func (s scriptCaptureStore) save(ctx context.Context, path string, capture program.ScriptObjectCapture) (string, error) {
	if secretCapturePath(path) || strings.EqualFold(capture.Ref.Kind, "Secret") || capture.Before != nil && strings.EqualFold(capture.Before.GetKind(), "Secret") || capture.After != nil && strings.EqualFold(capture.After.GetKind(), "Secret") {
		return "", fmt.Errorf("Secret images must not enter capture storage")
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(path)))[:16]
	name := s.application + "-object-" + digest
	labels := map[string]string{"app.kubernetes.io/managed-by": "sdo", "sdo.dev/script-capture": s.application}
	manifest, err := s.maps.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		manifest, err = s.maps.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: s.namespace, Labels: labels}, Data: map[string]string{"job_uid": s.jobUID, "path": path}}, metav1.CreateOptions{})
	}
	if err != nil {
		return "", err
	}
	if manifest.Data["job_uid"] != s.jobUID {
		return "", fmt.Errorf("script image storage identity changed")
	}
	if capture.BeforeAbsent && manifest.Data["before_chunks"] == "" {
		manifest.Data["before_chunks"] = "0"
		manifest.Data["before_absent"] = "true"
	}
	if capture.AfterAbsent {
		manifest.Data["after_chunks"] = "0"
		manifest.Data["after_absent"] = "true"
	} else if capture.After != nil {
		delete(manifest.Data, "after_absent")
	}
	ref, _ := json.Marshal(capture.Ref)
	manifest.Data["ref"] = string(ref)
	for _, stage := range []struct {
		name      string
		image     any
		present   bool
		immutable bool
	}{{"before", capture.Before, capture.Before != nil, true}, {"after", capture.After, capture.After != nil, false}} {
		if !stage.present || stage.immutable && manifest.Data[stage.name+"_chunks"] != "" {
			continue
		}
		raw, err := json.Marshal(stage.image)
		if err != nil {
			return "", err
		}
		count := 0
		for offset := 0; offset < len(raw); offset += scriptImageChunkBytes {
			end := min(len(raw), offset+scriptImageChunkBytes)
			chunkName := fmt.Sprintf("%s-%s-%d", name, stage.name, count)
			chunk := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: chunkName, Namespace: s.namespace, Labels: labels}, BinaryData: map[string][]byte{"image": raw[offset:end]}}
			existing, getErr := s.maps.Get(ctx, chunkName, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				_, err = s.maps.Create(ctx, chunk, metav1.CreateOptions{})
			} else if getErr != nil {
				err = getErr
			} else if !stage.immutable {
				existing.BinaryData = chunk.BinaryData
				_, err = s.maps.Update(ctx, existing, metav1.UpdateOptions{})
			}
			if err != nil {
				return "", err
			}
			count++
		}
		manifest.Data[stage.name+"_chunks"] = fmt.Sprint(count)
	}
	_, err = s.maps.Update(ctx, manifest, metav1.UpdateOptions{})
	if err != nil {
		return "", err
	}
	return "ConfigMap/" + s.namespace + "/" + name, nil
}

func (s scriptCaptureStore) receipt(ctx context.Context, receipt program.ScriptAPIReceipt) error {
	raw, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(raw))[:24]
	_, err = s.maps.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: s.application + "-api-" + digest, Namespace: s.namespace, Labels: map[string]string{"app.kubernetes.io/managed-by": "sdo", "sdo.dev/script-capture": s.application}}, Data: map[string]string{"job_uid": s.jobUID, "receipt.json": string(raw)}}, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}
