package runtime

import (
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
)

// The cached Secret keeps every metadata fact detectors read and no value.
func TestValueFreeSecretKeepsMetadataAndDropsValues(t *testing.T) {
	changed := metav1.NewTime(time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC))
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns", Name: "token", Labels: map[string]string{"tier": "a"},
			Annotations:       map[string]string{"kubectl.kubernetes.io/last-applied-configuration": `{"data":{"k":"c2VjcmV0"}}`},
			CreationTimestamp: metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
			ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "rotator", Operation: metav1.ManagedFieldsOperationUpdate, Time: &changed,
				FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:data":{"f:k":{}}}`)}}},
		},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"k": []byte("secret")},
		StringData: map[string]string{"s": "secret"},
	}
	safe := valueFreeSecret(secret)
	want := sdk.SecretMetadataFrom(*secret)
	if got := sdk.SecretMetadataFrom(*safe); !reflect.DeepEqual(got, want) {
		t.Fatalf("cached metadata = %+v, want %+v", got, want)
	}
	if !want.DataChangedAt.Equal(changed.Time) {
		t.Fatalf("DataChangedAt = %s, want %s", want.DataChangedAt, changed.Time)
	}
	for key, value := range safe.Data {
		if value != nil {
			t.Fatalf("cached data %s holds a value", key)
		}
	}
	if len(safe.StringData) != 0 || len(safe.Annotations) != 0 || strings.Contains(safe.String(), "secret\"") {
		t.Fatalf("cached Secret keeps values: %s", safe.String())
	}
}
