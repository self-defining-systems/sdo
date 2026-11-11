package hostio

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// The prober reads a scenario's Secret key from the application namespace,
// reuses it within the TTL and sees a rotation after it.
func TestSecretCredentialsReadsTheApplicationSecret(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "app-token", Namespace: "app"}, Data: map[string][]byte{"token": []byte("one")}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "app-token", Namespace: "other"}, Data: map[string][]byte{"token": []byte("foreign")}})
	now := time.Unix(1000, 0)
	source := &SecretCredentials{Client: client, Namespace: "app", Now: func() time.Time { return now }}
	if value, err := source.Value(ctx, "app-token", "token"); err != nil || value != "one" {
		t.Fatalf("value = %q, %v", value, err)
	}
	if _, err := client.CoreV1().Secrets("app").Update(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "app-token", Namespace: "app"}, Data: map[string][]byte{"token": []byte("two")}}, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if value, _ := source.Value(ctx, "app-token", "token"); value != "one" {
		t.Fatalf("within the TTL the cached value is reused, got %q", value)
	}
	now = now.Add(CredentialTTL)
	if value, _ := source.Value(ctx, "app-token", "token"); value != "two" {
		t.Fatalf("rotation not seen after the TTL: %q", value)
	}
	if _, err := source.Value(ctx, "app-token", "missing"); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("missing key: %v", err)
	}
}
