package deploywire

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func pushSecret(namespace, config string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "push"},
		Type:       corev1.SecretTypeDockerConfigJson,
		Data:       map[string][]byte{corev1.DockerConfigJsonKey: []byte(config)},
	}
}

// The registry login the installer already hands SDO (a dockerconfigjson Secret
// in the control namespace) also serves image resolution, and is read on every
// lookup so a rotated Secret applies.
func TestRegistryLoginReadsTheControlNamespaceSecretEachTime(t *testing.T) {
	client := fake.NewSimpleClientset(pushSecret("ctl", `{"auths":{"reg.synthetic.invalid":{"username":"u","password":"one"}}}`))
	provider := RegistryLogin(client, "ctl", "push")
	first, ok, err := provider.For(context.Background(), "reg.synthetic.invalid")
	if err != nil || !ok || first.Password != "one" {
		t.Fatalf("first = %+v ok=%v err=%v", first, ok, err)
	}
	if _, err := client.CoreV1().Secrets("ctl").Update(context.Background(), pushSecret("ctl", `{"auths":{"reg.synthetic.invalid":{"username":"u","password":"two"}}}`), metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	second, _, _ := provider.For(context.Background(), "reg.synthetic.invalid")
	if second.Password != "two" {
		t.Fatalf("rotated password not read: %+v", second)
	}
}

// A missing Secret means no login (anonymous); it must not break resolving
// public images.
func TestRegistryLoginMissingSecretIsAnonymous(t *testing.T) {
	provider := RegistryLogin(fake.NewSimpleClientset(), "ctl", "push")
	if _, ok, err := provider.For(context.Background(), "reg.synthetic.invalid"); ok || err != nil {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
}

func TestDeployerWiresTheRegistryLoginIntoThePrebuiltSource(t *testing.T) {
	client := fake.NewSimpleClientset(pushSecret("ctl", `{"auths":{"reg.synthetic.invalid":{"username":"u","password":"pw"}}}`))
	identity := fullIdentity()
	source := identity.prebuiltSource(client, "ctl")
	if source.Credentials == nil {
		t.Fatal("a configured registry Secret must reach the prebuilt source")
	}
	identity.RegistryPushSecret = ""
	if identity.prebuiltSource(client, "ctl").Credentials != nil {
		t.Fatal("no registry Secret must mean no credential provider")
	}
}
