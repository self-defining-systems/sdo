package imagesrc

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

func dockerConfig(auths string) []byte { return []byte(`{"auths":{` + auths + `}}`) }

func TestFromDockerConfigMatchesHostsInEveryKeySpelling(t *testing.T) {
	pair := base64.StdEncoding.EncodeToString([]byte("user:secret"))
	data := dockerConfig(`"https://reg.example/v2/":{"auth":"` + pair + `"},` +
		`"other.example:5000":{"username":"u2","password":"p2"},` +
		`"https://index.docker.io/v1/":{"auth":"` + pair + `"}`)
	provider := RegistryLoginsFromDockerConfig(func(context.Context) ([]byte, error) { return data, nil })
	for host, want := range map[string]RegistryLogin{
		"reg.example":          {Username: "user", Password: "secret"},
		"other.example:5000":   {Username: "u2", Password: "p2"},
		"docker.io":            {Username: "user", Password: "secret"},
		"registry-1.docker.io": {Username: "user", Password: "secret"},
	} {
		got, ok, err := provider.For(context.Background(), host)
		if err != nil || !ok || got != want {
			t.Fatalf("%s: got %+v ok=%v err=%v, want %+v", host, got, ok, err, want)
		}
	}
	if _, ok, err := provider.For(context.Background(), "unlisted.example"); ok || err != nil {
		t.Fatalf("unlisted host must be anonymous, got ok=%v err=%v", ok, err)
	}
}

func TestFromDockerConfigReadsEveryCallSoRotationApplies(t *testing.T) {
	password := "one"
	provider := RegistryLoginsFromDockerConfig(func(context.Context) ([]byte, error) {
		return dockerConfig(`"reg.example":{"username":"u","password":"` + password + `"}`), nil
	})
	first, _, _ := provider.For(context.Background(), "reg.example")
	password = "two"
	second, _, _ := provider.For(context.Background(), "reg.example")
	if first.Password != "one" || second.Password != "two" {
		t.Fatalf("credentials were cached: %q then %q", first.Password, second.Password)
	}
}

func TestFromDockerConfigRejectsMalformedInputWithoutLeakingIt(t *testing.T) {
	for name, data := range map[string][]byte{
		"not json":       []byte("hunter2"),
		"bad auth":       dockerConfig(`"reg.example":{"auth":"!!not-base64!!"}`),
		"auth no colon":  dockerConfig(`"reg.example":{"auth":"` + base64.StdEncoding.EncodeToString([]byte("nocolon")) + `"}`),
		"identity token": dockerConfig(`"reg.example":{"identitytoken":"tok"}`),
	} {
		provider := RegistryLoginsFromDockerConfig(func(context.Context) ([]byte, error) { return data, nil })
		_, _, err := provider.For(context.Background(), "reg.example")
		if err == nil {
			t.Fatalf("%s: want an error", name)
		}
		for _, secret := range []string{"hunter2", "nocolon", "\"tok\""} {
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("%s: error leaks credential material: %v", name, err)
			}
		}
	}
}

func TestFromDockerConfigPropagatesLoadErrors(t *testing.T) {
	boom := errors.New("boom")
	provider := RegistryLoginsFromDockerConfig(func(context.Context) ([]byte, error) { return nil, boom })
	if _, _, err := provider.For(context.Background(), "reg.example"); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}
