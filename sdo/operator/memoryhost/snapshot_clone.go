package memoryhost

import (
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"reflect"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// A decoded mirror contains only namespace identity and Kubernetes lists.
// Telemetry/scoped observations are attached separately to each RPC. Copy the
// Kubernetes values directly; a JSON round trip per detector repeats decoding
// and validation of the same complete namespace.
func cloneMirrorSnapshot(base sdktest.Snapshot) sdktest.Snapshot {
	clone := base
	fields := reflect.ValueOf(&clone).Elem()
	for i := 0; i < fields.NumField(); i++ {
		field := fields.Field(i)
		if field.Kind() != reflect.Slice || field.Type().Elem().Kind() != reflect.Struct || !reflect.PointerTo(field.Type().Elem()).Implements(reflect.TypeFor[kruntime.Object]()) {
			continue
		}
		if field.IsNil() {
			continue
		}
		items := reflect.MakeSlice(field.Type(), field.Len(), field.Len())
		for i := 0; i < field.Len(); i++ {
			object := field.Index(i).Addr().Interface().(kruntime.Object)
			items.Index(i).Set(reflect.ValueOf(object.DeepCopyObject()).Elem())
		}
		field.Set(items)
	}
	clone.SecretList = make([]sdk.SecretMetadata, len(base.SecretList))
	for i := range base.SecretList {
		clone.SecretList[i] = *base.SecretList[i].DeepCopy()
	}
	return clone
}
