package runtime

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

// No unqualified JSON or protobuf alternative: unsupported metadata negotiation
// must fail closed. client-go's metadata.Interface permits full-object fallback,
// so use explicit requests and a decoder that only knows metadata types instead.
const secretMetadataAccept = "application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1"
const secretMetadataListAccept = "application/json;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1"

func (t *StateTracker) secretMetadataRequest(options metav1.ListOptions, accept string) *rest.Request {
	return t.config.MetadataClient.Get().Namespace(t.config.Namespace).Resource("secrets").
		SetHeader("Accept", accept).VersionedParams(&options, metav1.ParameterCodec)
}

func (t *StateTracker) listSecretMetadata(ctx context.Context, options metav1.ListOptions) (*metav1.PartialObjectMetadataList, error) {
	object, err := t.secretMetadataRequest(options, secretMetadataListAccept).Do(ctx).Get()
	if err != nil {
		return nil, err
	}
	list, ok := object.(*metav1.PartialObjectMetadataList)
	if !ok {
		return nil, fmt.Errorf("expected Secret PartialObjectMetadataList, got %T", object)
	}
	return list, nil
}

func (t *StateTracker) secretMetadataInformer(ctx context.Context) cache.SharedIndexInformer {
	informer := cache.NewSharedIndexInformer(&cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (k8sruntime.Object, error) {
			return t.listSecretMetadata(ctx, options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.Watch = true
			return t.secretMetadataRequest(options, secretMetadataAccept).Watch(ctx)
		},
	}, &metav1.PartialObjectMetadata{}, 0, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	// Annotations can contain last-applied Secret data. Keep only the identity
	// fields needed for drift and the managed-by label used by sdoOwned.
	_ = informer.SetTransform(func(object any) (any, error) {
		meta, ok := object.(*metav1.PartialObjectMetadata)
		if !ok {
			return nil, fmt.Errorf("expected Secret PartialObjectMetadata, got %T", object)
		}
		var labels map[string]string
		if meta.Labels["app.kubernetes.io/managed-by"] == "sdo" {
			labels = map[string]string{"app.kubernetes.io/managed-by": "sdo"}
		}
		return &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{
			Name: meta.Name, Namespace: meta.Namespace, UID: meta.UID, ResourceVersion: meta.ResourceVersion,
			Labels: labels,
		}}, nil
	})
	return informer
}
