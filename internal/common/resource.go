package common

import (
	"context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var log = ctrl.Log.WithName("resourceFetcher")

type ResourceClient struct {
	Ctx       context.Context
	Client    client.Client
	Namespace string
}

// NewResourceClient new resource client
func NewResourceClient(ctx context.Context, client client.Client, namespace string) *ResourceClient {
	if namespace == "" {
		namespace = metav1.NamespaceDefault
	}
	return &ResourceClient{
		Ctx:       ctx,
		Client:    client,
		Namespace: namespace,
	}
}

func (r *ResourceClient) Get(obj client.Object) error {
	name := obj.GetName()
	kind := obj.GetObjectKind()
	if err := r.Client.Get(r.Ctx, client.ObjectKey{Namespace: r.Namespace, Name: name}, obj); err != nil {
		opt := []any{"ns", r.Namespace, "name", name, "kind", kind}
		if apierrors.IsNotFound(err) {
			log.Error(err, "Fetch resource NotFound", opt...)
		} else {
			log.Error(err, "Fetch resource occur some unknown err", opt...)
		}
		return err
	}
	return nil
}

type InstanceAttributes interface {
	RoleConfigSpec
	GetClusterConfig() any

	GetNamespace() string

	GetInstanceName() string
}
