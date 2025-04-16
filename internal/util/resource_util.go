package util

import (
	"context"
	"fmt"

	"github.com/cisco-open/k8s-objectmatcher/patch"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	logger = ctrl.Log.WithName("util")
)

func handleServiceUpdate(current, obj *corev1.Service) {
	// Preserve the ClusterIP when updating the service
	obj.Spec.ClusterIP = current.Spec.ClusterIP

	if obj.Spec.Type == corev1.ServiceTypeNodePort || obj.Spec.Type == corev1.ServiceTypeLoadBalancer {
		for i := range obj.Spec.Ports {
			obj.Spec.Ports[i].NodePort = current.Spec.Ports[i].NodePort
		}
	}
}

func handleObjectUpdate(current, obj client.Object) []patch.CalculateOption {
	calculateOpt := []patch.CalculateOption{patch.IgnoreStatusFields()}

	switch v := obj.(type) {
	case *corev1.Service:
		handleServiceUpdate(current.(*corev1.Service), v)
	case *appsv1.StatefulSet:
		calculateOpt = append(calculateOpt, patch.IgnoreVolumeClaimTemplateTypeMetaAndStatus())
	}

	return calculateOpt
}

func updateObject(ctx context.Context, c client.Client, current, obj client.Object, calculateOpt []patch.CalculateOption) (bool, error) {
	result, err := patch.DefaultPatchMaker.Calculate(current, obj, calculateOpt...)
	if err != nil {
		logger.Error(err, "failed to calculate patch to match objects, moving on to update")
		return updateWithResourceVersion(ctx, c, current, obj)
	}

	if !result.IsEmpty() {
		kinds, _, _ := scheme.Scheme.ObjectKinds(obj)
		logger.Info(
			fmt.Sprintf("Resource update for object %s:%s", kinds, obj.(metav1.ObjectMetaAccessor).GetObjectMeta().GetName()),
			"patch", string(result.Patch),
		)

		if err := patch.DefaultAnnotator.SetLastAppliedAnnotation(obj); err != nil {
			logger.Error(err, "failed to annotate modified object", "object", obj)
		}

		return updateWithResourceVersion(ctx, c, current, obj)
	}

	logger.V(1).Info(fmt.Sprintf("Skipping update for object %s:%s",
		obj.GetObjectKind().GroupVersionKind().Kind,
		obj.(metav1.ObjectMetaAccessor).GetObjectMeta().GetName()))
	return false, nil
}

func updateWithResourceVersion(ctx context.Context, c client.Client, current, obj client.Object) (bool, error) {
	resourceVersion := current.(metav1.ObjectMetaAccessor).GetObjectMeta().GetResourceVersion()
	obj.(metav1.ObjectMetaAccessor).GetObjectMeta().SetResourceVersion(resourceVersion)

	if err := c.Update(ctx, obj); err != nil {
		return false, err
	}
	return true, nil
}

func CreateOrUpdate(ctx context.Context, c client.Client, obj client.Object) (bool, error) {
	key := client.ObjectKeyFromObject(obj)
	namespace := obj.GetNamespace()
	kinds, _, _ := scheme.Scheme.ObjectKinds(obj)
	name := obj.GetName()

	logger.V(5).Info("Creating or updating object", "Kind", kinds, "Namespace", namespace, "Name", name)

	current := obj.DeepCopyObject().(client.Object)
	err := c.Get(ctx, key, current)

	if errors.IsNotFound(err) {
		if err := patch.DefaultAnnotator.SetLastAppliedAnnotation(obj); err != nil {
			return false, err
		}
		logger.Info("Creating a new object", "Kind", kinds, "Namespace", namespace, "Name", name)

		if err := c.Create(ctx, obj); err != nil {
			return false, err
		}
		return true, nil
	}

	if err == nil {
		calculateOpt := handleObjectUpdate(current, obj)
		return updateObject(ctx, c, current, obj, calculateOpt)
	}

	return false, err
}
