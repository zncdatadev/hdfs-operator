/*
Copyright 2024 zncdatadev.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"cmp"
	"context"
	"slices"

	authv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/authentication/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlhandler "sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
)

// ExternalDependencies declares objects referenced directly by the HdfsCluster. GenericReconciler
// validates these before building any role, preventing Pods from entering CreateContainerConfigError
// because the ZooKeeper ConfigMap or oauth2-proxy credentials do not exist.
func ExternalDependencies(cr *hdfsv1alpha1.HdfsCluster) []reconciler.Dependency {
	var dependencies []reconciler.Dependency
	if cr.Spec.ClusterConfig == nil {
		return dependencies
	}
	dependencies = append(dependencies, reconciler.Dependency{
		Kind: reconciler.DependencyConfigMap,
		Name: cr.Spec.ClusterConfig.ZookeeperConfigMapName,
	})
	if auth := cr.Spec.ClusterConfig.Authentication; auth != nil && auth.Oidc != nil {
		dependencies = append(dependencies, reconciler.Dependency{
			Kind: reconciler.DependencySecret,
			Name: auth.Oidc.ClientCredentialsSecret,
		})
	}
	return dependencies
}

// DependencyWatches maps changes to referenced ConfigMaps, Secrets and AuthenticationClasses back
// to every HdfsCluster that names the object. GenericReconciler's dependency reads do not create
// watches, and these shared inputs are not controller-owned by the cluster CR.
func DependencyWatches(k8sClient client.Client) []func(*builder.Builder) *builder.Builder {
	mapper := ctrlhandler.EnqueueRequestsFromMapFunc(dependencyRequestMapper(k8sClient))
	return []func(*builder.Builder) *builder.Builder{
		func(b *builder.Builder) *builder.Builder { return b.Watches(&corev1.ConfigMap{}, mapper) },
		func(b *builder.Builder) *builder.Builder { return b.Watches(&corev1.Secret{}, mapper) },
		func(b *builder.Builder) *builder.Builder {
			return b.Watches(&authv1alpha1.AuthenticationClass{}, mapper)
		},
	}
}

func dependencyRequestMapper(k8sClient client.Client) ctrlhandler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		clusters := &hdfsv1alpha1.HdfsClusterList{}
		if err := k8sClient.List(ctx, clusters, client.InNamespace(obj.GetNamespace())); err != nil {
			log.FromContext(ctx).Error(err, "Failed to list HdfsClusters for dependency event",
				"kind", obj.GetObjectKind().GroupVersionKind().Kind,
				"namespace", obj.GetNamespace(), "name", obj.GetName())
			return nil
		}

		requests := make([]reconcile.Request, 0)
		for i := range clusters.Items {
			cluster := &clusters.Items[i]
			if referencesDependency(cluster, obj) {
				requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
			}
		}
		slices.SortFunc(requests, func(a, b reconcile.Request) int {
			return cmp.Compare(a.String(), b.String())
		})
		return requests
	}
}

func referencesDependency(cr *hdfsv1alpha1.HdfsCluster, obj client.Object) bool {
	if cr.Spec.ClusterConfig == nil {
		return false
	}
	auth := cr.Spec.ClusterConfig.Authentication
	switch obj.(type) {
	case *corev1.ConfigMap:
		return cr.Spec.ClusterConfig.ZookeeperConfigMapName == obj.GetName()
	case *corev1.Secret:
		return auth != nil && auth.Oidc != nil && auth.Oidc.ClientCredentialsSecret == obj.GetName()
	case *authv1alpha1.AuthenticationClass:
		return auth != nil && auth.Oidc != nil && auth.AuthenticationClass == obj.GetName()
	default:
		return false
	}
}
