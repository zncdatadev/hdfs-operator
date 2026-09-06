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

// Package extensions holds HDFS ClusterExtensions — cluster-level hooks that run around the
// SDK's role-group reconciliation.
package extensions

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	listenerv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/listeners/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/common"
	"github.com/zncdatadev/operator-go/pkg/config"
	"github.com/zncdatadev/operator-go/pkg/constant"
	"github.com/zncdatadev/operator-go/pkg/listener"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlhandler "sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constants"
	"github.com/zncdatadev/hdfs-operator/internal/product"
)

const listenerKind = "Listener"

// DiscoveryExtension publishes a cluster-level discovery ConfigMap (named after the cluster)
// containing the client-facing core-site.xml / hdfs-site.xml, so external clients can reach the
// HA NameNodes. It is a ClusterExtension so it runs once per cluster (not per role group), after
// the role groups — and their Services — have been reconciled.
type DiscoveryExtension struct {
	common.BaseExtension
}

// NewDiscoveryExtension creates the discovery ClusterExtension.
func NewDiscoveryExtension() *DiscoveryExtension {
	return &DiscoveryExtension{BaseExtension: common.NewBaseExtension("hdfs-discovery")}
}

// PreReconcile is a no-op for discovery.
func (e *DiscoveryExtension) PreReconcile(_ context.Context, _ client.Client, _ *hdfsv1alpha1.HdfsCluster) error {
	return nil
}

// PostReconcile renders the discovery config and applies the ConfigMap via the SDK's shared
// ensure-helper (idempotent CreateOrUpdate + owner reference + canonical labels).
func (e *DiscoveryExtension) PostReconcile(ctx context.Context, k8sClient client.Client, hdfs *hdfsv1alpha1.HdfsCluster) error {
	endpoints, ready, err := resolveDiscoveryEndpoints(ctx, k8sClient, hdfs)
	if err != nil {
		return fmt.Errorf("resolve discovery endpoints: %w", err)
	}
	if !ready {
		// Listener publication is an asynchronous CSI/external-controller operation. A missing or
		// partial endpoint is normal during startup and rolling replacement, so preserve the last
		// complete ConfigMap and let the Listener/PodListeners watches (plus the framework health
		// cadence) trigger another pass instead of publishing a mixed internal/external snapshot.
		log.FromContext(ctx).Info("NameNode listeners are not ready; preserving the last complete discovery ConfigMap")
		return nil
	}

	generator := config.NewMultiFormatConfigGenerator()
	generator.RegisterDefaultFormats()
	data, err := generator.GenerateFiles(product.DiscoveryConfig(hdfs, endpoints))
	if err != nil {
		return fmt.Errorf("render discovery config: %w", err)
	}

	return reconciler.EnsureDiscoveryConfigMap(ctx, k8sClient, k8sClient.Scheme(), hdfs, hdfs.Name, data,
		reconciler.WithDiscoveryProductName(constants.ProductName),
	)
}

// resolveDiscoveryEndpoints starts with every NameNode's stable cluster DNS endpoint and replaces
// the entries of external listener classes with the per-Pod address published by the Listener CSI
// driver. PodListeners is intentional here: for a NodePort Listener, Listener.status contains all
// node addresses, while PodListeners records the address selected for this concrete Pod.
func resolveDiscoveryEndpoints(
	ctx context.Context, k8sClient client.Client, hdfs *hdfsv1alpha1.HdfsCluster,
) (map[string]product.DiscoveryEndpoint, bool, error) {
	endpoints := product.InternalNameNodeDiscoveryEndpoints(hdfs)
	if hdfs.Spec.NameNodes == nil {
		return endpoints, true, nil
	}

	groups := make([]string, 0, len(hdfs.Spec.NameNodes.RoleGroups))
	for group := range hdfs.Spec.NameNodes.RoleGroups {
		groups = append(groups, group)
	}
	slices.Sort(groups)

	for _, group := range groups {
		listenerClass, err := product.RoleGroupListenerClass(hdfs, hdfsv1alpha1.NameNodeRoleName, group)
		if err != nil {
			return nil, false, fmt.Errorf("resolve listener class for NameNode role group %q: %w", group, err)
		}
		if listenerClass == listener.ListenerClassClusterInternal {
			continue
		}

		resourceName := reconciler.RoleGroupResourceName(hdfs.Name, hdfsv1alpha1.NameNodeRoleName, group)
		roleGroup := hdfs.Spec.NameNodes.RoleGroups[group]
		for ordinal := range roleGroup.GetReplicas() {
			podName := fmt.Sprintf("%s-%d", resourceName, ordinal)
			endpoint, found, err := podListenerEndpoint(ctx, k8sClient, hdfs.Namespace, podName, tlsEnabled(hdfs))
			if err != nil {
				return nil, false, err
			}
			if !found {
				return endpoints, false, nil
			}
			endpoints[podName] = endpoint
		}
	}

	return endpoints, true, nil
}

func podListenerEndpoint(
	ctx context.Context, k8sClient client.Client, namespace, podName string, tls bool,
) (product.DiscoveryEndpoint, bool, error) {
	pod := &corev1.Pod{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return product.DiscoveryEndpoint{}, false, nil
		}
		return product.DiscoveryEndpoint{}, false, fmt.Errorf("get NameNode pod %s/%s: %w", namespace, podName, err)
	}
	if pod.UID == "" {
		return product.DiscoveryEndpoint{}, false, nil
	}

	podListeners := &listenerv1alpha1.PodListeners{}
	key := types.NamespacedName{Namespace: namespace, Name: "pod-" + string(pod.UID)}
	if err := k8sClient.Get(ctx, key, podListeners); err != nil {
		if apierrors.IsNotFound(err) {
			return product.DiscoveryEndpoint{}, false, nil
		}
		return product.DiscoveryEndpoint{}, false, fmt.Errorf("get PodListeners %s: %w", key, err)
	}

	mountedListener, ok := podListeners.Spec.Listeners[hdfsv1alpha1.ListenerVolumeName]
	if !ok {
		return product.DiscoveryEndpoint{}, false, nil
	}
	ingresses := slices.Clone(mountedListener.ListenerIngresses)
	slices.SortFunc(ingresses, func(a, b listenerv1alpha1.IngressAddressSpec) int {
		if byType := cmp.Compare(string(a.AddressType), string(b.AddressType)); byType != 0 {
			return byType
		}
		return cmp.Compare(a.Address, b.Address)
	})
	for _, ingress := range ingresses {
		if validNameNodeIngress(ingress, tls) {
			return product.DiscoveryEndpoint{Address: ingress.Address, Ports: maps.Clone(ingress.Ports)}, true, nil
		}
	}
	return product.DiscoveryEndpoint{}, false, nil
}

func validNameNodeIngress(ingress listenerv1alpha1.IngressAddressSpec, tls bool) bool {
	if ingress.Address == "" || ingress.Ports[hdfsv1alpha1.RpcName] == 0 || ingress.Ports[hdfsv1alpha1.HttpName] == 0 {
		return false
	}
	return !tls || ingress.Ports[hdfsv1alpha1.HttpsName] != 0
}

func tlsEnabled(hdfs *hdfsv1alpha1.HdfsCluster) bool {
	return hdfs.Spec.ClusterConfig != nil && hdfs.Spec.ClusterConfig.Authentication != nil &&
		hdfs.Spec.ClusterConfig.Authentication.Tls != nil
}

// DiscoveryWatches maps Listener and PodListeners changes back to the owning HdfsCluster. The
// listener resources are created by the CSI driver rather than by GenericReconciler, so they do
// not carry a direct HdfsCluster owner reference and must use an explicit map watch.
func DiscoveryWatches(k8sClient client.Client) []func(*builder.Builder) *builder.Builder {
	mapper := ctrlhandler.EnqueueRequestsFromMapFunc(discoveryRequestMapper(k8sClient))
	return []func(*builder.Builder) *builder.Builder{
		func(b *builder.Builder) *builder.Builder {
			return b.Watches(&listenerv1alpha1.Listener{}, mapper)
		},
		func(b *builder.Builder) *builder.Builder {
			return b.Watches(&listenerv1alpha1.PodListeners{}, mapper)
		},
	}
}

func discoveryRequestMapper(k8sClient client.Client) ctrlhandler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		listenerName := listenerNameForObject(obj)
		if !strings.HasSuffix(listenerName, "-"+hdfsv1alpha1.ListenerVolumeName) {
			return nil
		}
		podName := strings.TrimSuffix(listenerName, "-"+hdfsv1alpha1.ListenerVolumeName)
		pod := &corev1.Pod{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: obj.GetNamespace(), Name: podName}, pod); err != nil {
			return nil
		}
		labels := pod.GetLabels()
		if labels[constant.LabelKubernetesName] != constants.ProductName ||
			labels[constant.LabelKubernetesComponent] != hdfsv1alpha1.NameNodeRoleName {
			return nil
		}
		clusterName := labels[constant.LabelKubernetesInstance]
		if clusterName == "" {
			return nil
		}
		return []reconcile.Request{{NamespacedName: types.NamespacedName{
			Namespace: pod.Namespace,
			Name:      clusterName,
		}}}
	}
}

func listenerNameForObject(obj client.Object) string {
	switch typed := obj.(type) {
	case *listenerv1alpha1.Listener:
		return typed.Name
	case *listenerv1alpha1.PodListeners:
		for _, owner := range typed.OwnerReferences {
			if owner.APIVersion == listenerv1alpha1.GroupVersion.String() && owner.Kind == listenerKind {
				return owner.Name
			}
		}
	}
	return ""
}

// OnReconcileError is a no-op for discovery.
func (e *DiscoveryExtension) OnReconcileError(_ context.Context, _ client.Client, _ *hdfsv1alpha1.HdfsCluster, _ error) error {
	return nil
}

// Ensure DiscoveryExtension satisfies the SDK ClusterExtension contract.
var _ common.ClusterExtension[*hdfsv1alpha1.HdfsCluster] = &DiscoveryExtension{}
