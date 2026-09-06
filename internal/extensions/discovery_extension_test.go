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

package extensions

import (
	"context"
	"strings"
	"testing"

	listenerv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/listeners/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/constant"
	"github.com/zncdatadev/operator-go/pkg/listener"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constants"
)

const (
	discoveryTestCluster = "hdfs"
	discoveryTestNS      = "default"
	discoveryTestPodName = "hdfs-namenode-default-0"
	discoveryTestUID     = types.UID("86a79844-96bb-4a8f-a800-08f2df1dd712")
)

func discoveryTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"core":      corev1.AddToScheme,
		"hdfs":      hdfsv1alpha1.AddToScheme,
		"listeners": listenerv1alpha1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("add %s scheme: %v", name, err)
		}
	}
	return scheme
}

func externalDiscoveryCluster() *hdfsv1alpha1.HdfsCluster {
	external := listener.ListenerClassExternalUnstable
	return &hdfsv1alpha1.HdfsCluster{
		TypeMeta: metav1.TypeMeta{APIVersion: hdfsv1alpha1.GroupVersion.String(), Kind: "HdfsCluster"},
		ObjectMeta: metav1.ObjectMeta{
			Name: discoveryTestCluster, Namespace: discoveryTestNS, UID: types.UID("cluster-uid"),
		},
		Spec: hdfsv1alpha1.HdfsClusterSpec{
			ClusterConfig: &hdfsv1alpha1.ClusterConfigSpec{},
			NameNodes: &hdfsv1alpha1.NameNodeSpec{RoleSpec: hdfsv1alpha1.RoleSpec{
				Config: &hdfsv1alpha1.ConfigSpec{ListenerClass: &external},
				RoleGroups: map[string]hdfsv1alpha1.RoleGroupSpec{
					discoveryTestNS: {Replicas: ptr.To(int32(1))},
				},
			}},
		},
	}
}

func discoveryTestPod() *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: discoveryTestPodName, Namespace: discoveryTestNS, UID: discoveryTestUID,
		Labels: map[string]string{
			constant.LabelKubernetesName:      constants.ProductName,
			constant.LabelKubernetesInstance:  discoveryTestCluster,
			constant.LabelKubernetesComponent: hdfsv1alpha1.NameNodeRoleName,
		},
	}}
}

func discoveryTestPodListeners() *listenerv1alpha1.PodListeners {
	return &listenerv1alpha1.PodListeners{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-" + string(discoveryTestUID),
			Namespace: discoveryTestNS,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: listenerv1alpha1.GroupVersion.String(), Kind: listenerKind,
				Name: discoveryTestPodName + "-" + hdfsv1alpha1.ListenerVolumeName,
			}},
		},
		Spec: listenerv1alpha1.PodListenersSpec{Listeners: map[string]listenerv1alpha1.PodListener{
			hdfsv1alpha1.ListenerVolumeName: {
				Scope: listenerv1alpha1.PodlistenerNodeScope,
				ListenerIngresses: []listenerv1alpha1.IngressAddressSpec{
					{
						Address: "z-node.example.test", AddressType: listenerv1alpha1.AddressTypeHostname,
						Ports: map[string]int32{hdfsv1alpha1.RpcName: 32020, hdfsv1alpha1.HttpName: 32070},
					},
					{
						Address: "a-node.example.test", AddressType: listenerv1alpha1.AddressTypeHostname,
						Ports: map[string]int32{hdfsv1alpha1.RpcName: 31020, hdfsv1alpha1.HttpName: 31070},
					},
				},
			},
		}},
	}
}

func TestResolveDiscoveryEndpointsUsesPodListeners(t *testing.T) {
	cr := externalDiscoveryCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(discoveryTestScheme(t)).
		WithObjects(discoveryTestPod(), discoveryTestPodListeners()).Build()

	endpoints, ready, err := resolveDiscoveryEndpoints(context.Background(), k8sClient, cr)
	if err != nil {
		t.Fatalf("resolve discovery endpoints: %v", err)
	}
	if !ready {
		t.Fatal("external endpoint should be ready")
	}
	got := endpoints[discoveryTestPodName]
	if got.Address != "a-node.example.test" || got.Ports[hdfsv1alpha1.RpcName] != 31020 {
		t.Errorf("resolved endpoint = %+v, want deterministic first valid PodListeners ingress", got)
	}
}

func TestResolveDiscoveryEndpointsWaitsForCompletePodListeners(t *testing.T) {
	cr := externalDiscoveryCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(discoveryTestScheme(t)).
		WithObjects(discoveryTestPod()).Build()

	endpoints, ready, err := resolveDiscoveryEndpoints(context.Background(), k8sClient, cr)
	if err != nil {
		t.Fatalf("resolve discovery endpoints: %v", err)
	}
	if ready {
		t.Fatal("external endpoint must not be ready before PodListeners is published")
	}
	if got := endpoints[discoveryTestPodName].Address; !strings.Contains(got, ".svc.cluster.local") {
		t.Errorf("unresolved endpoint should retain internal candidate, got %q", got)
	}
}

func TestDiscoveryPostReconcilePreservesLastCompleteConfigMapWhileWaiting(t *testing.T) {
	cr := externalDiscoveryCluster()
	existing := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: cr.Name, Namespace: cr.Namespace}, Data: map[string]string{"hdfs-site.xml": "last-complete"}}
	k8sClient := fake.NewClientBuilder().WithScheme(discoveryTestScheme(t)).
		WithObjects(cr, discoveryTestPod(), existing).Build()

	if err := NewDiscoveryExtension().PostReconcile(context.Background(), k8sClient, cr); err != nil {
		t.Fatalf("PostReconcile while waiting: %v", err)
	}
	got := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(existing), got); err != nil {
		t.Fatalf("get discovery ConfigMap: %v", err)
	}
	if got.Data["hdfs-site.xml"] != "last-complete" {
		t.Errorf("partial Listener state replaced discovery data: %+v", got.Data)
	}
}

func TestDiscoveryPostReconcilePublishesResolvedEndpoint(t *testing.T) {
	cr := externalDiscoveryCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(discoveryTestScheme(t)).
		WithObjects(cr, discoveryTestPod(), discoveryTestPodListeners()).Build()

	if err := NewDiscoveryExtension().PostReconcile(context.Background(), k8sClient, cr); err != nil {
		t.Fatalf("PostReconcile: %v", err)
	}
	got := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}, got); err != nil {
		t.Fatalf("get discovery ConfigMap: %v", err)
	}
	if data := got.Data["hdfs-site.xml"]; !strings.Contains(data, "a-node.example.test:31020") {
		t.Errorf("discovery hdfs-site.xml does not contain resolved endpoint: %s", data)
	}
}

func TestDiscoveryRequestMapper(t *testing.T) {
	pod := discoveryTestPod()
	k8sClient := fake.NewClientBuilder().WithScheme(discoveryTestScheme(t)).WithObjects(pod).Build()
	mapper := discoveryRequestMapper(k8sClient)

	listenerObj := &listenerv1alpha1.Listener{ObjectMeta: metav1.ObjectMeta{
		Name: discoveryTestPodName + "-" + hdfsv1alpha1.ListenerVolumeName, Namespace: pod.Namespace,
	}}
	for name, obj := range map[string]client.Object{
		listenerKind:   listenerObj,
		"PodListeners": discoveryTestPodListeners(),
	} {
		requests := mapper(context.Background(), obj)
		if len(requests) != 1 || requests[0].Namespace != pod.Namespace || requests[0].Name != discoveryTestCluster {
			t.Errorf("%s mapping = %+v, want default/%s", name, requests, discoveryTestCluster)
		}
	}
}
