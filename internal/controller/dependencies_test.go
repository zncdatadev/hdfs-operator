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
	"context"
	"testing"

	authv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/authentication/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
)

const (
	dependencyTestCluster = "dependency-hdfs"
	dependencyTestZK      = "zookeeper-discovery"
	dependencyTestSecret  = "oidc-credentials"
)

func dependencyCluster() *hdfsv1alpha1.HdfsCluster {
	return &hdfsv1alpha1.HdfsCluster{
		ObjectMeta: metav1.ObjectMeta{Name: dependencyTestCluster, Namespace: testNamespace},
		Spec: hdfsv1alpha1.HdfsClusterSpec{ClusterConfig: &hdfsv1alpha1.ClusterConfigSpec{
			ZookeeperConfigMapName: dependencyTestZK,
			Authentication: &hdfsv1alpha1.AuthenticationSpec{
				AuthenticationClass: testAuthClass,
				Oidc: &hdfsv1alpha1.OidcSpec{
					ClientCredentialsSecret: dependencyTestSecret,
				},
			},
		}},
	}
}

func dependencyScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"core":           corev1.AddToScheme,
		"hdfs":           hdfsv1alpha1.AddToScheme,
		"authentication": authv1alpha1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("add %s scheme: %v", name, err)
		}
	}
	return scheme
}

func TestExternalDependencies(t *testing.T) {
	got := ExternalDependencies(dependencyCluster())
	want := []reconciler.Dependency{
		{Kind: reconciler.DependencyConfigMap, Name: dependencyTestZK},
		{Kind: reconciler.DependencySecret, Name: dependencyTestSecret},
	}
	if len(got) != len(want) {
		t.Fatalf("dependencies = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("dependency[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestDependencyRequestMapper(t *testing.T) {
	cluster := dependencyCluster()
	unrelated := dependencyCluster()
	unrelated.Name = "unrelated-hdfs"
	unrelated.Spec.ClusterConfig.ZookeeperConfigMapName = "other-zookeeper"
	unrelated.Spec.ClusterConfig.Authentication.AuthenticationClass = "other-authentication"
	unrelated.Spec.ClusterConfig.Authentication.Oidc.ClientCredentialsSecret = "other-secret"
	k8sClient := fake.NewClientBuilder().WithScheme(dependencyScheme(t)).WithObjects(cluster, unrelated).Build()
	mapper := dependencyRequestMapper(k8sClient)

	objects := map[string]client.Object{
		"ConfigMap": &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: dependencyTestZK, Namespace: testNamespace}},
		"Secret":    &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: dependencyTestSecret, Namespace: testNamespace}},
		"AuthenticationClass": &authv1alpha1.AuthenticationClass{ObjectMeta: metav1.ObjectMeta{
			Name: testAuthClass, Namespace: testNamespace,
		}},
	}
	for name, obj := range objects {
		requests := mapper(context.Background(), obj)
		if len(requests) != 1 || requests[0].Namespace != testNamespace || requests[0].Name != dependencyTestCluster {
			t.Errorf("%s dependency mapping = %+v, want %s/%s", name, requests, testNamespace, dependencyTestCluster)
		}
	}

	requests := mapper(context.Background(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: "not-referenced", Namespace: testNamespace,
	}})
	if len(requests) != 0 {
		t.Errorf("unreferenced object mapped to %+v", requests)
	}
}
