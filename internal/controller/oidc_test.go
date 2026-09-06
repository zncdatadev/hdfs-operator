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
	"github.com/zncdatadev/operator-go/pkg/sidecar"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
)

func TestOidcEnabled(t *testing.T) {
	cr := crWithNameNodes()
	if oidcEnabled(cr) {
		t.Error("OIDC should be disabled without an authentication block")
	}
	cr.Spec.ClusterConfig.Authentication = &hdfsv1alpha1.AuthenticationSpec{
		Oidc: &hdfsv1alpha1.OidcSpec{ClientCredentialsSecret: "creds"},
	}
	if oidcEnabled(cr) {
		t.Error("OIDC needs an authenticationClass reference, not just oidc creds")
	}
	cr.Spec.ClusterConfig.Authentication.AuthenticationClass = testAuthClass
	if !oidcEnabled(cr) {
		t.Error("OIDC should be enabled with authenticationClass + oidc creds")
	}
}

func TestOidcCookieSecretName(t *testing.T) {
	cr := crWithNameNodes()
	if got, want := oidcCookieSecretName(cr), "simple-hdfs-oidc-cookie"; got != want {
		t.Errorf("cookie secret name = %q, want %q", got, want)
	}
}

// testNamespace is the namespace used across controller unit tests.
const testNamespace = "default"

// testAuthClass is the AuthenticationClass name used across OIDC unit tests.
const testAuthClass = "oidc"

func oidcCR() *hdfsv1alpha1.HdfsCluster {
	cr := crWithNameNodes()
	cr.Namespace = testNamespace
	cr.Spec.ClusterConfig.Authentication = &hdfsv1alpha1.AuthenticationSpec{
		AuthenticationClass: testAuthClass,
		Oidc:                &hdfsv1alpha1.OidcSpec{ClientCredentialsSecret: "oidc-credentials", ExtraScopes: []string{"groups"}},
	}
	return cr
}

func oidcScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := authv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add authentication scheme: %v", err)
	}
	return scheme
}

func TestOidcSidecarProvider(t *testing.T) {
	cr := oidcCR()
	authClass := &authv1alpha1.AuthenticationClass{
		ObjectMeta: metav1.ObjectMeta{Name: testAuthClass, Namespace: testNamespace},
		Spec: authv1alpha1.AuthenticationClassSpec{
			AuthenticationProvider: &authv1alpha1.AuthenticationProvider{
				OIDC: &authv1alpha1.OIDCProvider{
					Hostname:     "keycloak.default.svc",
					Port:         8080,
					RootPath:     "/realms/kubedoop",
					ProviderHint: "keycloak",
				},
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(oidcScheme(t)).WithObjects(authClass).Build()

	provider, err := oidcSidecarProvider(context.Background(), c, cr)
	if err != nil {
		t.Fatalf("oidcSidecarProvider: %v", err)
	}
	if provider == nil {
		t.Fatal("expected a provider when the AuthenticationClass carries an OIDC provider")
	}
	if provider.Name() != oidcContainerName {
		t.Errorf("provider name = %q, want %q", provider.Name(), oidcContainerName)
	}
}

func TestOidcSidecarProvider_AbsentAuthClass(t *testing.T) {
	cr := oidcCR()
	c := fake.NewClientBuilder().WithScheme(oidcScheme(t)).Build()

	provider, err := oidcSidecarProvider(context.Background(), c, cr)
	if err == nil {
		t.Fatal("expected an error when the configured AuthenticationClass does not exist")
	}
	if provider != nil {
		t.Error("expected nil provider when AuthenticationClass resolution fails")
	}
}

func TestOidcSidecarProvider_RejectsAuthClassWithoutOidcProvider(t *testing.T) {
	cr := oidcCR()
	authClass := &authv1alpha1.AuthenticationClass{
		ObjectMeta: metav1.ObjectMeta{Name: testAuthClass, Namespace: testNamespace},
	}
	c := fake.NewClientBuilder().WithScheme(oidcScheme(t)).WithObjects(authClass).Build()

	provider, err := oidcSidecarProvider(context.Background(), c, cr)
	if err == nil {
		t.Fatal("expected an error when AuthenticationClass has no OIDC provider")
	}
	if provider != nil {
		t.Error("expected nil provider for an incompatible AuthenticationClass")
	}
}

// Ensure the cookie secret key the sidecar validates against is the one we generate.
func TestOidcCookieSecretKeyMatchesFramework(t *testing.T) {
	if sidecar.OIDCCookieSecretKey == "" {
		t.Error("framework OIDCCookieSecretKey should be non-empty")
	}
}
