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
	"testing"

	authv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/authentication/v1alpha1"
	corev1 "k8s.io/api/core/v1"

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
	cr.Spec.ClusterConfig.Authentication.AuthenticationClass = "oidc"
	if !oidcEnabled(cr) {
		t.Error("OIDC should be enabled with authenticationClass + oidc creds")
	}
}

func TestOidcContainer(t *testing.T) {
	cr := crWithNameNodes()
	provider := &authv1alpha1.OIDCProvider{
		Hostname:     "keycloak.default.svc",
		Port:         8080,
		RootPath:     "/realms/kubedoop",
		ProviderHint: "keycloak",
	}
	oidc := &hdfsv1alpha1.OidcSpec{ClientCredentialsSecret: "oidc-credentials", ExtraScopes: []string{"groups"}}

	c := oidcContainer(cr, provider, oidc, hdfsv1alpha1.NameNodeHttpPort)

	if c.Name != oidcContainerName || len(c.Ports) != 1 || c.Ports[0].ContainerPort != oidcProxyPort {
		t.Errorf("oidc container = name %q ports %+v, want %q on %d", c.Name, c.Ports, oidcContainerName, oidcProxyPort)
	}
	if c.RestartPolicy == nil || *c.RestartPolicy != corev1.ContainerRestartPolicyAlways {
		t.Error("oidc proxy should be a native sidecar (RestartPolicy=Always)")
	}

	env := map[string]corev1.EnvVar{}
	for _, e := range c.Env {
		env[e.Name] = e
	}
	if got := env["OAUTH2_PROXY_OIDC_ISSUER_URL"].Value; got != "http://keycloak.default.svc:8080/realms/kubedoop" {
		t.Errorf("issuer url = %q", got)
	}
	if got := env["OAUTH2_PROXY_PROVIDER"].Value; got != "keycloak-oidc" {
		t.Errorf("provider = %q, want keycloak-oidc (hint remap)", got)
	}
	if got := env["UPSTREAM"].Value; got != "http://$(POD_IP):9870" {
		t.Errorf("upstream = %q, want http://$(POD_IP):9870", got)
	}
	if ref := env["OAUTH2_PROXY_CLIENT_ID"].ValueFrom; ref == nil || ref.SecretKeyRef == nil ||
		ref.SecretKeyRef.Name != "oidc-credentials" || ref.SecretKeyRef.Key != "CLIENT_ID" {
		t.Errorf("CLIENT_ID should come from the credentials secret, got %+v", ref)
	}
	if got := env["OAUTH2_PROXY_SCOPE"].Value; got != "openid email profile groups" {
		t.Errorf("scope = %q, want openid email profile groups", got)
	}
}
