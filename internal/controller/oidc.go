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
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	authv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/authentication/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
)

const (
	oidcContainerName = "oidc"
	// oidcProxyPort is the port oauth2-proxy listens on; it fronts the NameNode web UI.
	oidcProxyPort int32 = 4180
)

// oidcEnabled reports whether the CR requests OIDC (an AuthenticationClass reference plus the
// client credentials secret).
func oidcEnabled(cr *hdfsv1alpha1.HdfsCluster) bool {
	return cr.Spec.ClusterConfig != nil &&
		cr.Spec.ClusterConfig.Authentication != nil &&
		cr.Spec.ClusterConfig.Authentication.Oidc != nil &&
		cr.Spec.ClusterConfig.Authentication.AuthenticationClass != ""
}

// oidcSidecar fetches the referenced AuthenticationClass and, when it carries an OIDC provider,
// builds the oauth2-proxy sidecar that fronts the NameNode web UI. Returns (nil, nil) when OIDC is
// not configured or the AuthenticationClass/provider is absent.
func oidcSidecar(ctx context.Context, c ctrlclient.Client, cr *hdfsv1alpha1.HdfsCluster) (*corev1.Container, error) {
	if !oidcEnabled(cr) {
		return nil, nil
	}
	auth := cr.Spec.ClusterConfig.Authentication

	authClass := &authv1alpha1.AuthenticationClass{}
	key := ctrlclient.ObjectKey{Namespace: cr.Namespace, Name: auth.AuthenticationClass}
	if err := c.Get(ctx, key, authClass); err != nil {
		if ctrlclient.IgnoreNotFound(err) != nil {
			return nil, fmt.Errorf("get AuthenticationClass %q: %w", auth.AuthenticationClass, err)
		}
		return nil, nil // not found yet; a later reconcile picks it up
	}
	if authClass.Spec.AuthenticationProvider == nil || authClass.Spec.AuthenticationProvider.OIDC == nil {
		return nil, nil
	}

	container := oidcContainer(cr, authClass.Spec.AuthenticationProvider.OIDC, auth.Oidc, hdfsv1alpha1.NameNodeHttpPort)
	return &container, nil
}

// oidcContainer builds the oauth2-proxy container that proxies OIDC-authenticated traffic to the
// local NameNode web UI (upstream). Modeled on the pre-refactor implementation.
func oidcContainer(cr *hdfsv1alpha1.HdfsCluster, provider *authv1alpha1.OIDCProvider, oidc *hdfsv1alpha1.OidcSpec, upstreamPort int32) corev1.Container {
	return corev1.Container{
		Name:    oidcContainerName,
		Image:   resolveImage(cr),
		Command: []string{"sh", "-c"},
		Args:    []string{"/kubedoop/oauth2-proxy/oauth2-proxy --upstream=${UPSTREAM}"},
		Env:     oidcEnv(cr, provider, oidc, upstreamPort),
		Ports: []corev1.ContainerPort{
			{Name: oidcContainerName, ContainerPort: oidcProxyPort, Protocol: corev1.ProtocolTCP},
		},
		// Native sidecar: oauth2-proxy runs for the pod's lifetime.
		RestartPolicy: ptr.To(corev1.ContainerRestartPolicyAlways),
	}
}

// oidcEnv builds the OAUTH2_PROXY_* environment for the sidecar.
func oidcEnv(cr *hdfsv1alpha1.HdfsCluster, provider *authv1alpha1.OIDCProvider, oidc *hdfsv1alpha1.OidcSpec, upstreamPort int32) []corev1.EnvVar {
	scopes := make([]string, 0, 3+len(oidc.ExtraScopes))
	scopes = append(scopes, "openid", "email", "profile")
	scopes = append(scopes, oidc.ExtraScopes...)

	issuer := url.URL{Scheme: "http", Host: provider.Hostname, Path: provider.RootPath}
	if provider.Port != 0 && provider.Port != 80 {
		issuer.Host += ":" + strconv.Itoa(provider.Port)
	}

	providerHint := provider.ProviderHint
	if providerHint == "keycloak" {
		providerHint = "keycloak-oidc"
	}

	secretRef := func(key string) *corev1.EnvVarSource {
		return &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: oidc.ClientCredentialsSecret},
			Key:                  key,
		}}
	}

	return []corev1.EnvVar{
		{Name: "OAUTH2_PROXY_COOKIE_SECRET", Value: oidcCookieSecret(cr)},
		{Name: "OAUTH2_PROXY_CLIENT_ID", ValueFrom: secretRef("CLIENT_ID")},
		{Name: "OAUTH2_PROXY_CLIENT_SECRET", ValueFrom: secretRef("CLIENT_SECRET")},
		{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}},
		{Name: "OAUTH2_PROXY_OIDC_ISSUER_URL", Value: issuer.String()},
		{Name: "OAUTH2_PROXY_SCOPE", Value: strings.Join(scopes, " ")},
		{Name: "OAUTH2_PROXY_PROVIDER", Value: providerHint},
		{Name: "UPSTREAM", Value: fmt.Sprintf("http://$(POD_IP):%d", upstreamPort)},
		{Name: "OAUTH2_PROXY_HTTP_ADDRESS", Value: "0.0.0.0:" + strconv.Itoa(int(oidcProxyPort))},
		{Name: "OAUTH2_PROXY_CODE_CHALLENGE_METHOD", Value: "S256"},
		{Name: "OAUTH2_PROXY_EMAIL_DOMAINS", Value: "*"},
		{Name: "OAUTH2_PROXY_COOKIE_SECURE", Value: "false"},
		{Name: "OAUTH2_PROXY_WHITELIST_DOMAINS", Value: "*"},
	}
}

// oidcCookieSecret derives a stable oauth2-proxy cookie secret from the cluster UID, so it is
// deterministic across reconciles without persisting a generated secret.
func oidcCookieSecret(cr *hdfsv1alpha1.HdfsCluster) string {
	hash := sha256.Sum256([]byte(string(cr.UID)))
	token := hex.EncodeToString(hash[:])[:16]
	return base64.StdEncoding.EncodeToString([]byte(base64.StdEncoding.EncodeToString([]byte(token))))
}
