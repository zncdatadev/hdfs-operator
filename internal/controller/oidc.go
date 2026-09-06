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
	"fmt"

	authv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/authentication/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	"github.com/zncdatadev/operator-go/pkg/sidecar"
	corev1 "k8s.io/api/core/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constants"
)

// oidcContainerName is the oauth2-proxy sidecar container name. Kept as "oidc" (the framework
// default is "oauth2-proxy") so the rendered NameNode pod is unchanged for existing clusters.
const oidcContainerName = "oidc"

// oidcEnabled reports whether the CR requests OIDC (an AuthenticationClass reference plus the
// client credentials secret).
func oidcEnabled(cr *hdfsv1alpha1.HdfsCluster) bool {
	return cr.Spec.ClusterConfig != nil &&
		cr.Spec.ClusterConfig.Authentication != nil &&
		cr.Spec.ClusterConfig.Authentication.Oidc != nil &&
		cr.Spec.ClusterConfig.Authentication.AuthenticationClass != ""
}

// oidcCookieSecretName is the cluster-owned Secret holding the oauth2-proxy session cookie secret.
func oidcCookieSecretName(cr *hdfsv1alpha1.HdfsCluster) string {
	return cr.Name + "-oidc-cookie"
}

// ensureOidcCookieSecret creates (once) the generated session-cookie Secret the oauth2-proxy
// sidecar signs sessions with. reconciler.EnsureGeneratedSecret generates the value only when the
// Secret is absent and never re-converges it, so restarts and re-reconciles keep every existing
// session valid. Replaces the old UID-derived cookie, which baked a deterministic secret into the
// pod spec.
func ensureOidcCookieSecret(ctx context.Context, c ctrlclient.Client, cr *hdfsv1alpha1.HdfsCluster) error {
	_, err := reconciler.EnsureGeneratedSecret(ctx, c, c.Scheme(), cr, oidcCookieSecretName(cr),
		map[string]func() (string, error){sidecar.OIDCCookieSecretKey: sidecar.GenerateCookieSecret},
		reconciler.WithGeneratedSecretProductName(constants.ProductName),
	)
	return err
}

// oidcSidecarProvider builds the framework oauth2-proxy sidecar that fronts the NameNode web UI.
// It returns nil only when OIDC is not configured. Once the CR explicitly requests OIDC, a
// missing AuthenticationClass or OIDC provider is an error so the NameNode cannot silently start
// without the authentication sidecar the user requested.
func oidcSidecarProvider(ctx context.Context, c ctrlclient.Client, cr *hdfsv1alpha1.HdfsCluster) (*sidecar.OAuth2ProxySidecarProvider, error) {
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
		return nil, fmt.Errorf("AuthenticationClass %q not found", auth.AuthenticationClass)
	}
	if authClass.Spec.AuthenticationProvider == nil || authClass.Spec.AuthenticationProvider.OIDC == nil {
		return nil, fmt.Errorf("AuthenticationClass %q does not define an OIDC provider", auth.AuthenticationClass)
	}

	return sidecar.NewOAuth2ProxySidecarProvider(
		authClass.Spec.AuthenticationProvider.OIDC,
		auth.Oidc.ClientCredentialsSecret,
		hdfsv1alpha1.NameNodeHttpPort,
		sidecar.WithOAuth2ProxyContainerName(oidcContainerName),
		sidecar.WithOAuth2ProxyExtraScopes(auth.Oidc.ExtraScopes...),
		// The IdP realm is dedicated to this cluster, so every authenticated account is allowed.
		// Spelled out (not a default) per the framework's authorization contract; preserves the
		// pre-refactor behaviour (OAUTH2_PROXY_EMAIL_DOMAINS="*").
		sidecar.WithOAuth2ProxyAllowAllEmails(),
		sidecar.WithOAuth2ProxyCookieSecretRef(&corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: oidcCookieSecretName(cr)},
			Key:                  sidecar.OIDCCookieSecretKey,
		}),
	), nil
}
