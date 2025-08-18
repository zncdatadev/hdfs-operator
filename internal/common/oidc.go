package common

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/url"
	"strconv"
	"strings"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	authv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/authentication/v1alpha1"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	"github.com/zncdatadev/operator-go/pkg/util"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	oidcLogger = ctrl.Log.WithName("oidc")
)

// OidcContainerBuilder builds OIDC proxy containers using new architecture
type OidcContainerBuilder struct {
	instance        *hdfsv1alpha1.HdfsCluster
	roleGroupInfo   *reconciler.RoleGroupInfo
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec
	image           *util.Image
	port            int32
	oidcProvider    *authv1alpha1.OIDCProvider
	oidc            *hdfsv1alpha1.OidcSpec
}

// NewOidcContainerBuilder creates a new OIDC container builder
func NewOidcContainerBuilder(
	instance *hdfsv1alpha1.HdfsCluster,
	roleGroupInfo *reconciler.RoleGroupInfo,
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec,
	image *util.Image,
	port int32,
	oidcProvider *authv1alpha1.OIDCProvider,
	oidc *hdfsv1alpha1.OidcSpec,
) *OidcContainerBuilder {
	return &OidcContainerBuilder{
		instance:        instance,
		roleGroupInfo:   roleGroupInfo,
		roleGroupConfig: roleGroupConfig,
		image:           image,
		port:            port,
		oidcProvider:    oidcProvider,
		oidc:            oidc,
	}
}

// Build builds the OIDC container using new architecture
func (b *OidcContainerBuilder) Build() *corev1.Container {
	// Create the common container builder
	builder := NewHdfsContainerBuilder(
		constant.OidcComponent, // Use the defined OIDC container component
		b.image,
		b.instance.Spec.ClusterConfig.ZookeeperConfigMapName,
		b.roleGroupInfo,
		b.roleGroupConfig,
	)

	// Create OIDC component and build container
	component := newOidcComponent(b.instance, b.port, b.oidcProvider, b.oidc)

	return builder.BuildWithComponent(component)
}

// oidcComponent implements ContainerComponentInterface for OIDC proxy
type oidcComponent struct {
	instance     *hdfsv1alpha1.HdfsCluster
	port         int32
	oidcProvider *authv1alpha1.OIDCProvider
	oidc         *hdfsv1alpha1.OidcSpec
}

// Ensure oidcComponent implements all required interfaces
var _ ContainerComponentInterface = &oidcComponent{}
var _ ContainerPortsProvider = &oidcComponent{}

func newOidcComponent(
	instance *hdfsv1alpha1.HdfsCluster,
	port int32,
	oidcProvider *authv1alpha1.OIDCProvider,
	oidc *hdfsv1alpha1.OidcSpec,
) *oidcComponent {
	return &oidcComponent{
		instance:     instance,
		port:         port,
		oidcProvider: oidcProvider,
		oidc:         oidc,
	}
}

func (c *oidcComponent) GetContainerName() string {
	return "oidc"
}

func (c *oidcComponent) GetCommand() []string {
	return []string{
		"sh",
		"-c",
	}
}

func (c *oidcComponent) GetArgs() []string {
	// OIDC proxy doesn't need complex args, command handles everything
	return []string{
		"/kubedoop/oauth2-proxy/oauth2-proxy --upstream=${UPSTREAM}",
	}
}

func (c *oidcComponent) GetEnvVars() []corev1.EnvVar {
	scopes := []string{"openid", "email", "profile"}

	if c.oidc.ExtraScopes != nil {
		scopes = append(scopes, c.oidc.ExtraScopes...)
	}

	issuer := url.URL{
		Scheme: "http",
		Host:   c.oidcProvider.Hostname,
		Path:   c.oidcProvider.RootPath,
	}

	if c.oidcProvider.Port != 0 && c.oidcProvider.Port != 80 {
		issuer.Host += ":" + strconv.Itoa(c.oidcProvider.Port)
	}

	providerHint := c.oidcProvider.ProviderHint
	// TODO: fix support keycloak-oidc
	if providerHint == "keycloak" {
		providerHint = "keycloak-oidc"
	}

	clientCredentialsSecretName := c.oidc.ClientCredentialsSecret

	hash := sha256.Sum256([]byte(string(c.instance.UID)))
	hashStr := hex.EncodeToString(hash[:])
	tokenBytes := []byte(hashStr[:16])
	cookieSecret := base64.StdEncoding.EncodeToString([]byte(base64.StdEncoding.EncodeToString(tokenBytes)))

	return []corev1.EnvVar{
		{
			Name:  "OAUTH2_PROXY_COOKIE_SECRET",
			Value: cookieSecret,
		},
		{
			Name: "OAUTH2_PROXY_CLIENT_ID",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: clientCredentialsSecretName,
					},
					Key: "CLIENT_ID",
				},
			},
		},
		{
			Name: "OAUTH2_PROXY_CLIENT_SECRET",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: clientCredentialsSecretName,
					},
					Key: "CLIENT_SECRET",
				},
			},
		},
		{
			Name: "POD_IP",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "status.podIP",
				},
			},
		},
		{
			Name:  "OAUTH2_PROXY_OIDC_ISSUER_URL",
			Value: issuer.String(),
		},
		{
			Name:  "OAUTH2_PROXY_SCOPE",
			Value: strings.Join(scopes, " "),
		},
		{
			Name:  "OAUTH2_PROXY_PROVIDER",
			Value: providerHint,
		},
		{
			Name:  "UPSTREAM",
			Value: "http://$(POD_IP):" + strconv.Itoa(int(c.port)),
		},
		{
			Name:  "OAUTH2_PROXY_HTTP_ADDRESS",
			Value: "0.0.0.0:4180",
		},
		{
			Name:  "OAUTH2_PROXY_CODE_CHALLENGE_METHOD",
			Value: "S256",
		},
		{
			Name:  "OAUTH2_PROXY_EMAIL_DOMAINS",
			Value: "*",
		},
		{
			Name:  "OAUTH2_PROXY_COOKIE_SECURE",
			Value: "false",
		},
		{
			Name:  "OAUTH2_PROXY_WHITELIST_DOMAINS",
			Value: "*",
		},
	}
}

func (c *oidcComponent) GetVolumeMounts() []corev1.VolumeMount {
	// OIDC proxy typically doesn't need additional volume mounts beyond common ones
	return []corev1.VolumeMount{}
}

// ContainerPortsProvider interface implementation
func (c *oidcComponent) GetPorts() []corev1.ContainerPort {
	return []corev1.ContainerPort{
		{
			Name:          "oidc",
			ContainerPort: 4180,
			Protocol:      corev1.ProtocolTCP,
		},
	}
}

// MakeOidcContainer creates an OIDC container using the new architecture
func MakeOidcContainer(
	ctx context.Context,
	client ctrlclient.Client,
	instance *hdfsv1alpha1.HdfsCluster,
	roleGroupInfo *reconciler.RoleGroupInfo,
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec,
	port int32,
	image *util.Image,
) (*corev1.Container, error) {
	authClass := &authv1alpha1.AuthenticationClass{}
	if err := client.Get(ctx, ctrlclient.ObjectKey{Namespace: instance.Namespace, Name: instance.Spec.ClusterConfig.Authentication.AuthenticationClass}, authClass); err != nil {
		if ctrlclient.IgnoreNotFound(err) != nil {
			return nil, err
		}
		return nil, nil
	}

	if authClass.Spec.AuthenticationProvider.OIDC == nil || instance.Spec.ClusterConfig.Authentication.Oidc == nil {
		oidcLogger.Info("OIDC provider is not configured", "OidcProvider", authClass.Spec.AuthenticationProvider.OIDC, "OidcCredential", instance.Spec.ClusterConfig.Authentication.Oidc)
		return nil, nil
	}

	builder := NewOidcContainerBuilder(
		instance,
		roleGroupInfo,
		roleGroupConfig,
		image,
		port,
		authClass.Spec.AuthenticationProvider.OIDC,
		instance.Spec.ClusterConfig.Authentication.Oidc,
	)

	container := builder.Build()
	return container, nil
}
