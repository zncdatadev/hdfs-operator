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
	"path"

	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/config"
	"github.com/zncdatadev/operator-go/pkg/constant"
	"github.com/zncdatadev/operator-go/pkg/listener"
	"github.com/zncdatadev/operator-go/pkg/productlogging"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	"github.com/zncdatadev/operator-go/pkg/security"
	"github.com/zncdatadev/operator-go/pkg/sidecar"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constants"
)

// HdfsRoleGroupHandler builds HDFS role group resources. It embeds the SDK BaseRoleGroupHandler
// (which owns resource orchestration: ConfigMap, Services, StatefulSet, PDB) and also implements
// reconciler.RoleProvider — DeclareRoles states, per reconcile with the cr in hand, everything a
// role is made of (ports, primary container name, start command, data volume, log producers). The
// product-specific pieces the declaration cannot express (init containers, CSI volumes, metrics
// Service) are added in the BuildResources override.
type HdfsRoleGroupHandler struct {
	*reconciler.BaseRoleGroupHandler[*hdfsv1alpha1.HdfsCluster]
}

// NewHdfsRoleGroupHandler creates the handler. It carries only reconcile-invariant collaborators;
// everything a role is made of is declared per reconcile by DeclareRoles.
func NewHdfsRoleGroupHandler(scheme *runtime.Scheme) *HdfsRoleGroupHandler {
	base := reconciler.NewBaseRoleGroupHandler[*hdfsv1alpha1.HdfsCluster](scheme)

	// core-site.xml / hdfs-site.xml are rendered as Hadoop XML by the default formats.
	base.ConfigGenerator = config.NewMultiFormatConfigGenerator()
	base.ConfigGenerator.RegisterDefaultFormats()

	// HDFS reads its config from the Hadoop config dir.
	base.ConfigMountPath = hdfsv1alpha1.HadoopHome + "/etc/hadoop"

	return &HdfsRoleGroupHandler{BaseRoleGroupHandler: base}
}

// roleContainerNames maps each HDFS role to its primary (daemon) container name. These become both
// the renamed StatefulSet container and the per-container logging key (logging.containers.<name>).
var roleContainerNames = map[string]string{
	hdfsv1alpha1.NameNodeRoleName:    constants.NameNodeContainerName,
	hdfsv1alpha1.DataNodeRoleName:    constants.DataNodeContainerName,
	hdfsv1alpha1.JournalNodeRoleName: constants.JournalNodeContainerName,
}

// DeclareRoles implements reconciler.RoleProvider: one statement per role, produced once per
// reconcile pass with the cr in hand (so a port that moves because the CR enabled TLS is computed
// from THIS cr, not from process-wide handler state).
func (h *HdfsRoleGroupHandler) DeclareRoles(
	_ context.Context, _ client.Client, cr *hdfsv1alpha1.HdfsCluster,
) (reconciler.RoleCatalog, error) {
	catalog := reconciler.RoleCatalog{}
	for _, role := range []string{
		hdfsv1alpha1.NameNodeRoleName,
		hdfsv1alpha1.DataNodeRoleName,
		hdfsv1alpha1.JournalNodeRoleName,
	} {
		catalog[role] = h.roleDeclaration(cr, role)
	}
	return catalog, nil
}

// roleDeclaration is the per-role statement DeclareRoles returns.
func (h *HdfsRoleGroupHandler) roleDeclaration(cr *hdfsv1alpha1.HdfsCluster, roleName string) reconciler.RoleDeclaration {
	cname := roleContainerNames[roleName]
	containerPorts, servicePorts := rolePorts(roleName)
	// Under TLS the role also serves HTTPS; expose the port so the listener projects HTTPS_PORT.
	if tlsOn(cr) {
		if p := httpsContainerPort(roleName); p != nil {
			containerPorts = append(containerPorts, *p)
		}
	}
	return reconciler.RoleDeclaration{
		MainContainerName: cname,
		// ContainerPorts[0] (the RPC / data-transfer port) backs the framework's generated TCP
		// readiness probe — HDFS's readiness signal. No liveness probe: operator-go #562 removed
		// the guessed liveness that killed NameNodes mid-fsimage-load, and TCP is auth-agnostic
		// where an HTTP web-UI probe would 401 forever under Kerberos SPNEGO.
		ContainerPorts: containerPorts,
		ServicePorts:   servicePorts,
		// The whole entrypoint is a bash script (export the listener address, then exec the daemon);
		// it goes in Command, not Args, since Args are the user's cliOverrides channel.
		Command: []string{bashShell, "-c", mainContainerScript(cr, roleName)},
		// Static env with valueFrom (POD_NAME downward API, the ZOOKEEPER ConfigMap ref, Kerberos
		// paths). Computed env (HDFS_<ROLE>_OPTS, sized from the resolved memory limit) flows through
		// ComputeConfig's Contribution.EnvVars instead, which sees the effective config.
		Env: commonEnv(cr, h.ConfigMountPath),
		// Every HDFS role persists to KubedoopDataDir. The framework builds the VolumeClaimTemplate
		// only when DataVolume != nil AND the effective config.resources.storage is non-nil, so
		// ConfigDefaults below supplies an empty (non-nil) storage — GetCapacity then fills the
		// commons DefaultStorageCapacity. Without it a CR that omits storage yields NO data volume,
		// and the init containers' "data" mount fails the StatefulSet ("volumeMounts...data: Not
		// found"). A user's config.resources.storage folds on top, so an explicit size still wins.
		DataVolume: &reconciler.DataVolume{MountPath: constant.KubedoopDataDir},
		// HDFS processes discover peers by the stable pod DNS names before the daemons can become
		// ready. Publishing only ready EndpointSlices creates a bootstrap cycle for NameNode HA and
		// the JournalNode quorum. Preserve the pre-v0.13 operator's behavior for all three roles.
		PublishNotReadyAddresses: true,
		ConfigDefaults:           dataStorageDefaults(),
		LogProducers: []productlogging.ContainerLogging{
			{Container: cname, Framework: productlogging.LoggingFrameworkLog4j},
		},
	}
}

// dataStorageDefaults gives every role group a non-nil (empty) storage default so the framework
// builds the data PVC. It is the lowest-precedence config layer (ConfigDefaults), folded beneath
// the CR's role and role group levels, so a user-set config.resources.storage always wins. A fresh
// value is returned per call so the fold never mutates a shared instance.
func dataStorageDefaults() *commonsv1alpha1.RoleGroupConfigSpec {
	return &commonsv1alpha1.RoleGroupConfigSpec{
		Resources: &commonsv1alpha1.ResourcesSpec{Storage: &commonsv1alpha1.StorageResource{}},
	}
}

// rolePorts returns the container and service ports for a role. The first container port is the
// role's "ready" port (RPC for NameNode/JournalNode, data transfer for DataNode); the framework's
// generated readiness probe targets ContainerPorts[0].
func rolePorts(roleName string) ([]corev1.ContainerPort, []corev1.ServicePort) {
	type namedPort struct {
		name string
		port int32
	}
	byRole := map[string][]namedPort{
		hdfsv1alpha1.NameNodeRoleName: {
			{hdfsv1alpha1.RpcName, hdfsv1alpha1.NameNodeRpcPort},
			{hdfsv1alpha1.HttpName, hdfsv1alpha1.NameNodeHttpPort},
			{hdfsv1alpha1.MetricName, hdfsv1alpha1.NameNodeMetricPort},
		},
		hdfsv1alpha1.DataNodeRoleName: {
			{hdfsv1alpha1.DataName, hdfsv1alpha1.DataNodeDataPort},
			{hdfsv1alpha1.HttpName, hdfsv1alpha1.DataNodeHttpPort},
			{hdfsv1alpha1.IpcName, hdfsv1alpha1.DataNodeIpcPort},
			{hdfsv1alpha1.MetricName, hdfsv1alpha1.DataNodeMetricPort},
		},
		hdfsv1alpha1.JournalNodeRoleName: {
			{hdfsv1alpha1.RpcName, hdfsv1alpha1.JournalNodeRpcPort},
			{hdfsv1alpha1.HttpName, hdfsv1alpha1.JournalNodeHttpPort},
			{hdfsv1alpha1.MetricName, hdfsv1alpha1.JournalNodeMetricPort},
		},
	}
	ports := byRole[roleName]
	containerPorts := make([]corev1.ContainerPort, 0, len(ports))
	servicePorts := make([]corev1.ServicePort, 0, len(ports))
	for _, p := range ports {
		containerPorts = append(containerPorts, corev1.ContainerPort{
			Name: p.name, ContainerPort: p.port, Protocol: corev1.ProtocolTCP,
		})
		servicePorts = append(servicePorts, corev1.ServicePort{
			Name: p.name, Port: p.port, Protocol: corev1.ProtocolTCP,
		})
	}
	return containerPorts, servicePorts
}

// listenerVolumeName is the name of the listener CSI volume mounted on every HDFS pod.
const listenerVolumeName = "listener"

// bashShell is the shell every HDFS container's entrypoint runs under.
const bashShell = "/bin/bash"

// newListenerProvisioner declares the per-pod listener volume. cluster-internal is the default
// class; per-role-group listenerClass overrides are reintroduced in a later phase.
func newListenerProvisioner() *listener.ListenerProvisioner {
	return listener.NewProvisioner().RegisterVolume(
		listener.NewVolume(listenerVolumeName, listener.ListenerClassClusterInternal),
	)
}

// tlsSecretProvisioner returns a SecretProvisioner that mounts the TLS PKCS12 keystore/truststore
// (named constants.TlsSecretVolumeName), or nil when the CR does not enable TLS. Defaults mirror
// the CRD: secretClass "tls", password "changeit".
func tlsSecretProvisioner(cr *hdfsv1alpha1.HdfsCluster) *security.SecretProvisioner {
	if cr.Spec.ClusterConfig == nil ||
		cr.Spec.ClusterConfig.Authentication == nil ||
		cr.Spec.ClusterConfig.Authentication.Tls == nil {
		return nil
	}
	tls := cr.Spec.ClusterConfig.Authentication.Tls
	secretClass := tls.SecretClass
	if secretClass == "" {
		secretClass = constants.DefaultTlsSecretClass
	}
	password := tls.JksPassword
	if password == "" {
		password = "changeit"
	}
	return security.NewSecretProvisioner().Register(
		security.TLS(constants.TlsSecretVolumeName, secretClass).WithPassword(password),
	)
}

// kerberosServiceNames maps an HDFS role to its Kerberos service (principal) short name.
var kerberosServiceNames = map[string]string{
	hdfsv1alpha1.NameNodeRoleName:    "nn",
	hdfsv1alpha1.DataNodeRoleName:    "dn",
	hdfsv1alpha1.JournalNodeRoleName: "jn",
}

// kerberosSecretProvisioner returns a SecretProvisioner that mounts the role's Kerberos keytab +
// krb5.conf (named constants.KerberosSecretVolumeName), or nil when Kerberos is disabled. The
// secret is service-scoped so the KDC issues a principal for the cluster's service DNS.
func kerberosSecretProvisioner(cr *hdfsv1alpha1.HdfsCluster, roleName string) *security.SecretProvisioner {
	if cr.Spec.ClusterConfig == nil ||
		cr.Spec.ClusterConfig.Authentication == nil ||
		cr.Spec.ClusterConfig.Authentication.Kerberos == nil {
		return nil
	}
	svc, ok := kerberosServiceNames[roleName]
	if !ok {
		return nil
	}
	secretClass := cr.Spec.ClusterConfig.Authentication.Kerberos.SecretClass
	return security.NewSecretProvisioner().Register(
		security.KerberosVolume(constants.KerberosSecretVolumeName, secretClass, svc, "HTTP").
			WithScope("service=" + cr.Name),
	)
}

// kerberosEnabled reports whether the CR requests Kerberos (controller-side check).
func kerberosEnabled(cr *hdfsv1alpha1.HdfsCluster) bool {
	return cr.Spec.ClusterConfig != nil &&
		cr.Spec.ClusterConfig.Authentication != nil &&
		cr.Spec.ClusterConfig.Authentication.Kerberos != nil
}

// BuildResources delegates the bulk to the framework, then adds the product-specific pieces the
// declarative model cannot express: the CSI volume provisioners (listener/TLS/Kerberos), the init
// containers / native sidecars, and the per-role metrics Service.
func (h *HdfsRoleGroupHandler) BuildResources(
	ctx context.Context,
	k8sClient client.Client,
	cr *hdfsv1alpha1.HdfsCluster,
	buildCtx *reconciler.RoleGroupBuildContext,
) (*reconciler.RoleGroupResources, error) {
	// Per-pod listener CSI volume: the pod reads its externally reachable address from this mount
	// (DataNode registration + address advertisement). VolumeProviders is per-role/per-reconcile.
	buildCtx.VolumeProviders = append(buildCtx.VolumeProviders, newListenerProvisioner())
	if p := tlsSecretProvisioner(cr); p != nil {
		buildCtx.VolumeProviders = append(buildCtx.VolumeProviders, p)
	}
	if p := kerberosSecretProvisioner(cr, buildCtx.RoleName); p != nil {
		buildCtx.VolumeProviders = append(buildCtx.VolumeProviders, p)
	}

	// Init containers / native sidecars (format-namenode, format-zk, zkfc for NameNode;
	// wait-for-namenodes for DataNode) go into the framework-provided manager (always non-nil).
	registerRoleSidecars(cr, buildCtx.RoleName, h.ConfigMountPath, buildCtx.SidecarManager)

	// The NameNode web UI is fronted by the framework's oauth2-proxy sidecar when OIDC is enabled.
	if buildCtx.RoleName == hdfsv1alpha1.NameNodeRoleName && oidcEnabled(cr) {
		if err := ensureOidcCookieSecret(ctx, k8sClient, cr); err != nil {
			return nil, err
		}
		provider, err := oidcSidecarProvider(ctx, k8sClient, cr)
		if err != nil {
			return nil, err
		}
		if provider != nil {
			buildCtx.SidecarManager.Register(provider, &sidecar.SidecarConfig{Enabled: true})
		}
	}

	resources, err := h.BaseRoleGroupHandler.BuildResources(ctx, k8sClient, cr, buildCtx)
	if err != nil {
		return nil, err
	}

	// Publish the per-role metrics Service so Prometheus can scrape the daemon's /jmx endpoint.
	if svc := metricsService(buildCtx); svc != nil {
		resources.MetricsService = svc
	}

	return resources, nil
}

// registerRoleSidecars registers the role's init containers and native sidecars into the
// framework-provided SidecarManager. StaticContainerProvider injects non-restart containers as init
// containers and RestartPolicy=Always containers as native sidecars.
func registerRoleSidecars(cr *hdfsv1alpha1.HdfsCluster, roleName, confDir string, sm *sidecar.SidecarManager) {
	var containers []corev1.Container
	switch roleName {
	case hdfsv1alpha1.NameNodeRoleName:
		containers = []corev1.Container{
			formatNameNodeContainer(cr, confDir),
			formatZookeeperContainer(cr, confDir),
			zkfcContainer(cr, confDir),
		}
	case hdfsv1alpha1.DataNodeRoleName:
		containers = []corev1.Container{
			waitForNameNodesContainer(cr, confDir),
		}
	default:
		return
	}
	for _, c := range containers {
		sm.Register(sidecar.NewStaticContainerProvider(c), &sidecar.SidecarConfig{Enabled: true})
	}
}

// commonEnv builds the env vars every HDFS container needs. HADOOP_CONF_DIR points at the path
// where the framework mounts the config ConfigMap. POD_NAME and ZOOKEEPER are the ${env.X}
// references the generated config depends on.
func commonEnv(cr *hdfsv1alpha1.HdfsCluster, confDir string) []corev1.EnvVar {
	env := []corev1.EnvVar{
		{Name: constants.EnvHadoopHome, Value: hdfsv1alpha1.HadoopHome},
		{Name: constants.EnvHadoopConfDir, Value: confDir},
		{Name: constants.EnvPodName, ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
		}},
	}
	if cr.Spec.ClusterConfig != nil && cr.Spec.ClusterConfig.ZookeeperConfigMapName != "" {
		env = append(env, corev1.EnvVar{
			Name: constants.EnvZookeeper,
			ValueFrom: &corev1.EnvVarSource{
				ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: cr.Spec.ClusterConfig.ZookeeperConfigMapName},
					Key:                  constants.ZookeeperDiscoveryKey,
				},
			},
		})
	}
	if kerberosEnabled(cr) {
		krb5 := path.Join(constant.KubedoopKerberosDir, constants.Krb5ConfFile)
		env = append(env,
			corev1.EnvVar{Name: "KRB5_CONFIG", Value: krb5},
			corev1.EnvVar{Name: "KRB5_CLIENT_KTNAME", Value: path.Join(constant.KubedoopKerberosDir, constants.KeytabFile)},
			corev1.EnvVar{Name: "HADOOP_OPTS", Value: "-Djava.security.krb5.conf=" + krb5},
		)
	}
	return env
}

// Ensure interface implementations.
var (
	_ reconciler.RoleGroupHandler[*hdfsv1alpha1.HdfsCluster] = &HdfsRoleGroupHandler{}
	_ reconciler.RoleProvider[*hdfsv1alpha1.HdfsCluster]     = &HdfsRoleGroupHandler{}
	// HdfsCluster exposes its Vector aggregator ConfigMap so the framework wires the Vector sidecar.
	_ reconciler.VectorAggregatorProvider = &hdfsv1alpha1.HdfsCluster{}
)
