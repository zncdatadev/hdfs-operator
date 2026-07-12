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
	"path"

	"github.com/zncdatadev/operator-go/pkg/config"
	"github.com/zncdatadev/operator-go/pkg/constant"
	"github.com/zncdatadev/operator-go/pkg/listener"
	"github.com/zncdatadev/operator-go/pkg/productlogging"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	"github.com/zncdatadev/operator-go/pkg/security"
	"github.com/zncdatadev/operator-go/pkg/sidecar"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constants"
)

// HdfsRoleGroupHandler builds HDFS role group resources. It embeds the SDK
// BaseRoleGroupHandler so the framework owns resource orchestration — the ConfigMap (rendered
// from the merged config, including the product config from product.ComputeConfig), Services,
// the StatefulSet, and the PDB.
//
// NOTE (skeleton): the product-specific pieces HDFS needs beyond the framework defaults —
// ZKFC sidecar, format-namenode / format-zk / wait-for-namenodes init containers, the
// discovery ConfigMap, Kerberos/TLS volumes — are reintroduced in later refactor phases via a
// BuildResources override and the SDK's declarative provisioners.
type HdfsRoleGroupHandler struct {
	*reconciler.BaseRoleGroupHandler[*hdfsv1alpha1.HdfsCluster]
}

// NewHdfsRoleGroupHandler creates the handler and configures the framework defaults for the
// three HDFS roles.
func NewHdfsRoleGroupHandler(scheme *runtime.Scheme) *HdfsRoleGroupHandler {
	base := reconciler.NewBaseRoleGroupHandler[*hdfsv1alpha1.HdfsCluster](defaultImage(), scheme)

	// core-site.xml / hdfs-site.xml are rendered as Hadoop XML by the default formats.
	base.ConfigGenerator = config.NewMultiFormatConfigGenerator()
	base.ConfigGenerator.RegisterDefaultFormats()

	// HDFS reads its config from the Hadoop config dir.
	base.ConfigMountPath = hdfsv1alpha1.HadoopHome + "/etc/hadoop"

	// Persist role data (NameNode name.dir, JournalNode edits.dir, DataNode data.dir all live
	// under KubedoopDataDir). The framework builds the VolumeClaimTemplate from the role group's
	// configured storage and mounts it here.
	base.StorageMountPath = constant.KubedoopDataDir

	setRolePorts(base)
	setRoleLogging(base)

	return &HdfsRoleGroupHandler{BaseRoleGroupHandler: base}
}

// roleContainerNames maps each HDFS role to its primary (daemon) container name. These become
// both the renamed StatefulSet container and the per-container logging key
// (logging.containers.<name>) in the CRD.
var roleContainerNames = map[string]string{
	hdfsv1alpha1.NameNodeRoleName:    constants.NameNodeContainerName,
	hdfsv1alpha1.DataNodeRoleName:    constants.DataNodeContainerName,
	hdfsv1alpha1.JournalNodeRoleName: constants.JournalNodeContainerName,
}

// setRoleLogging gives each role its own primary container name and declarative log4j logging.
// The SDK renders a log4j.properties from the merged CRD logging spec into the role group
// ConfigMap (mounted at HADOOP_CONF_DIR) and, when the Vector agent is enabled, ships the
// container's log files. Uses the SDK per-role hooks (operator-go #531) since HDFS container
// names differ per role.
func setRoleLogging(base *reconciler.BaseRoleGroupHandler[*hdfsv1alpha1.HdfsCluster]) {
	for role, cname := range roleContainerNames {
		base.SetRoleMainContainerName(role, cname)
		base.SetRoleLoggingContainers(role, []productlogging.ContainerLogging{
			{Container: cname, Framework: productlogging.LoggingFrameworkLog4j},
		})
	}
}

// listenerVolumeName is the name of the listener CSI volume mounted on every HDFS pod.
const listenerVolumeName = "listener"

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

// setRolePorts declares the container/service ports for each role.
func setRolePorts(base *reconciler.BaseRoleGroupHandler[*hdfsv1alpha1.HdfsCluster]) {
	rolePorts := map[string][]struct {
		name string
		port int32
	}{
		hdfsv1alpha1.NameNodeRoleName: {
			{hdfsv1alpha1.RpcName, hdfsv1alpha1.NameNodeRpcPort},
			{hdfsv1alpha1.HttpName, hdfsv1alpha1.NameNodeHttpPort},
		},
		hdfsv1alpha1.DataNodeRoleName: {
			{hdfsv1alpha1.DataName, hdfsv1alpha1.DataNodeDataPort},
			{hdfsv1alpha1.HttpName, hdfsv1alpha1.DataNodeHttpPort},
			{hdfsv1alpha1.IpcName, hdfsv1alpha1.DataNodeIpcPort},
		},
		hdfsv1alpha1.JournalNodeRoleName: {
			{hdfsv1alpha1.RpcName, hdfsv1alpha1.JournalNodeRpcPort},
			{hdfsv1alpha1.HttpName, hdfsv1alpha1.JournalNodeHttpPort},
		},
	}

	for role, ports := range rolePorts {
		containerPorts := make([]corev1.ContainerPort, 0, len(ports))
		servicePorts := make([]corev1.ServicePort, 0, len(ports))
		for _, p := range ports {
			containerPorts = append(containerPorts, corev1.ContainerPort{
				Name:          p.name,
				ContainerPort: p.port,
				Protocol:      corev1.ProtocolTCP,
			})
			servicePorts = append(servicePorts, corev1.ServicePort{
				Name:     p.name,
				Port:     p.port,
				Protocol: corev1.ProtocolTCP,
			})
		}
		base.SetRoleContainerPorts(role, containerPorts)
		base.SetRoleServicePorts(role, servicePorts)
	}
}

// BuildResources delegates to the framework. Product-specific resources are reintroduced here
// in later phases (see type doc).
func (h *HdfsRoleGroupHandler) BuildResources(
	ctx context.Context,
	k8sClient client.Client,
	cr *hdfsv1alpha1.HdfsCluster,
	buildCtx *reconciler.RoleGroupBuildContext,
) (*reconciler.RoleGroupResources, error) {
	// Register the per-pod listener CSI volume before the framework builds the StatefulSet. The
	// pod reads its externally reachable address from this mount (used for DataNode registration
	// and address advertisement). buildCtx.VolumeProviders is per-role/per-reconcile, so this
	// never accumulates across reconciles.
	buildCtx.VolumeProviders = append(buildCtx.VolumeProviders, newListenerProvisioner())

	// Register the TLS secret volume (keystore/truststore) when the CR enables TLS. The secret
	// provisioner satisfies the same VolumeProvider contract as the listener volume.
	if p := tlsSecretProvisioner(cr); p != nil {
		buildCtx.VolumeProviders = append(buildCtx.VolumeProviders, p)
	}

	// Register the Kerberos secret volume (keytab + krb5.conf) for the role when enabled.
	if p := kerberosSecretProvisioner(cr, buildCtx.RoleName); p != nil {
		buildCtx.VolumeProviders = append(buildCtx.VolumeProviders, p)
	}

	// Register the role's init containers / native sidecars (format-namenode, format-zk, zkfc for
	// NameNode; wait-for-namenodes for DataNode) so the framework injects them during the build.
	if sm := roleSidecarManager(cr, buildCtx.RoleName, h.ConfigMountPath); sm != nil {
		buildCtx.SidecarManager = sm
	}

	resources, err := h.BaseRoleGroupHandler.BuildResources(ctx, k8sClient, cr, buildCtx)
	if err != nil {
		return nil, err
	}

	if resources.StatefulSet != nil {
		h.applyMainContainer(cr, buildCtx.RoleName, resources.StatefulSet)
	}

	return resources, nil
}

// roleSidecarManager returns a SidecarManager carrying the role's init containers and native
// sidecars, or nil when the role needs none (JournalNode). StaticContainerProvider injects
// non-restart containers as init containers and RestartPolicy=Always containers as native
// sidecars.
func roleSidecarManager(cr *hdfsv1alpha1.HdfsCluster, roleName, confDir string) *sidecar.SidecarManager {
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
		return nil
	}

	sm := sidecar.NewSidecarManager()
	for _, c := range containers {
		sm.Register(sidecar.NewStaticContainerProvider(c), &sidecar.SidecarConfig{Enabled: true})
	}
	return sm
}

// applyMainContainer sets the CR-driven image, the common env vars, and the role startup command
// (which exports the listener address then execs the HDFS daemon) on the primary container.
func (h *HdfsRoleGroupHandler) applyMainContainer(cr *hdfsv1alpha1.HdfsCluster, roleName string, sts *appsv1.StatefulSet) {
	containers := sts.Spec.Template.Spec.Containers
	if len(containers) == 0 {
		return
	}
	c := &containers[0]

	if cr.Spec.Image != nil {
		if image := cr.Spec.Image.GetImage(constants.ProductName); image != "" {
			c.Image = image
			c.ImagePullPolicy = cr.Spec.Image.GetPullPolicy()
		}
	}

	c.Env = append(c.Env, commonEnv(cr, h.ConfigMountPath)...)
	if heap := jvmHeapEnv(roleName, c); heap != nil {
		c.Env = append(c.Env, *heap)
	}
	c.Command = []string{"/bin/bash", "-c"}
	c.Args = []string{mainContainerScript(cr, roleName)}
}

// roleOptsEnv maps each role to the Hadoop env var that carries its daemon JVM options.
var roleOptsEnv = map[string]string{
	hdfsv1alpha1.NameNodeRoleName:    "HDFS_NAMENODE_OPTS",
	hdfsv1alpha1.DataNodeRoleName:    "HDFS_DATANODE_OPTS",
	hdfsv1alpha1.JournalNodeRoleName: "HDFS_JOURNALNODE_OPTS",
}

// jvmHeapEnv sizes the daemon's max heap (-Xmx) from the container's memory limit (which the
// framework set from the role group's configured resources), scaled by JvmHeapFactor. Returns
// nil when no memory limit is set, leaving the image's JVM defaults in place.
func jvmHeapEnv(roleName string, c *corev1.Container) *corev1.EnvVar {
	envName := roleOptsEnv[roleName]
	if envName == "" {
		return nil
	}
	limit, ok := c.Resources.Limits[corev1.ResourceMemory]
	if !ok || limit.IsZero() {
		return nil
	}
	heapMi := int64(float64(limit.Value())*hdfsv1alpha1.JvmHeapFactor) / (1024 * 1024)
	if heapMi < 1 {
		return nil
	}
	return &corev1.EnvVar{Name: envName, Value: fmt.Sprintf("-Xmx%dm", heapMi)}
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

// defaultImage is the operator's default HDFS image. The CR's spec.image overrides it per
// reconcile in BuildResources.
func defaultImage() string {
	return fmt.Sprintf("%s/%s:%s-kubedoop%s",
		constants.DefaultImageRepo,
		constants.ProductName,
		constants.DefaultProductVersion,
		constants.DefaultKubedoopVersion,
	)
}

// Ensure interface implementation.
var _ reconciler.RoleGroupHandler[*hdfsv1alpha1.HdfsCluster] = &HdfsRoleGroupHandler{}
