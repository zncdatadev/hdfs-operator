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
	"fmt"
	"path"
	"strings"

	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/constant"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constants"
	"github.com/zncdatadev/hdfs-operator/internal/product"
)

// Pod volume names. "config" and "data" are created by the framework (config ConfigMap and the
// data PVC via StorageMountPath); listenerVolumeName is registered via buildCtx.VolumeProviders.
const (
	configVolumeName = "config"
	dataVolumeName   = "data"
)

// Init/sidecar container names.
const (
	formatNameNodeContainerName   = "format-namenode"
	formatZookeeperContainerName  = "format-zookeeper"
	waitForNameNodesContainerName = "wait-for-namenodes"
	zkfcContainerName             = "zkfc"
)

// hdfsBin is the hdfs CLI path inside the image.
var hdfsBin = hdfsv1alpha1.HadoopHome + "/bin/hdfs"

// listenerMountPath is where the listener CSI volume is mounted (SDK convention:
// <KubedoopListenerDir>/<volumeName>). The pod reads its address from here.
func listenerMountPath() string {
	return path.Join(constant.KubedoopListenerDir, listenerVolumeName)
}

// roleMetricPorts is each role's native metrics HTTP port (where the daemon serves /jmx),
// published by the per-role metrics Service.
var roleMetricPorts = map[string]int32{
	hdfsv1alpha1.NameNodeRoleName:    hdfsv1alpha1.NameNodeMetricPort,
	hdfsv1alpha1.DataNodeRoleName:    hdfsv1alpha1.DataNodeMetricPort,
	hdfsv1alpha1.JournalNodeRoleName: hdfsv1alpha1.JournalNodeMetricPort,
}

// metricsService builds the role group's headless metrics Service ({resource}-metrics), scraping
// the "metric" container port. The selector uses the framework's default identity labels
// (instance + component) so it matches the role's pods.
func metricsService(buildCtx *reconciler.RoleGroupBuildContext) *corev1.Service {
	port, ok := roleMetricPorts[buildCtx.RoleName]
	if !ok {
		return nil
	}
	labels := map[string]string{
		"app.kubernetes.io/instance":  buildCtx.ClusterName,
		"app.kubernetes.io/component": buildCtx.RoleName,
	}
	return builder.NewMetricsServiceBuilder(buildCtx.ResourceName, buildCtx.ClusterNamespace, port, labels).
		WithPortName(hdfsv1alpha1.MetricName).
		WithTargetPortName(hdfsv1alpha1.MetricName).
		Build()
}

// roleHTTPSPorts is each role's HTTPS web port, exposed only when TLS is enabled.
var roleHTTPSPorts = map[string]int32{
	hdfsv1alpha1.NameNodeRoleName:    hdfsv1alpha1.NameNodeHttpsPort,
	hdfsv1alpha1.DataNodeRoleName:    hdfsv1alpha1.DataNodeHttpsPort,
	hdfsv1alpha1.JournalNodeRoleName: hdfsv1alpha1.JournalNodeHttpsPort,
}

// tlsOn reports whether the CR enables TLS.
func tlsOn(cr *hdfsv1alpha1.HdfsCluster) bool {
	return cr.Spec.ClusterConfig != nil &&
		cr.Spec.ClusterConfig.Authentication != nil &&
		cr.Spec.ClusterConfig.Authentication.Tls != nil
}

// httpsContainerPort returns the role's HTTPS container port (named so the listener exposes it and
// projects ${env.HTTPS_PORT}), or nil for an unknown role.
func httpsContainerPort(roleName string) *corev1.ContainerPort {
	port, ok := roleHTTPSPorts[roleName]
	if !ok {
		return nil
	}
	return &corev1.ContainerPort{Name: hdfsv1alpha1.HttpsName, ContainerPort: port, Protocol: corev1.ProtocolTCP}
}

// resolveImage returns the CR-driven image (resolved with the product name), or the operator
// default when the CR does not set spec.image.
func resolveImage(cr *hdfsv1alpha1.HdfsCluster) string {
	if cr.Spec.Image != nil {
		if img := cr.Spec.Image.GetImage(constants.ProductName); img != "" {
			return img
		}
	}
	return defaultImage()
}

// exportPodAddressScript reads the pod's externally reachable address and ports from the listener
// mount and exports them (POD_ADDRESS, plus <NAME>_PORT for each registered port), so the
// generated config's ${env.POD_ADDRESS}/${env.IPC_PORT}/${env.DATA_PORT} resolve at runtime.
func exportPodAddressScript() string {
	l := listenerMountPath()
	return fmt.Sprintf(`if [[ -d %[1]s ]]; then
  export POD_ADDRESS=$(cat %[1]s/default-address/address)
  for i in %[1]s/default-address/ports/*; do
      export $(basename "$i" | tr a-z A-Z)_PORT="$(cat "$i")"
  done
fi`, l)
}

// exportKerberosRealmScript resolves the Kerberos realm from the mounted krb5.conf and exports it
// as KERBEROS_REALM, which the generated principals reference as ${env.KERBEROS_REALM}. Empty when
// Kerberos is disabled.
func exportKerberosRealmScript(cr *hdfsv1alpha1.HdfsCluster) string {
	if !kerberosEnabled(cr) {
		return ""
	}
	krb5 := path.Join(constant.KubedoopKerberosDir, constants.Krb5ConfFile)
	return fmt.Sprintf("export KERBEROS_REALM=$(grep -oP 'default_realm = \\K.*' %s)\n", krb5)
}

// kerberosMount is the volume mount for the Kerberos keytab + krb5.conf.
func kerberosMount() corev1.VolumeMount {
	return corev1.VolumeMount{Name: constants.KerberosSecretVolumeName, MountPath: constant.KubedoopKerberosDir}
}

// kinitScriptPrefix returns the realm-export + kinit prelude a Kerberos client operation (e.g.
// `hdfs haadmin`, `zkfc -formatZK`) needs to obtain a TGT, or "" when Kerberos is disabled.
// serviceName is the role's Kerberos short name (nn/dn/jn).
func kinitScriptPrefix(cr *hdfsv1alpha1.HdfsCluster, serviceName string) string {
	if !kerberosEnabled(cr) {
		return ""
	}
	krb5 := path.Join(constant.KubedoopKerberosDir, constants.Krb5ConfFile)
	keytab := path.Join(constant.KubedoopKerberosDir, constants.KeytabFile)
	principal := fmt.Sprintf("%s/%s.%s.svc.cluster.local@${KERBEROS_REALM}", serviceName, cr.Name, cr.Namespace)
	return fmt.Sprintf("export KERBEROS_REALM=$(grep -oP 'default_realm = \\K.*' %s)\nkinit -kt %s \"%s\"\n", krb5, keytab, principal)
}

// mainContainerScript is the primary container's startup: resolve the Kerberos realm (if any),
// export the listener address, then exec the HDFS daemon for the role.
func mainContainerScript(cr *hdfsv1alpha1.HdfsCluster, roleName string) string {
	sub := map[string]string{
		hdfsv1alpha1.NameNodeRoleName:    "namenode",
		hdfsv1alpha1.DataNodeRoleName:    "datanode",
		hdfsv1alpha1.JournalNodeRoleName: "journalnode",
	}[roleName]
	return exportKerberosRealmScript(cr) + exportPodAddressScript() + "\n" + fmt.Sprintf("exec %s %s", hdfsBin, sub)
}

// newContainer builds a bash-driven container with the common env and the given volume mounts.
// When restartAlways is true the container is a native sidecar (K8s 1.28+); otherwise it is a
// plain init container.
func newContainer(name string, cr *hdfsv1alpha1.HdfsCluster, confDir, script string, mounts []corev1.VolumeMount, restartAlways bool) corev1.Container {
	// Kerberos containers also mount the keytab + krb5.conf so they can authenticate.
	if kerberosEnabled(cr) {
		mounts = append(mounts, kerberosMount())
	}
	c := corev1.Container{
		Name:         name,
		Image:        resolveImage(cr),
		Command:      []string{"/bin/bash", "-c"},
		Args:         []string{script},
		Env:          commonEnv(cr, confDir),
		VolumeMounts: mounts,
	}
	if restartAlways {
		c.RestartPolicy = ptr.To(corev1.ContainerRestartPolicyAlways)
	}
	return c
}

// configMount / dataMount are the shared volume mounts.
func configMount(confDir string) corev1.VolumeMount {
	return corev1.VolumeMount{Name: configVolumeName, MountPath: confDir}
}

func dataMount() corev1.VolumeMount {
	return corev1.VolumeMount{Name: dataVolumeName, MountPath: constant.KubedoopDataDir}
}

// formatNameNodeContainer formats this NameNode pod on first start: it becomes the active
// namenode if none is active yet, otherwise it bootstraps as standby. Already-formatted pods are
// skipped (VERSION file present).
func formatNameNodeContainer(cr *hdfsv1alpha1.HdfsCluster, confDir string) corev1.Container {
	ids := strings.Join(product.NameNodePodNames(cr), " ")
	script := fmt.Sprintf(`echo "Formatting namenode $POD_NAME. Checking for an active namenode:"
for namenode_id in %[1]s; do
    echo -n "Checking pod $namenode_id... "
    SERVICE_STATE=$(%[2]s haadmin -getServiceState "$namenode_id" | tail -n1 || true)
    if [ "$SERVICE_STATE" == "active" ]; then ACTIVE_NAMENODE=$namenode_id; echo "active"; break; fi
    echo ""
done
if [ ! -f "%[3]s/current/VERSION" ]; then
    if [ -z ${ACTIVE_NAMENODE+x} ]; then
        echo "Formatting $POD_NAME as the active namenode."
        %[2]s namenode -format -noninteractive
    else
        echo "Bootstrapping $POD_NAME as a standby namenode."
        %[2]s namenode -bootstrapStandby -nonInteractive
    fi
else
    echo "$POD_NAME already formatted. Skipping."
fi`, ids, hdfsBin, hdfsv1alpha1.NameNodeRootDataDir)
	script = kinitScriptPrefix(cr, kerberosServiceNames[hdfsv1alpha1.NameNodeRoleName]) + script
	return newContainer(formatNameNodeContainerName, cr, confDir, script,
		[]corev1.VolumeMount{configMount(confDir), dataMount()}, false)
}

// formatZookeeperContainer formats the HA ZNode once, from the first namenode pod (ordinal 0).
func formatZookeeperContainer(cr *hdfsv1alpha1.HdfsCluster, confDir string) corev1.Container {
	script := fmt.Sprintf(`if [[ "0" -eq "$(echo "$POD_NAME" | sed -e 's/.*-//')" ]]; then
    echo "Formatting ZooKeeper HA znode..."
    set +e
    %[1]s zkfc -formatZK -nonInteractive
    EXITCODE=$?
    set -e
    if [[ $EXITCODE -eq 0 ]]; then echo "Successfully formatted"
    elif [[ $EXITCODE -eq 2 ]]; then echo "ZNode already existed, did nothing"
    else echo "ZooKeeper format failed with exit code $EXITCODE"; exit $EXITCODE; fi
else
    echo "ZooKeeper already formatted by pod 0."
fi`, hdfsBin)
	script = kinitScriptPrefix(cr, kerberosServiceNames[hdfsv1alpha1.NameNodeRoleName]) + script
	return newContainer(formatZookeeperContainerName, cr, confDir, script,
		[]corev1.VolumeMount{configMount(confDir)}, false)
}

// waitForNameNodesContainer blocks DataNode startup until the namenodes report a HA state.
func waitForNameNodesContainer(cr *hdfsv1alpha1.HdfsCluster, confDir string) corev1.Container {
	ids := strings.Join(product.NameNodePodNames(cr), " ")
	script := fmt.Sprintf(`echo "Waiting for namenodes to get ready:"
n=0
while [ ${n} -lt 12 ]; do
    ALL_NODES_READY=true
    for namenode_id in %[1]s; do
        echo -n "Checking pod $namenode_id... "
        SERVICE_STATE=$(%[2]s haadmin -getServiceState "$namenode_id" | tail -n1 || true)
        if [ "$SERVICE_STATE" = "active" ] || [ "$SERVICE_STATE" = "standby" ]; then
            echo "$SERVICE_STATE"
        else
            echo "not ready"; ALL_NODES_READY=false
        fi
    done
    if [ "$ALL_NODES_READY" == "true" ]; then echo "All namenodes ready!"; break; fi
    n=$((n + 1)); sleep 5
done`, ids, hdfsBin)
	script = kinitScriptPrefix(cr, kerberosServiceNames[hdfsv1alpha1.DataNodeRoleName]) + script
	return newContainer(waitForNameNodesContainerName, cr, confDir, script,
		[]corev1.VolumeMount{configMount(confDir)}, false)
}

// zkfcContainer runs the ZooKeeper Failover Controller as a native sidecar next to the namenode.
func zkfcContainer(cr *hdfsv1alpha1.HdfsCluster, confDir string) corev1.Container {
	return newContainer(zkfcContainerName, cr, confDir, fmt.Sprintf("exec %s zkfc", hdfsBin),
		[]corev1.VolumeMount{configMount(confDir)}, true)
}
