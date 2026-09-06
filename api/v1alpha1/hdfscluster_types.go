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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"

	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/common"
	"github.com/zncdatadev/operator-go/pkg/constant"
	"github.com/zncdatadev/operator-go/pkg/listener"
)

// Role names. These are the keys used in the GenericClusterSpec.Roles map and in
// {cluster}-{role}-{group} resource names produced by the SDK.
const (
	NameNodeRoleName    = "namenode"
	DataNodeRoleName    = "datanode"
	JournalNodeRoleName = "journalnode"
)

// file name
const (
	CoreSiteFileName = "core-site.xml"
	HdfsSiteFileName = "hdfs-site.xml"
	// SslServerFileName see https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/EncryptedShuffle.html
	SslServerFileName = "ssl-server.xml"
	SslClientFileName = "ssl-client.xml"
	// SecurityFileName this is for java security, not for hadoop
	SecurityFileName = "security.properties"
	// HadoopPolicyFileName see: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ServiceLevelAuth.html
	HadoopPolicyFileName = "hadoop-policy.xml"
	Log4jFileName        = "log4j.properties"
)

// volume name
const (
	ListenerVolumeName                    = "listener"
	TlsStoreVolumeName                    = "tls"
	KerberosVolumeName                    = "kerberos"
	KubedoopLogVolumeMountName            = "log"
	DataVolumeMountName                   = "data"
	HdfsConfigVolumeMountName             = "hdfs-config"
	HdfsLogVolumeMountName                = "hdfs-log-config"
	ZkfcConfigVolumeMountName             = "zkfc-config"
	ZkfcLogVolumeMountName                = "zkfc-log-config"
	FormatNamenodesConfigVolumeMountName  = "format-namenodes-config"
	FormatNamenodesLogVolumeMountName     = "format-namenodes-log-config"
	FormatZookeeperConfigVolumeMountName  = "format-zookeeper-config"
	FormatZookeeperLogVolumeMountName     = "format-zookeeper-log-config"
	WaitForNamenodesConfigVolumeMountName = "wait-for-namenodes-config"
	WaitForNamenodesLogVolumeMountName    = "wait-for-namenodes-log-config"

	JvmHeapFactor = 0.8
)

// directory
const (
	NameNodeRootDataDir    = constant.KubedoopDataDir + "namenode"
	JournalNodeRootDataDir = constant.KubedoopDataDir + "journalnode"

	DataNodeRootDataDirPrefix = constant.KubedoopDataDir
	DataNodeRootDataDirSuffix = "/datanode"

	// KubedoopRoot already ends with a slash, so no extra separator is needed here.
	HadoopHome = constant.KubedoopRoot + "hadoop"
)

// port names
const (
	MetricName = "metric"
	HttpName   = "http"
	HttpsName  = "https"
	RpcName    = "rpc"
	IpcName    = "ipc"
	DataName   = "data"
)

// native metrics port
const (
	NameNodeNativeMetricsHttpPort     = 9870
	NameNodeNativeMetricsHttpsPort    = 9871
	DataNodeNativeMetricsHttpPort     = 9864
	DataNodeNativeMetricsHttpsPort    = 9865
	JournalNodeNativeMetricsHttpPort  = 8480
	JournalNodeNativeMetricsHttpsPort = 8481
)

// service port
const (
	NameNodeHttpPort      = 9870
	NameNodeHttpsPort     = 9871
	NameNodeRpcPort       = 8020
	NameNodeMetricPort    = 8183
	DataNodeMetricPort    = 8082
	DataNodeHttpPort      = 9864
	DataNodeHttpsPort     = 9865
	DataNodeDataPort      = 9866
	DataNodeIpcPort       = 9867
	JournalNodeMetricPort = 8081
	JournalNodeRpcPort    = 8485
	JournalNodeHttpPort   = 8480
	JournalNodeHttpsPort  = 8481
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// HdfsCluster is the Schema for the hdfsclusters API
type HdfsCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HdfsClusterSpec   `json:"spec,omitempty"`
	Status HdfsClusterStatus `json:"status,omitempty"`
}

// HdfsClusterStatus defines the observed state of HdfsCluster.
// It embeds the SDK GenericClusterStatus (Conditions, RoleGroups, ObservedGeneration)
// and can be extended with HDFS-specific status fields.
type HdfsClusterStatus struct {
	commonsv1alpha1.GenericClusterStatus `json:",inline"`
}

// +kubebuilder:object:root=true

// HdfsClusterList contains a list of HdfsCluster
type HdfsClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HdfsCluster `json:"items"`
}

// HdfsClusterSpec defines the desired state of HdfsCluster
type HdfsClusterSpec struct {
	// Image specifies the HDFS container image configuration. Fields left empty are filled from
	// the handler's ImageDefaults on every reconcile (operator-go #581); the webhook only
	// validates that the result resolves.
	// +kubebuilder:validation:Optional
	Image *commonsv1alpha1.ImageSpec `json:"image,omitempty"`

	// ClusterOperation controls operator behavior at runtime (pause/stop).
	// +kubebuilder:validation:Optional
	ClusterOperation *commonsv1alpha1.ClusterOperationSpec `json:"clusterOperation,omitempty"`

	// ClusterConfig holds HDFS cluster-wide, product-specific configuration. It is NOT part of
	// the SDK GenericClusterSpec; the product handler/ProductConfig reads it directly.
	// +kubebuilder:validation:Required
	ClusterConfig *ClusterConfigSpec `json:"clusterConfig,omitempty"`

	// NameNodes defines the NameNode role (metadata servers; HA usually runs 2+).
	// +kubebuilder:validation:Required
	NameNodes *NameNodeSpec `json:"nameNodes,omitempty"`

	// DataNodes defines the DataNode role (storage workers).
	// +kubebuilder:validation:Required
	DataNodes *DataNodeSpec `json:"dataNodes,omitempty"`

	// JournalNodes defines the JournalNode role (metadata edit log quorum; odd replica count).
	// +kubebuilder:validation:Required
	JournalNodes *JournalNodeSpec `json:"journalNodes,omitempty"`
}

// NameNodeSpec embeds the HDFS role shape and can carry NameNode-specific fields later.
type NameNodeSpec struct {
	RoleSpec `json:",inline"`
}

// DataNodeSpec embeds the HDFS role shape and can carry DataNode-specific fields later.
type DataNodeSpec struct {
	RoleSpec `json:",inline"`
}

// JournalNodeSpec embeds the HDFS role shape and can carry JournalNode-specific fields later.
type JournalNodeSpec struct {
	RoleSpec `json:",inline"`
}

// RoleSpec keeps the framework-owned role fields while allowing HDFS to extend the folded config.
// GetSpec projects this product shape onto commons RoleSpec for GenericReconciler.
type RoleSpec struct {
	// Config contains workload runtime configuration defaults for all RoleGroups.
	// Each RoleGroup inherits these values and can selectively override them.
	// +kubebuilder:validation:Optional
	Config *ConfigSpec `json:"config,omitempty"`

	// RoleGroups defines the role group configurations. Each RoleGroup maps to a StatefulSet.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:MaxProperties=256
	// +kubebuilder:validation:XValidation:rule=`self.all(k, size(k) <= 63 && k.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'))`,message=`each role group name must be a lowercase RFC 1123 label (lowercase alphanumerics and '-', starting and ending with an alphanumeric, at most 63 characters): role group names become part of the name and labels of every resource built for the group`
	RoleGroups map[string]RoleGroupSpec `json:"roleGroups,omitempty"`

	// RoleConfig contains Kubernetes-level role management controls that role groups do not inherit.
	// +kubebuilder:validation:Optional
	RoleConfig *commonsv1alpha1.RoleConfigSpec `json:"roleConfig,omitempty"`

	// ConfigOverrides applies configuration-file overrides to all role groups.
	// +kubebuilder:validation:Optional
	ConfigOverrides map[string]map[string]string `json:"configOverrides,omitempty"`

	// EnvOverrides applies environment-variable overrides to all role groups.
	// +kubebuilder:validation:Optional
	EnvOverrides map[string]string `json:"envOverrides,omitempty"`

	// CliOverrides replaces the declared command arguments for all role groups.
	// +kubebuilder:validation:Optional
	CliOverrides []string `json:"cliOverrides,omitempty"`

	// PodOverrides applies a strategic merge patch to all role groups.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Type=object
	PodOverrides *k8sruntime.RawExtension `json:"podOverrides,omitempty"`
}

// ConfigSpec composes the framework-owned role-group configuration with HDFS-specific fields.
type ConfigSpec struct {
	*commonsv1alpha1.RoleGroupConfigSpec `json:",inline"`

	// ListenerClass selects how this role group is exposed. It deliberately has no structural
	// default: role -> role-group inheritance must see whether the field was omitted. The runtime
	// fold applies cluster-internal only when neither user layer states a value.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Enum=cluster-internal;external-unstable;external-stable
	ListenerClass *listener.ListenerClass `json:"listenerClass,omitempty"`
}

// RoleGroupSpec defines one StatefulSet and its role-group-level overrides.
type RoleGroupSpec struct {
	// Replicas is the number of pod replicas for this role group.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=1
	// +kubebuilder:validation:Optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Config contains role-group-level configuration and overrides the role-level Config.
	// +kubebuilder:validation:Optional
	Config *ConfigSpec `json:"config,omitempty"`

	// ConfigOverrides overrides role-level configuration-file entries per key.
	// +kubebuilder:validation:Optional
	ConfigOverrides map[string]map[string]string `json:"configOverrides,omitempty"`

	// EnvOverrides overrides role-level environment variables per key.
	// +kubebuilder:validation:Optional
	EnvOverrides map[string]string `json:"envOverrides,omitempty"`

	// CliOverrides replaces the role-level command arguments.
	// +kubebuilder:validation:Optional
	CliOverrides []string `json:"cliOverrides,omitempty"`

	// PodOverrides applies a strategic merge patch after role-level pod overrides.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Type=object
	PodOverrides *k8sruntime.RawExtension `json:"podOverrides,omitempty"`
}

// GetReplicas returns the requested replica count, defaulting to one like commons RoleGroupSpec.
func (r *RoleGroupSpec) GetReplicas() int32 {
	if r == nil || r.Replicas == nil {
		return 1
	}
	return *r.Replicas
}

type ClusterConfigSpec struct {
	// +kubebuilder:validation:Optional
	VectorAggregatorConfigMapName string `json:"vectorAggregatorConfigMapName,omitempty"`

	// +kubebuilder:validation:Optional
	Authentication *AuthenticationSpec `json:"authentication,omitempty"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="cluster.local"
	ClusterDomain string `json:"clusterDomain,omitempty"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=1
	DfsReplication int32 `json:"dfsReplication,omitempty"`

	// +kubebuilder:validation:required
	ZookeeperConfigMapName string `json:"zookeeperConfigMapName,omitempty"`
}

type AuthenticationSpec struct {
	// AuthenticationClass references an authentication.kubedoop.dev AuthenticationClass. For OIDC
	// it must name a class whose provider.oidc block describes the issuer; the operator fronts the
	// NameNode web UI with an oauth2-proxy sidecar configured from it.
	// +kubebuilder:validation:Optional
	AuthenticationClass string `json:"authenticationClass,omitempty"`

	// +kubebuilder:validation:Optional
	Oidc *OidcSpec `json:"oidc,omitempty"`

	// +kubebuilder:validation:Optional
	Tls *TlsSpec `json:"tls,omitempty"`

	// +kubebuilder:validation:Optional
	Kerberos *KerberosSpec `json:"kerberos,omitempty"`
}

// OidcSpec defines the OIDC spec.
type OidcSpec struct {
	// OIDC client credentials secret. It must contain the following keys:
	//   - `CLIENT_ID`: The client ID of the OIDC client.
	//   - `CLIENT_SECRET`: The client secret of the OIDC client.
	// credentials will omit to pod environment variables.
	// +kubebuilder:validation:Required
	ClientCredentialsSecret string `json:"clientCredentialsSecret"`

	// +kubebuilder:validation:Optional
	ExtraScopes []string `json:"extraScopes,omitempty"`
}

type TlsSpec struct {
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="tls"
	SecretClass string `json:"secretClass,omitempty"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="changeit"
	JksPassword string `json:"jksPassword,omitempty"`
}

type KerberosSpec struct {
	// +kubebuilder:validation:Optional
	SecretClass string `json:"secretClass,omitempty"`
}

// ==================== ClusterResource Implementation ====================
// HdfsCluster implements common.ClusterResource so the SDK GenericReconciler can drive it.
// client.Object is satisfied by the embedded metadata and generated runtime methods; DeepCopy is
// generated by controller-gen. The only product-written bridge is GetSpec/GetStatus below.

// GetSpec bridges the type-safe role fields (NameNodes/DataNodes/JournalNodes) to the SDK's
// generic Roles map, keyed by the canonical role names. It does not expose ClusterConfig,
// which stays product-specific and is read directly from the typed CR.
func (c *HdfsCluster) GetSpec() *commonsv1alpha1.GenericClusterSpec {
	roles := make(map[string]commonsv1alpha1.RoleSpec)
	if c.Spec.NameNodes != nil {
		roles[NameNodeRoleName] = c.Spec.NameNodes.toGeneric()
	}
	if c.Spec.DataNodes != nil {
		roles[DataNodeRoleName] = c.Spec.DataNodes.toGeneric()
	}
	if c.Spec.JournalNodes != nil {
		roles[JournalNodeRoleName] = c.Spec.JournalNodes.toGeneric()
	}
	// Storage is no longer defaulted here: each role's RoleDeclaration.DataVolume opts it into a
	// data PVC, and the framework builds the VolumeClaimTemplate from the effective
	// config.resources.storage (defaulting the capacity to the commons DefaultStorageCapacity).
	return &commonsv1alpha1.GenericClusterSpec{
		Image:            c.Spec.Image,
		ClusterOperation: c.Spec.ClusterOperation,
		Roles:            roles,
	}
}

// Role returns the typed HDFS role for the framework's canonical role name.
func (c *HdfsCluster) Role(roleName string) *RoleSpec {
	switch roleName {
	case NameNodeRoleName:
		if c.Spec.NameNodes != nil {
			return &c.Spec.NameNodes.RoleSpec
		}
	case DataNodeRoleName:
		if c.Spec.DataNodes != nil {
			return &c.Spec.DataNodes.RoleSpec
		}
	case JournalNodeRoleName:
		if c.Spec.JournalNodes != nil {
			return &c.Spec.JournalNodes.RoleSpec
		}
	}
	return nil
}

// toGeneric removes the HDFS-owned portion of ConfigSpec while preserving every framework field.
func (r *RoleSpec) toGeneric() commonsv1alpha1.RoleSpec {
	out := commonsv1alpha1.RoleSpec{RoleConfig: r.RoleConfig}
	if r.Config != nil {
		out.Config = r.Config.RoleGroupConfigSpec
	}
	out.ConfigOverrides = r.ConfigOverrides
	out.EnvOverrides = r.EnvOverrides
	out.CliOverrides = r.CliOverrides
	out.PodOverrides = r.PodOverrides
	out.RoleGroups = make(map[string]commonsv1alpha1.RoleGroupSpec, len(r.RoleGroups))
	for name, group := range r.RoleGroups {
		out.RoleGroups[name] = group.toGeneric()
	}
	return out
}

func (r *RoleGroupSpec) toGeneric() commonsv1alpha1.RoleGroupSpec {
	out := commonsv1alpha1.RoleGroupSpec{Replicas: r.Replicas}
	if r.Config != nil {
		out.Config = r.Config.RoleGroupConfigSpec
	}
	out.ConfigOverrides = r.ConfigOverrides
	out.EnvOverrides = r.EnvOverrides
	out.CliOverrides = r.CliOverrides
	out.PodOverrides = r.PodOverrides
	return out
}

// GetStatus returns the generic cluster status.
func (c *HdfsCluster) GetStatus() *commonsv1alpha1.GenericClusterStatus {
	return &c.Status.GenericClusterStatus
}

// VectorAggregatorConfigMapName implements the SDK VectorAggregatorProvider: it exposes the
// user's Vector aggregator discovery ConfigMap so the framework wires the Vector log sidecar when
// a role group enables the agent. Empty when unset (Vector disabled).
func (c *HdfsCluster) VectorAggregatorConfigMapName() string {
	if c.Spec.ClusterConfig == nil {
		return ""
	}
	return c.Spec.ClusterConfig.VectorAggregatorConfigMapName
}

// Ensure HdfsCluster satisfies the complete v0.13 GenericReconciler constraint, including the
// concrete generated DeepCopy() *HdfsCluster method.
var _ common.ClusterResource[*HdfsCluster] = &HdfsCluster{}

func init() {
	SchemeBuilder.Register(&HdfsCluster{}, &HdfsClusterList{})
}
