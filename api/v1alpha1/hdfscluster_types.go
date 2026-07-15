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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/common"
	"github.com/zncdatadev/operator-go/pkg/constant"
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
	// Image specifies the HDFS container image configuration.
	// If not set, the webhook defaulter should provide product defaults.
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

// NameNodeSpec embeds the SDK generic RoleSpec and can carry NameNode-specific fields.
type NameNodeSpec struct {
	commonsv1alpha1.RoleSpec `json:",inline"`
}

// DataNodeSpec embeds the SDK generic RoleSpec and can carry DataNode-specific fields.
type DataNodeSpec struct {
	commonsv1alpha1.RoleSpec `json:",inline"`
}

// JournalNodeSpec embeds the SDK generic RoleSpec and can carry JournalNode-specific fields.
type JournalNodeSpec struct {
	commonsv1alpha1.RoleSpec `json:",inline"`
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

// ==================== ClusterInterface Implementation ====================
// HdfsCluster implements common.ClusterInterface so the SDK GenericReconciler can drive it.
// GetName/GetNamespace/GetLabels/GetAnnotations are inherited from the embedded ObjectMeta.

// GetSpec bridges the type-safe role fields (NameNodes/DataNodes/JournalNodes) to the SDK's
// generic Roles map, keyed by the canonical role names. It does not expose ClusterConfig,
// which stays product-specific and is read directly from the typed CR.
func (c *HdfsCluster) GetSpec() *commonsv1alpha1.GenericClusterSpec {
	roles := make(map[string]commonsv1alpha1.RoleSpec)
	if c.Spec.NameNodes != nil {
		roles[NameNodeRoleName] = c.Spec.NameNodes.RoleSpec
	}
	if c.Spec.DataNodes != nil {
		roles[DataNodeRoleName] = c.Spec.DataNodes.RoleSpec
	}
	if c.Spec.JournalNodes != nil {
		roles[JournalNodeRoleName] = c.Spec.JournalNodes.RoleSpec
	}
	return &commonsv1alpha1.GenericClusterSpec{
		Image:            c.Spec.Image,
		ClusterOperation: c.Spec.ClusterOperation,
		Roles:            roles,
	}
}

// GetStatus returns the generic cluster status.
func (c *HdfsCluster) GetStatus() *commonsv1alpha1.GenericClusterStatus {
	return &c.Status.GenericClusterStatus
}

// SetStatus sets the generic cluster status.
func (c *HdfsCluster) SetStatus(status *commonsv1alpha1.GenericClusterStatus) {
	c.Status.GenericClusterStatus = *status
}

// DeepCopyCluster returns a deep copy as a ClusterInterface.
func (c *HdfsCluster) DeepCopyCluster() common.ClusterInterface {
	return c.DeepCopy()
}

// GetRuntimeObject returns the underlying runtime.Object.
func (c *HdfsCluster) GetRuntimeObject() runtime.Object {
	return c
}

// GetObjectMeta returns the object metadata.
func (c *HdfsCluster) GetObjectMeta() *metav1.ObjectMeta {
	return &c.ObjectMeta
}

// GetScheme returns the runtime scheme (set by the manager).
func (c *HdfsCluster) GetScheme() *runtime.Scheme {
	return nil
}

// GetUID returns the cluster UID.
func (c *HdfsCluster) GetUID() types.UID {
	return c.UID
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

// Ensure HdfsCluster satisfies the SDK ClusterInterface.
var _ common.ClusterInterface = &HdfsCluster{}

func init() {
	SchemeBuilder.Register(&HdfsCluster{}, &HdfsClusterList{})
}
