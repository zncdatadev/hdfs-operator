/*
Copyright 2024.

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
	"github.com/zncdatadev/operator-go/pkg/constants"
	"github.com/zncdatadev/operator-go/pkg/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	NameNodeRootDataDir    = constants.KubedoopDataDir + "/namenode"
	JournalNodeRootDataDir = constants.KubedoopDataDir + "/journalnode"

	DataNodeRootDataDirPrefix = constants.KubedoopDataDir + "/"
	DataNodeRootDataDirSuffix = "/datanode"

	HadoopHome = constants.KubedoopRoot + "/hadoop"
)

// port names
const (
	MetricName = "metric"
	HttpName   = "http"
	HttpsName  = "https"
	RpcName    = "rpc"
	IpcName    = "ipc"
	DataName   = "data"

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

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// HdfsCluster is the Schema for the hdfsclusters API
type HdfsCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HdfsClusterSpec `json:"spec,omitempty"`
	Status status.Status   `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// HdfsClusterList contains a list of HdfsCluster
type HdfsClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HdfsCluster `json:"items"`
}

// HdfsClusterSpec defines the desired state of HdfsCluster
type HdfsClusterSpec struct {
	// +kubebuilder:validation:Optional
	Image *ImageSpec `json:"image,omitempty"`

	// +kubebuilder:validation:Required
	ClusterConfigSpec *ClusterConfigSpec `json:"clusterConfig,omitempty"`

	// roles defined: nameNode, dataNode, journalNode
	// +kubebuilder:validation:Required
	NameNode *NameNodeSpec `json:"nameNode,omitempty"`

	// +kubebuilder:validation:Required
	DataNode *DataNodeSpec `json:"dataNode,omitempty"`

	// +kubebuilder:validation:Required
	JournalNode *JournalNodeSpec `json:"journalNode,omitempty"`
}

type NameNodeSpec struct {
	// +kubebuilder:validation:Optional
	Config *NameNodeConfigSpec `json:"config,omitempty"`

	// +kubebuilder:validation:Optional
	RoleGroups map[string]*NameNodeRoleGroupSpec `json:"roleGroups,omitempty"`

	// +kubebuilder:validation:Optional
	PodDisruptionBudget *PodDisruptionBudgetSpec `json:"podDisruptionBudget,omitempty"`

	// +kubebuilder:validation:Optional
	CliOverrides []string `json:"cliOverrides,omitempty"`

	// +kubebuilder:validation:Optional
	ConfigOverrides *ConfigOverridesSpec `json:"configOverrides,omitempty"`

	// +kubebuilder:validation:Optional
	EnvOverrides map[string]string `json:"envOverrides,omitempty"`
}

type DataNodeSpec struct {
	// +kubebuilder:validation:Optional
	Config *DataNodeConfigSpec `json:"config,omitempty"`

	// +kubebuilder:validation:Optional
	RoleGroups map[string]*DataNodeRoleGroupSpec `json:"roleGroups,omitempty"`

	// +kubebuilder:validation:Optional
	PodDisruptionBudget *PodDisruptionBudgetSpec `json:"podDisruptionBudget,omitempty"`

	// +kubebuilder:validation:Optional
	CliOverrides []string `json:"cliOverrides,omitempty"`

	// +kubebuilder:validation:Optional
	ConfigOverrides *ConfigOverridesSpec `json:"configOverrides,omitempty"`

	// +kubebuilder:validation:Optional
	EnvOverrides map[string]string `json:"envOverrides,omitempty"`
}

type JournalNodeSpec struct {
	// +kubebuilder:validation:Optional
	Config *JournalNodeConfigSpec `json:"config,omitempty"`

	// +kubebuilder:validation:Optional
	RoleGroups map[string]*JournalNodeRoleGroupSpec `json:"roleGroups,omitempty"`

	// +kubebuilder:validation:Optional
	PodDisruptionBudget *PodDisruptionBudgetSpec `json:"podDisruptionBudget,omitempty"`

	// +kubebuilder:validation:Optional
	CliOverrides []string `json:"cliOverrides,omitempty"`

	// +kubebuilder:validation:Optional
	ConfigOverrides *ConfigOverridesSpec `json:"configOverrides,omitempty"`

	// +kubebuilder:validation:Optional
	EnvOverrides map[string]string `json:"envOverrides,omitempty"`
}

type ClusterConfigSpec struct {
	// +kubebuilder:validation:Optional
	VectorAggregatorConfigMapName string `json:"vectorAggregatorConfigMapName,omitempty"`

	// +kubebuilder:validation:Optional
	Service *ServiceSpec `json:"service,omitempty"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="cluster.local"
	ClusterName string `json:"clusterName,omitempty"`

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

type ConfigOverridesSpec struct {
	CoreSite map[string]string `json:"core-site.xml,omitempty"`
	HdfsSite map[string]string `json:"hdfs-site.xml,omitempty"`
	// only for nameNode
	Log4j        map[string]string `json:"log4j.properties,omitempty"`
	Security     map[string]string `json:"security.properties,omitempty"`
	HadoopPolicy map[string]string `json:"hadoop-policy.xml,omitempty"`
	SslServer    map[string]string `json:"ssl-server.xml,omitempty"`
	SslClient    map[string]string `json:"ssl-client.xml,omitempty"`
}

type PodDisruptionBudgetSpec struct {
	// +kubebuilder:validation:Optional
	MinAvailable int32 `json:"minAvailable,omitempty"`

	// +kubebuilder:validation:Optional
	MaxUnavailable int32 `json:"maxUnavailable,omitempty"`
}

type ServiceSpec struct {
	// +kubebuilder:validation:Optional
	Annotations map[string]string `json:"annotations,omitempty"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:enum=ClusterIP;NodePort;LoadBalancer;ExternalName
	// +kubebuilder:default=ClusterIP
	Type corev1.ServiceType `json:"type,omitempty"`

	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=18080
	Port int32 `json:"port,omitempty"`
}

func init() {
	SchemeBuilder.Register(&HdfsCluster{}, &HdfsClusterList{})
}
