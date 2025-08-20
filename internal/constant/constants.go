package constant

import "github.com/zncdatadev/operator-go/pkg/constants"

// Role types for HDFS components
type Role string

const (
	NameNode    Role = "namenode"
	DataNode    Role = "datanode"
	JournalNode Role = "journalnode"
)

// RoleType is an alias for Role to maintain backward compatibility
type RoleType = Role

// ContainerComponent use for define container name
type ContainerComponent string

// Container component names
const (
	NameNodeContainer         = "namenode"
	DataNodeContainer         = "datanode"
	JournalNodeContainer      = "journalnode"
	ZkfcContainer             = "zkfc"
	FormatNameNodeContainer   = "format-namenodes"
	FormatZookeeperContainer  = "format-zookeeper"
	WaitForNameNodesContainer = "wait-for-namenodes"
	OidcContainer             = "oidc"
	// const ContainerVector ContainerComponent = "vector"
	VectorContainer = "vector"
)

// Container components (for ContainerComponent type)
const (
	NameNodeComponent         ContainerComponent = ContainerComponent(NameNodeContainer)
	DataNodeComponent         ContainerComponent = ContainerComponent(DataNodeContainer)
	JournalNodeComponent      ContainerComponent = ContainerComponent(JournalNodeContainer)
	ZkfcComponent             ContainerComponent = ContainerComponent(ZkfcContainer)
	FormatNameNodeComponent   ContainerComponent = ContainerComponent(FormatNameNodeContainer)
	FormatZookeeperComponent  ContainerComponent = ContainerComponent(FormatZookeeperContainer)
	WaitForNameNodesComponent ContainerComponent = ContainerComponent(WaitForNameNodesContainer)
	OidcComponent             ContainerComponent = ContainerComponent(OidcContainer)
)

// NameNode service ports
const (
	NameNodeServiceHttpPort   = 9870
	NameNodeServiceHttpsPort  = 9871
	NameNodeServiceRpcPort    = 8020
	NameNodeServiceMetricPort = 8183
)

// DataNode service ports
const (
	DataNodeServiceHttpPort   = 9864
	DataNodeServiceHttpsPort  = 9865
	DataNodeServiceDataPort   = 9866
	DataNodeServiceIpcPort    = 9867
	DataNodeServiceMetricPort = 8082
)

// JournalNode service ports
const (
	JournalNodeServiceHttpPort   = 8480
	JournalNodeServiceHttpsPort  = 8481
	JournalNodeServiceRpcPort    = 8485
	JournalNodeServiceMetricPort = 8081
)

// Volume mount names
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
)

// Directory paths
const (
	KubedoopConfigDirMount = constants.KubedoopConfigDir
	KubedoopLogDirMount    = constants.KubedoopLogDir
	KubedoopListenerDir    = constants.KubedoopListenerDir
	KubedoopDataDir        = constants.KubedoopDataDir
	KubedoopRoot           = constants.KubedoopRoot
)

// Port names
const (
	MetricPortName = "metric"
	HttpPortName   = "http"
	HttpsPortName  = "https"
	RpcPortName    = "rpc"
	IpcPortName    = "ipc"
	DataPortName   = "data"
)

// Zookeeper

const ZookeeperHdfsDiscoveryKey = "ZOOKEEPER"
