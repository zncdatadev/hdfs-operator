package constant

import "github.com/zncdatadev/operator-go/pkg/constants"

// Container component names
const (
	NameNodeContainer         = "namenode"
	DataNodeContainer         = "datanode"
	JournalNodeContainer      = "journalnode"
	ZkfcContainer             = "zkfc"
	FormatNameNodeContainer   = "format-namenode"
	FormatZookeeperContainer  = "format-zookeeper"
	WaitForNameNodesContainer = "wait-for-namenodes"
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

// Container resource keys
const (
	NameNodeResourceKey        = "namenode"
	DataNodeResourceKey        = "datanode"
	JournalNodeResourceKey     = "journalnode"
	ZkfcResourceKey            = "zkfc"
	FormatNameNodeResourceKey  = "format-namenode"
	FormatZookeeperResourceKey = "format-zookeeper"
)
