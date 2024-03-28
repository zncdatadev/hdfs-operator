package common

// Listener

const ListenerStorageClass = "listeners.zncdata.labs"
const ListenerAnnotationKey = ListenerStorageClass + "/listener-class"

type ListenerClass string

const (
	// ClusterIp is the default listener class for internal communication
	ClusterIp ListenerClass = "cluster-internal"
	// NodePort is for external communication
	NodePort          ListenerClass = "external-unstable"
	LoadBalancerClass ListenerClass = "external-stable"
)

// Zookeeper

const ZookeeperHdfsDiscoveryKey = "ZOOKEEPER"
