package container

import "github.com/zncdatadev/hdfs-operator/internal/common"

// Component ContainerComponent name node container component
// contains: zkfc, namenode, format-namenode, format-zookeeper

func LogVolumeName() string {
	return "log"
}

func NameNodeConfVolumeName() string {
	return "namenode-config"
}

func NameNodeLogVolumeName() string {
	return "namenode-log-config"
}

func ZkfcVolumeName() string {
	return "zkfc-config"
}

func ZkfcLogVolumeName() string {
	return "zkfc-log-config"
}

func FormatNameNodeVolumeName() string {
	return "format-namenode-config"
}

func FormatNameNodeLogVolumeName() string {
	return "format-namenode-log-config"
}

func FormatZookeeperVolumeName() string {
	return "format-zookeeper-config"
}

func FormatZookeeperLogVolumeName() string {
	return "format-zookeeper-log-config"
}

func DataVolumeName() string {
	return "data"
}

func ListenerVolumeName() string {
	return "listener"
}

func GetRole() common.Role {
	return common.NameNode
}

const (
	Zkfc            common.ContainerComponent = "zkfc"
	NameNode        common.ContainerComponent = common.ContainerComponent(common.NameNode)
	FormatNameNode  common.ContainerComponent = "format-namenodes"
	FormatZookeeper common.ContainerComponent = "format-zookeeper"
)
