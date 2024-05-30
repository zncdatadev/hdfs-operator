package container

import "github.com/zncdatadev/hdfs-operator/internal/common"

func DataNodeConfVolumeName() string {
	return "datanode-config"
}

func DataNodeLogVolumeName() string {
	return "datanode-log-config"
}

func WaitNameNodeConfigVolumeName() string {
	return "wait-for-namenodes-config"
}

func WaitNameNodeLogVolumeName() string {
	return "wait-for-namenodes-log-config"
}
func DataVolumeName() string {
	return "data"
}

func ListenerVolumeName() string {
	return "listener"
}

func LogVolumeName() string {
	return "log"
}

const (
	DataNode     common.ContainerComponent = "datanode"
	WaitNameNode common.ContainerComponent = "wait-for-namenodes"
)
