package container

import "github.com/zncdatadev/hdfs-operator/internal/common"

func GetRole() common.Role {
	return common.DataNode
}

const (
	DataNode     common.ContainerComponent = common.ContainerComponent(common.DataNode)
	WaitNameNode common.ContainerComponent = "wait-for-namenodes"
)
