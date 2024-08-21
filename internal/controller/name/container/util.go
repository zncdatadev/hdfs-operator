package container

import "github.com/zncdatadev/hdfs-operator/internal/common"

// Component ContainerComponent name node container component
// contains: zkfc, namenode, format-namenode, format-zookeeper

func GetRole() common.Role {
	return common.NameNode
}

const (
	Zkfc            common.ContainerComponent = "zkfc"
	NameNode        common.ContainerComponent = common.ContainerComponent(common.NameNode)
	FormatNameNode  common.ContainerComponent = "format-namenodes"
	FormatZookeeper common.ContainerComponent = "format-zookeeper"
)
