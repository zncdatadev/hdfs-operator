package data

import "github.com/zncdata-labs/hdfs-operator/internal/common"

func createConfigName(instanceName string, groupName string) string {
	return common.NewResourceNameGenerator(instanceName, string(common.DataNode), groupName).GenerateResourceName("")
}

func createStatefulSetName(instanceName string, groupName string) string {
	return common.NewResourceNameGenerator(instanceName, string(common.DataNode), groupName).GenerateResourceName("")
}

func createServiceName(instanceName string, groupName string) string {
	return common.NewResourceNameGenerator(instanceName, string(common.DataNode), groupName).GenerateResourceName("")
}

const (
	ServiceHttpPort   = 9870
	ServiceRpcPort    = 8020
	ServiceMetricPort = 8183
)

const (
	ContainerDataNode     common.ContainerComponent = "datanode"
	ContainerWaitNameNode common.ContainerComponent = "wait-for-namenodes"
)
