package name

import "github.com/zncdata-labs/hdfs-operator/internal/common"

func createConfigName(instanceName string, groupName string) string {
	return common.NewResourceNameGenerator(instanceName, string(common.NameNode), groupName).GenerateResourceName("")
}

func createStatefulSetName(instanceName string, groupName string) string {
	return common.NewResourceNameGenerator(instanceName, string(common.NameNode), groupName).GenerateResourceName("")
}

func createServiceName(instanceName string, groupName string) string {
	return common.NewResourceNameGenerator(instanceName, string(common.NameNode), groupName).GenerateResourceName("")
}

const (
	ServiceHttpPort   = 9870
	ServiceRpcPort    = 8020
	ServiceMetricPort = 8183
)
