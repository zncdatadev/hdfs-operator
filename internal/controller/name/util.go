package name

import "github.com/zncdata-labs/hdfs-operator/internal/common"

func createConfigName(instanceName string, groupName string) string {
	return common.NewResourceNameGenerator(instanceName, string(common.NameNode), groupName).GenerateResourceName("")
}

func createStatefulsetName(instanceName string, groupName string) string {
	return common.NewResourceNameGenerator(instanceName, string(common.NameNode), groupName).GenerateResourceName("")
}
