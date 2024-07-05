package journal

import (
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/util"
)

func createConfigName(instanceName string, groupName string) string {
	return util.NewResourceNameGenerator(instanceName, string(common.JournalNode), groupName).GenerateResourceName("")
}

func createStatefulSetName(instanceName string, groupName string) string {
	return util.NewResourceNameGenerator(instanceName, string(common.JournalNode), groupName).GenerateResourceName("")
}

func createServiceName(instanceName string, groupName string) string {
	return util.NewResourceNameGenerator(instanceName, string(common.JournalNode), groupName).GenerateResourceName("")
}

func logVolumeName() string {
	return "log"
}

func journalNodeConfigVolumeName() string {
	return "journalnode-config"
}

func journalNodeLogVolumeName() string {
	return "journalnode-log-config"
}

func dataVolumeName() string {
	return "data"
}

func GetRole() common.Role {
	return common.JournalNode
}

const (
	ServiceHttpPort   = 8480
	ServiceHttpsPort  = 8481
	ServiceRpcPort    = 8485
	ServiceMetricPort = 8081
)

const ContainerJournalNode common.ContainerComponent = common.ContainerComponent(common.JournalNode)
