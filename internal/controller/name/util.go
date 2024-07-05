package name

import (
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/util"
)

func createConfigName(instanceName string, groupName string) string {
	return util.NewResourceNameGenerator(instanceName, string(common.NameNode), groupName).GenerateResourceName("")
}

func createServiceName(instanceName string, groupName string) string {
	return util.NewResourceNameGenerator(instanceName, string(common.NameNode), groupName).GenerateResourceName("")
}

const (
	ServiceHttpPort   = 9870
	ServiceHttpsPort  = 9871
	ServiceRpcPort    = 8020
	ServiceMetricPort = 8183
)
