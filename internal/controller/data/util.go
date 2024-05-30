package data

import (
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/util"
)

func createConfigName(instanceName string, groupName string) string {
	return util.NewResourceNameGenerator(instanceName, string(common.DataNode), groupName).GenerateResourceName("")
}

func createStatefulSetName(instanceName string, groupName string) string {
	return util.NewResourceNameGenerator(instanceName, string(common.DataNode), groupName).GenerateResourceName("")
}

func createServiceName(instanceName string, groupName string) string {
	return util.NewResourceNameGenerator(instanceName, string(common.DataNode), groupName).GenerateResourceName("")
}

const (
	ServiceHttpPort   = 9864
	ServiceDataPort   = 9866
	ServiceIpcPort    = 9867
	ServiceMetricPort = 8082
)
