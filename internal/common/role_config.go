package common

import (
	"time"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/constants"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	ListenerPvcStorage = "10Mi"
)

type RoleNodeConfig struct {
	resources *commonsv1alpha1.ResourcesSpec
	// logging config todo
	listenerClass constants.ListenerClass
	common        *GeneralNodeConfig
}

type GeneralNodeConfig struct {
	Affinity *AffinityBuilder

	gracefulShutdownTimeout time.Duration
}

func newDefaultResourceSpec(role constant.Role) *commonsv1alpha1.ResourcesSpec {
	return GetContainerResource(role, constant.ContainerComponent(role)) // todo: fix container name
}

// DefaultNodeConfig default node config
func DefaultNodeConfig(clusterName string, role constant.Role, listenerClass constants.ListenerClass, gracefulShutdownTimeout time.Duration) *RoleNodeConfig {
	return &RoleNodeConfig{
		resources:     newDefaultResourceSpec(role),
		listenerClass: listenerClass,
		common: &GeneralNodeConfig{
			Affinity: &AffinityBuilder{
				[]PodAffinity{
					*NewPodAffinity(map[string]string{LabelCrName: clusterName}, false, false).Weight(20),
					*NewPodAffinity(map[string]string{LabelCrName: clusterName, LabelComponent: string(role)}, false, true).Weight(70),
				},
			},
			gracefulShutdownTimeout: gracefulShutdownTimeout,
		},
	}
}

func DefaultRoleConfig(clusterName string, role constant.Role) *RoleNodeConfig {
	switch role {
	case constant.NameNode:
		return DefaultNameNodeConfig(clusterName)
	case constant.DataNode:
		return DefaultDataNodeConfig(clusterName)
	case constant.JournalNode:
		return DefaultJournalNodeConfig(clusterName)
	default:
		panic("unsupported role: " + string(role))
	}
}

func DefaultNameNodeConfig(clusterName string) *RoleNodeConfig {
	return DefaultNodeConfig(clusterName, constant.NameNode, constants.ClusterInternal, 15*time.Minute)
}

func DefaultDataNodeConfig(clusterName string) *RoleNodeConfig {
	return DefaultNodeConfig(clusterName, constant.DataNode, constants.ClusterInternal, 30*time.Minute)
}

func DefaultJournalNodeConfig(clusterName string) *RoleNodeConfig {
	return DefaultNodeConfig(clusterName, constant.JournalNode, "", 15*time.Minute)
}

// MergeDefaultConfig merges default configuration with the provided config
func (n *RoleNodeConfig) MergeDefaultConfig(mergedCfg *hdfsv1alpha1.ConfigSpec) {
	if mergedCfg == nil {
		return
	}

	// Ensure RoleGroupConfigSpec is initialized
	if mergedCfg.RoleGroupConfigSpec == nil {
		mergedCfg.RoleGroupConfigSpec = &commonsv1alpha1.RoleGroupConfigSpec{}
	}

	// Merge Resources configuration
	var resourceRes *commonsv1alpha1.ResourcesSpec
	if mergedCfg.Resources == nil {
		resourceRes = n.resources
	} else {
		mergedResource := mergedCfg.Resources
		resourceRes = mergedResource
		if mergedResource.CPU == nil {
			resourceRes.CPU = n.resources.CPU
		}
		if mergedResource.Memory == nil {
			resourceRes.Memory = n.resources.Memory
		}
		if mergedResource.Storage == nil {
			resourceRes.Storage = n.resources.Storage
		}
	}
	mergedCfg.Resources = resourceRes

	// Merge ListenerClass configuration
	if mergedCfg.ListenerClass == nil && n.listenerClass != "" {
		listenerClass := string(n.listenerClass)
		mergedCfg.ListenerClass = &listenerClass
	}

	// Merge Affinity configuration
	// Note: Need to check the exact type of Affinity field in RoleGroupConfigSpec
	// For now, commenting out until the type is confirmed
	/*
		if mergedCfg.RoleGroupConfigSpec.Affinity == nil {
			mergedCfg.RoleGroupConfigSpec.Affinity = n.common.Affinity.Build()
		}
	*/

	// You can continue to add logic to handle other fields
	// e.g., Logging, GracefulShutdownTimeout, etc.
}

func parseQuantity(q string) resource.Quantity {
	r := resource.MustParse(q)
	return r
}

func GetContainerResource(role constant.Role, containerName constant.ContainerComponent) *commonsv1alpha1.ResourcesSpec {
	var cpuMin, cpuMax, memoryLimit, storage resource.Quantity
	switch role {
	case constant.NameNode:
		switch containerName {
		case constant.FormatNameNodeComponent:
			cpuMin = parseQuantity("100m")
			cpuMax = parseQuantity("200m")
			memoryLimit = parseQuantity("512Mi")
		case constant.FormatZookeeperComponent:
			cpuMin = parseQuantity("100m")
			cpuMax = parseQuantity("200m")
			memoryLimit = parseQuantity("512Mi")
		case constant.ZkfcComponent:
			cpuMin = parseQuantity("100m")
			cpuMax = parseQuantity("200m")
			memoryLimit = parseQuantity("512Mi")
		case constant.NameNodeComponent:
			cpuMin = parseQuantity("300m")
			cpuMax = parseQuantity("600m")
			memoryLimit = parseQuantity("1024Mi")
			storage = parseQuantity("1Gi")
		default:
			panic("invalid container name in NameNode role:" + containerName)
		}
	case constant.DataNode:
		switch containerName {
		case "datanode":
			cpuMin = parseQuantity("100m")
			cpuMax = parseQuantity("300m")
			memoryLimit = parseQuantity("512Mi")
			storage = parseQuantity("2Gi")
		case "wait-for-namenodes":
			cpuMin = parseQuantity("100m")
			cpuMax = parseQuantity("200m")
			memoryLimit = parseQuantity("512Mi")
		default:
			panic("invalid container name in DataNode role" + containerName)
		}
	case constant.JournalNode:
		cpuMin = parseQuantity("100m")
		cpuMax = parseQuantity("300m")
		memoryLimit = parseQuantity("512Mi")
		storage = parseQuantity("1Gi")
	default:
		panic("unsupported role: " + role)
	}
	return &commonsv1alpha1.ResourcesSpec{
		CPU: &commonsv1alpha1.CPUResource{
			Min: cpuMin,
			Max: cpuMax,
		},
		Memory: &commonsv1alpha1.MemoryResource{
			Limit: memoryLimit,
		},
		Storage: &commonsv1alpha1.StorageResource{
			Capacity: storage,
		},
	}
}
