package common

import (
	"reflect"
	"time"

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

func newDefaultResourceSpec(role Role) *commonsv1alpha1.ResourcesSpec {
	return GetContainerResource(role, string(role))
}

// DefaultNodeConfig default node config
func DefaultNodeConfig(clusterName string, role Role, listenerClass constants.ListenerClass, gracefulShutdownTimeout time.Duration) *RoleNodeConfig {
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

func DefaultNameNodeConfig(clusterName string) *RoleNodeConfig {
	return DefaultNodeConfig(clusterName, NameNode, constants.ClusterInternal, 15*time.Minute)
}

func DefaultDataNodeConfig(clusterName string) *RoleNodeConfig {
	return DefaultNodeConfig(clusterName, DataNode, constants.ClusterInternal, 30*time.Minute)
}

func DefaultJournalNodeConfig(clusterName string) *RoleNodeConfig {
	return DefaultNodeConfig(clusterName, JournalNode, "", 15*time.Minute)
}

func (n *RoleNodeConfig) isConfigValid(configValue reflect.Value) bool {
	return configValue.Kind() == reflect.Ptr
}

func (n *RoleNodeConfig) getConfigValue(mergedCfg any) (reflect.Value, bool) {
	configValue := reflect.ValueOf(mergedCfg)
	if !n.isConfigValid(configValue) {
		return reflect.Value{}, false
	}

	configValue = configValue.Elem()
	config := configValue.FieldByName("Config")
	if !config.IsValid() || !config.CanSet() {
		return reflect.Value{}, false
	}

	return config.Elem(), true
}

func (n *RoleNodeConfig) mergeResourceFields(mergedResource *commonsv1alpha1.ResourcesSpec) *commonsv1alpha1.ResourcesSpec {
	if mergedResource == nil {
		return n.resources
	}

	result := &commonsv1alpha1.ResourcesSpec{
		CPU:     mergedResource.CPU,
		Memory:  mergedResource.Memory,
		Storage: mergedResource.Storage,
	}

	if result.CPU == nil {
		result.CPU = n.resources.CPU
	}
	if result.Memory == nil {
		result.Memory = n.resources.Memory
	}
	if result.Storage == nil {
		result.Storage = n.resources.Storage
	}

	return result
}

func (n *RoleNodeConfig) handleResourcesField(config reflect.Value) {
	resourcesField := config.FieldByName("Resources")
	if !resourcesField.IsValid() || !resourcesField.CanSet() {
		return
	}

	if resourcesField.Type().Kind() != reflect.Ptr ||
		resourcesField.Type().Elem() != reflect.TypeOf(commonsv1alpha1.ResourcesSpec{}) {
		return
	}

	var mergedResource *commonsv1alpha1.ResourcesSpec
	if !resourcesField.IsZero() {
		mergedResource = resourcesField.Interface().(*commonsv1alpha1.ResourcesSpec)
	}

	resourceRes := n.mergeResourceFields(mergedResource)
	resourcesField.Set(reflect.ValueOf(resourceRes))
}

func (n *RoleNodeConfig) handleListenerClass(config reflect.Value) {
	listenerClassField := config.FieldByName("ListenerClass")
	if listenerClassField.IsValid() && listenerClassField.IsZero() && listenerClassField.CanSet() {
		listenerClassField.Set(reflect.ValueOf(n.listenerClass))
	}
}

func (n *RoleNodeConfig) handleAffinity(config reflect.Value) {
	affinityField := config.FieldByName("Affinity")
	if affinityField.IsValid() && affinityField.IsZero() && affinityField.CanSet() {
		affinityField.Set(reflect.ValueOf(n.common.Affinity.Build()))
	}
}

func (n *RoleNodeConfig) MergeDefaultConfig(mergedCfg any) {
	config, ok := n.getConfigValue(mergedCfg)
	if !ok {
		return
	}

	n.handleResourcesField(config)
	n.handleListenerClass(config)
	n.handleAffinity(config)
}

func parseQuantity(q string) resource.Quantity {
	r := resource.MustParse(q)
	return r
}

func GetContainerResource(role Role, containerName string) *commonsv1alpha1.ResourcesSpec {
	var cpuMin, cpuMax, memoryLimit, storage resource.Quantity
	switch role {
	case NameNode:
		switch containerName {
		case "format-namenodes":
			cpuMin = parseQuantity("100m")
			cpuMax = parseQuantity("200m")
			memoryLimit = parseQuantity("512Mi")
		case "format-zookeeper":
			cpuMin = parseQuantity("100m")
			cpuMax = parseQuantity("200m")
			memoryLimit = parseQuantity("512Mi")
		case "zkfc":
			cpuMin = parseQuantity("100m")
			cpuMax = parseQuantity("200m")
			memoryLimit = parseQuantity("512Mi")
		case "namenode":
			cpuMin = parseQuantity("300m")
			cpuMax = parseQuantity("600m")
			memoryLimit = parseQuantity("1024Mi")
			storage = parseQuantity("1Gi")
		default:
			panic("invalid container name in NameNode role:" + containerName)
		}
	case DataNode:
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
	case JournalNode:
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
