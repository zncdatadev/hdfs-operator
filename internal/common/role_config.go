package common

import (
	"reflect"
	"time"

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
	return GetContainerResource(role, "todo")
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

func DefaultNameNodeConfig(clusterName string) *RoleNodeConfig {
	return DefaultNodeConfig(clusterName, constant.NameNode, constants.ClusterInternal, 15*time.Minute)
}

func DefaultDataNodeConfig(clusterName string) *RoleNodeConfig {
	return DefaultNodeConfig(clusterName, constant.DataNode, constants.ClusterInternal, 30*time.Minute)
}

func DefaultJournalNodeConfig(clusterName string) *RoleNodeConfig {
	return DefaultNodeConfig(clusterName, constant.JournalNode, "", 15*time.Minute)
}

// todo: refactor this, do this using detail type
func (n *RoleNodeConfig) MergeDefaultConfig(mergedCfg any) {

	// Make sure mergedCfg is a pointer type
	configValue := reflect.ValueOf(mergedCfg)
	if configValue.Kind() != reflect.Ptr {
		return
	}

	// Get the value that the pointer points to
	configValue = configValue.Elem()

	// Get the Config field
	config := configValue.FieldByName("Config")
	if !config.IsValid() || !config.CanSet() {
		return
	}
	config = config.Elem()

	// Get the Resources field
	resourcesField := config.FieldByName("Resources")
	var resourceRes *commonsv1alpha1.ResourcesSpec
	if resourcesField.IsValid() && resourcesField.CanSet() {
		if resourcesField.IsZero() {
			resourceRes = n.resources
		} else {
			// adjust resourcesField is commonsv1alpha1.ResourcesSpec
			if resourcesField.Type().Kind() == reflect.Ptr && resourcesField.Type().Elem() == reflect.TypeOf(commonsv1alpha1.ResourcesSpec{}) {
				// transform resourcesField to *commonsv1alpha1.ResourcesSpec
				if resourcesField.Kind() == reflect.Ptr && resourcesField.Type().Elem() == reflect.TypeOf(commonsv1alpha1.ResourcesSpec{}) {
					mergedResource := resourcesField.Interface().(*commonsv1alpha1.ResourcesSpec)
					if mergedResource == nil {
						resourceRes = n.resources
					} else {
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
				}
			}
		}
		resourcesField.Set(reflect.ValueOf(resourceRes))
	}

	// Get the ListenerClass field
	listenerClassField := config.FieldByName("ListenerClass")
	if listenerClassField.IsValid() && listenerClassField.IsZero() && listenerClassField.CanSet() {
		listenerClassField.Set(reflect.ValueOf(n.listenerClass))
	}

	// Get the Affinity field
	affinityField := config.FieldByName("Affinity")
	if affinityField.IsValid() && affinityField.IsZero() && affinityField.CanSet() {
		affinityField.Set(reflect.ValueOf(n.common.Affinity.Build()))
	}

	// You can continue to add logic to handle other fieldgracefulShutdownTimeoutSecondss
	// config.FieldByName("Logging").Set(reflect.ValueOf(n.common.Logging))
	// config.FieldByName("GracefulShutdownTimeoutSeconds").Set(reflect.ValueOf(n.common.gracefulShutdownTimeoutSeconds))
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
