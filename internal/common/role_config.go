package common

import (
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
	"reflect"
	"time"
)

const (
	ListenerPvcStorage = "10Mi"
)

type RoleNodeConfig struct {
	resources *hdfsv1alpha1.ResourcesSpec
	// logging config todo
	listenerClass string
	common        *GeneralNodeConfig
}

type GeneralNodeConfig struct {
	Affinity *AffinityBuilder

	gracefulShutdownTimeoutSeconds time.Duration
}

func newDefaultResourceSpec(role Role) *hdfsv1alpha1.ResourcesSpec {
	var cpuMin, cpuMax, memoryLimit, storage *resource.Quantity
	switch role {
	case NameNode:
		cpuMin = parseQuantity("250m")
		cpuMax = parseQuantity("1000m")
		memoryLimit = parseQuantity("1024Mi")
		storage = parseQuantity("2Gi")
	case DataNode:
		cpuMin = parseQuantity("100m")
		cpuMax = parseQuantity("400m")
		memoryLimit = parseQuantity("512Mi")
		storage = parseQuantity("10Gi")
	case JournalNode:
		cpuMin = parseQuantity("100m")
		cpuMax = parseQuantity("400m")
		memoryLimit = parseQuantity("512Mi")
		storage = parseQuantity("1Gi")
	default:
		panic("invalid role")
	}
	return &hdfsv1alpha1.ResourcesSpec{
		CPU: &hdfsv1alpha1.CPUResource{
			Min: cpuMin,
			Max: cpuMax,
		},
		Memory: &hdfsv1alpha1.MemoryResource{
			Limit: memoryLimit,
		},
		Storage: &hdfsv1alpha1.StorageResource{
			Capacity: storage,
		},
	}
}

// DefaultNodeConfig default node config
func DefaultNodeConfig(clusterName string, role Role, listenerClass string, gracefulShutdownTimeoutSeconds time.Duration) *RoleNodeConfig {
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
			gracefulShutdownTimeoutSeconds: gracefulShutdownTimeoutSeconds,
		},
	}
}

func DefaultNameNodeConfig(clusterName string) *RoleNodeConfig {
	return DefaultNodeConfig(clusterName, NameNode, string(LoadBalancerClass), 15*time.Minute)
}

func DefaultDataNodeConfig(clusterName string) *RoleNodeConfig {
	return DefaultNodeConfig(clusterName, DataNode, string(NodePort), 30*time.Minute)
}

func DefaultJournalNodeConfig(clusterName string) *RoleNodeConfig {
	return DefaultNodeConfig(clusterName, JournalNode, "", 15*time.Minute)
}

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
	if resourcesField.IsValid() && resourcesField.IsZero() && resourcesField.CanSet() {
		resourcesField.Set(reflect.ValueOf(n.resources))
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

	// You can continue to add logic to handle other fields
	// config.FieldByName("Logging").Set(reflect.ValueOf(n.common.Logging))
	// config.FieldByName("GracefulShutdownTimeoutSeconds").Set(reflect.ValueOf(n.common.gracefulShutdownTimeoutSeconds))
}

func parseQuantity(q string) *resource.Quantity {
	r := resource.MustParse(q)
	return &r
}
