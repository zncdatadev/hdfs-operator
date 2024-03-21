package common

import (
	hdfsv1alpha1 "github.com/zncdata-labs/hdfs-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

type ResourceNameGenerator struct {
	InstanceName string
	RoleName     string
	GroupName    string
}

// NewResourceNameGenerator new a ResourceNameGenerator
func NewResourceNameGenerator(instanceName, roleName, groupName string) *ResourceNameGenerator {
	return &ResourceNameGenerator{
		InstanceName: instanceName,
		RoleName:     roleName,
		GroupName:    groupName,
	}
}

// NewResourceNameGeneratorOneRole new a ResourceNameGenerator without roleName
func NewResourceNameGeneratorOneRole(instanceName, groupName string) *ResourceNameGenerator {
	return &ResourceNameGenerator{
		InstanceName: instanceName,
		GroupName:    groupName,
	}
}

// GenerateResourceName generate resource Name
func (r *ResourceNameGenerator) GenerateResourceName(extraSuffix string) string {
	var res string
	if r.InstanceName != "" {
		res = r.InstanceName + "-"
	}
	if r.GroupName != "" {
		res = res + r.GroupName + "-"
	}
	if r.RoleName != "" {
		res = res + r.RoleName
	} else {
		res = res[:len(res)-1]
	}
	if extraSuffix != "" {
		return res + "-" + extraSuffix
	}
	return res
}

func OverrideEnvVars(origin *[]corev1.EnvVar, override map[string]string) {
	for _, env := range *origin {
		// if env Name is in override, then override it
		if value, ok := override[env.Name]; ok {
			env.Value = value
		}
	}
}

func CreateClusterServiceName(instanceName string) string {
	return instanceName + "-cluster"
}

// CreateRoleGroupLoggingConfigMapName create role group logging config-map name
func CreateRoleGroupLoggingConfigMapName(instanceName string, role string, groupName string) string {
	return NewResourceNameGenerator(instanceName, role, groupName).GenerateResourceName("log")
}

func ConvertToResourceRequirements(resources *hdfsv1alpha1.ResourcesSpec) *corev1.ResourceRequirements {
	var (
		cpuMin      = resource.MustParse(hdfsv1alpha1.CpuMin)
		cpuMax      = resource.MustParse(hdfsv1alpha1.CpuMax)
		memoryLimit = resource.MustParse(hdfsv1alpha1.MemoryLimit)
	)
	if resources != nil {
		if resources.CPU != nil && resources.CPU.Min != nil {
			cpuMin = *resources.CPU.Min
		}
		if resources.CPU != nil && resources.CPU.Max != nil {
			cpuMax = *resources.CPU.Max
		}
		if resources.Memory != nil && resources.Memory.Limit != nil {
			memoryLimit = *resources.Memory.Limit
		}
	}
	return &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    cpuMax,
			corev1.ResourceMemory: memoryLimit,
		},
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    cpuMin,
			corev1.ResourceMemory: memoryLimit,
		},
	}
}
