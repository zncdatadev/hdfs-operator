package common

import (
	"fmt"
	hdfsv1alpha1 "github.com/zncdata-labs/hdfs-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"strings"
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

// Name node

func CreateNameNodeStatefulSetName(instanceName string, groupName string) string {
	return CreateRoleStatefulSetName(instanceName, NameNode, groupName)
}

func CreateNameNodeServiceName(instanceName string, groupName string) string {
	return CreateRoleServiceName(instanceName, NameNode, groupName)
}

// Data node

func CreateDataNodeStatefulSetName(instanceName string, groupName string) string {
	return CreateRoleStatefulSetName(instanceName, DataNode, groupName)
}

func CreateDataNodeServiceName(instanceName string, groupName string) string {
	return CreateRoleServiceName(instanceName, DataNode, groupName)
}

// Journal node

func CreateJournalNodeStatefulSetName(instanceName string, groupName string) string {
	return CreateRoleStatefulSetName(instanceName, JournalNode, groupName)
}

func CreateJournalNodeServiceName(instanceName string, groupName string) string {
	return CreateRoleServiceName(instanceName, JournalNode, groupName)
}

// CreateJournalUrl create Journal Url
func CreateJournalUrl(jnSvcs []string, instanceName string) string {
	return fmt.Sprintf("qjournal://%s/%s", strings.Join(jnSvcs, ";"), instanceName)
}

func CreateNetworksByReplicates(replicates int32, statefulResourceName, svcName, namespace,
	clusterDomain string, port int32) []string {
	networks := make([]string, replicates)
	for i := int32(0); i < replicates; i++ {
		podName := fmt.Sprintf("%s-%d", statefulResourceName, i)
		networkUrl := CreateNetworkUrl(podName, svcName, namespace, clusterDomain, port)
		networks[i] = networkUrl
	}
	return networks
}

func CreateNetworkUrl(podName string, svcName, namespace, clusterDomain string, port int32) string {
	return podName + "." + CreateDnsDomain(svcName, namespace, clusterDomain, port)
}

func CreateDnsDomain(svcName, namespace, clusterDomain string, port int32) string {
	return fmt.Sprintf("%s.%s.svc.%s:%d", svcName, namespace, clusterDomain, port)
}

func CreateRoleStatefulSetName(instanceName string, role Role, groupName string) string {
	return NewResourceNameGenerator(instanceName, string(role), groupName).GenerateResourceName("")
}

func CreateRoleServiceName(instanceName string, role Role, groupName string) string {
	return NewResourceNameGenerator(instanceName, string(role), groupName).GenerateResourceName("")
}

// CreatePodNamesByReplicas create pod names by replicas
func CreatePodNamesByReplicas(replicas int32, statefulResourceName string) []string {
	podNames := make([]string, replicas)
	for i := int32(0); i < replicas; i++ {
		podName := fmt.Sprintf("%s-%d", statefulResourceName, i)
		podNames[i] = podName
	}
	return podNames
}

func CreateKvContentByReplicas(replicas int32, keyTemplate string, valueTemplate string) [][2]string {
	var res [][2]string
	for i := int32(0); i < replicas; i++ {
		key := fmt.Sprintf(keyTemplate, i)
		value := fmt.Sprintf(valueTemplate, i)
		res = append(res, [2]string{key, value})
	}
	return res
}

const xmlContentTemplate = `  <property>
	<name>%s</name>
	<value>%s</value>
  </property>\n
`

func CreateXmlContentByReplicas(replicas int32, keyTemplate string, valueTemplate string) string {
	var res string
	for _, kv := range CreateKvContentByReplicas(replicas, keyTemplate, valueTemplate) {
		res += fmt.Sprintf(xmlContentTemplate, kv[0], kv[1])
	}
	return res
}

func CreateRoleCfgCacheKey(instanceName string, role Role, groupName string) string {
	return NewResourceNameGenerator(instanceName, string(role), groupName).GenerateResourceName("cache")
}
