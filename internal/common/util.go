package common

import (
	"fmt"
	hdfsv1alpha1 "github.com/zncdata-labs/hdfs-operator/api/v1alpha1"
	"github.com/zncdata-labs/hdfs-operator/internal/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"strings"
)

func OverrideEnvVars(origin *[]corev1.EnvVar, override map[string]string) {
	var originVars = make(map[string]int)
	for i, env := range *origin {
		originVars[env.Name] = i
	}

	for k, v := range override {
		// if env Name is in override, then override it
		if idx, ok := originVars[k]; ok {
			(*origin)[idx].Value = v
		} else {
			// if override's key is new, then append it
			*origin = append(*origin, corev1.EnvVar{
				Name:  k,
				Value: v,
			})
		}
	}
}

func CreateClusterServiceName(instanceName string) string {
	return instanceName + "-cluster"
}

// CreateRoleGroupLoggingConfigMapName create role group logging config-map name
func CreateRoleGroupLoggingConfigMapName(instanceName string, role string, groupName string) string {
	return util.NewResourceNameGenerator(instanceName, role, groupName).GenerateResourceName("log")
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
	return util.NewResourceNameGenerator(instanceName, string(role), groupName).GenerateResourceName("")
}

func CreateRoleServiceName(instanceName string, role Role, groupName string) string {
	return util.NewResourceNameGenerator(instanceName, string(role), groupName).GenerateResourceName("")
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

func CreateServiceAccountName(instanceName string) string {
	return util.NewResourceNameGeneratorOneRole(instanceName, "").GenerateResourceName("sa")
}

func CreateKvContentByReplicas(replicas int32, keyTemplate string, valueTemplate string) [][2]string {
	var res [][2]string
	for i := int32(0); i < replicas; i++ {
		key := fmt.Sprintf(keyTemplate, i)
		var value string
		if strings.Contains(valueTemplate, "%d") {
			value = fmt.Sprintf(valueTemplate, i)
		} else {
			value = valueTemplate
		}
		res = append(res, [2]string{key, value})
	}
	return res
}

func CreateXmlContentByReplicas(replicas int32, keyTemplate string, valueTemplate string) []util.XmlNameValuePair {
	var res []util.XmlNameValuePair
	for _, kv := range CreateKvContentByReplicas(replicas, keyTemplate, valueTemplate) {
		res = append(res, util.XmlNameValuePair{Name: kv[0], Value: kv[1]})
	}
	return res
}

func CreateRoleCfgCacheKey(instanceName string, role Role, groupName string) string {
	return util.NewResourceNameGenerator(instanceName, string(role), groupName).GenerateResourceName("cache")
}
func GetMergedRoleGroupCfg(role Role, instanceName string, groupName string) any {
	cfg, ok := MergedCache.Get(CreateRoleCfgCacheKey(instanceName, role, groupName))
	if !ok {
		cfg, ok = MergedCache.Get(CreateRoleCfgCacheKey(instanceName, role, "default"))
		if ok {
			return cfg
		}
		panic(fmt.Sprintf("%s config not found in cache)", role))
	}
	return cfg
}

func GetCommonContainerEnv(zkDiscoveryZNode string, container ContainerComponent) []corev1.EnvVar {
	return []corev1.EnvVar{
		{
			Name:  "HADOOP_CONF_DIR",
			Value: "/stackable/config/" + string(container),
		},
		{
			Name:  "HADOOP_HOME",
			Value: "/stackable/hadoop", // todo rename
		},
		{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			},
		},
		{
			Name: "ZOOKEEPER",
			ValueFrom: &corev1.EnvVarSource{
				ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: zkDiscoveryZNode,
					},
					Key: ZookeeperHdfsDiscoveryKey,
				},
			},
		},
	}
}

func GetCommonCommand() []string {
	return []string{"/bin/bash", "-x", "-euo", "pipefail", "-c"}
}

const (
	HdfsConsoleLogAppender = "CONSOLE"
	HdfsFileLogAppender    = "FILE"
)

func CreateLog4jBuilder(containerLogging *hdfsv1alpha1.LoggingConfigSpec, consoleAppenderName,
	fileAppenderName string) *Log4jLoggingDataBuilder {
	log4jBuilder := &Log4jLoggingDataBuilder{}
	if loggers := containerLogging.Loggers; loggers != nil {
		var builderLoggers []LogBuilderLoggers
		for logger, level := range loggers {
			builderLoggers = append(builderLoggers, LogBuilderLoggers{
				logger: logger,
				level:  level.Level,
			})
		}
		log4jBuilder.Loggers = builderLoggers
	}
	if console := containerLogging.Console; console != nil {
		log4jBuilder.Console = &LogBuilderAppender{
			appenderName: consoleAppenderName,
			level:        console.Level,
		}
	}
	if file := containerLogging.File; file != nil {
		log4jBuilder.File = &LogBuilderAppender{
			appenderName: fileAppenderName,
			level:        file.Level,
		}
	}

	return log4jBuilder
}

func NameNodePodNames(instanceName string, groupName string) []string {
	nameNodeStatefulSetName := CreateNameNodeStatefulSetName(instanceName, groupName)
	nameNodeCfg := GetMergedRoleGroupCfg(NameNode, instanceName, groupName).(*hdfsv1alpha1.NameNodeRoleGroupSpec)
	naneNodeReplicas := nameNodeCfg.Replicas
	pods := CreatePodNamesByReplicas(naneNodeReplicas, nameNodeStatefulSetName)
	return pods
}
