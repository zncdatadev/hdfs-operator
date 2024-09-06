package common

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/util"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/constants"
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

func ConvertToResourceRequirements(resources *commonsv1alpha1.ResourcesSpec) *corev1.ResourceRequirements {
	if resources != nil {
		request := corev1.ResourceList{}
		limit := corev1.ResourceList{}
		if resources.CPU != nil && resources.CPU.Min.IsZero() {
			request[corev1.ResourceCPU] = resources.CPU.Min
		}
		if resources.CPU != nil && resources.CPU.Max.IsZero() {
			limit[corev1.ResourceCPU] = resources.CPU.Max
		}
		if resources.Memory != nil && resources.Memory.Limit.IsZero() {
			request[corev1.ResourceMemory] = resources.Memory.Limit
			limit[corev1.ResourceMemory] = resources.Memory.Limit
		}
		r := &corev1.ResourceRequirements{}
		if len(request) > 0 {
			r.Requests = request
		}
		if len(limit) > 0 {
			r.Limits = limit
		}
		return r
	}
	return nil
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

func GetCommonContainerEnv(clusterConfig *hdfsv1alpha1.ClusterConfigSpec, container ContainerComponent) []corev1.EnvVar {
	envs := []corev1.EnvVar{
		{
			Name:  "HADOOP_CONF_DIR",
			Value: constants.KubedoopConfigDir + "/" + string(container),
		},
		{
			Name:  "HADOOP_HOME",
			Value: hdfsv1alpha1.HadoopHome,
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
						Name: clusterConfig.ZookeeperConfigMapName,
					},
					Key: ZookeeperHdfsDiscoveryKey,
				},
			},
		},
	}

	var jvmArgs = make([]string, 0)
	var envName string
	if IsKerberosEnabled(clusterConfig) {
		envs = append(envs, SecurityEnvs(container, &jvmArgs)...)
	}
	if envName = getEnvNameByContainerComponent(container); envName != "" {
		jvmArgs = append(jvmArgs, "-Xmx419430k")
		securityDir := getSubDirByContainerComponent(container)
		securityConfigEnValue := fmt.Sprintf("-Djava.security.properties=%s/%s/security.properties", constants.KubedoopConfigDir, securityDir)
		jvmArgs = append(jvmArgs, securityConfigEnValue)

	}
	if len(jvmArgs) != 0 && envName != "" {
		envs = append(envs, corev1.EnvVar{
			Name:  envName,
			Value: strings.Join(jvmArgs, " "),
		})
	}
	return envs
}

func GetCommonVolumes(clusterConfig *hdfsv1alpha1.ClusterConfigSpec, instanceName string, role Role) []corev1.Volume {
	limit := resource.MustParse("150Mi")
	volumes := []corev1.Volume{
		{
			Name: hdfsv1alpha1.KubedoopLogVolumeMountName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{
					SizeLimit: &limit,
				},
			},
		},
	}
	if IsKerberosEnabled(clusterConfig) {
		secretClass := clusterConfig.Authentication.Kerberos.SecretClass
		volumes = append(volumes, CreateKerberosSecretPvc(secretClass, instanceName, role))
	}
	if IsTlsEnabled(clusterConfig) {
		tlsSecretClass := clusterConfig.Authentication.Tls.SecretClass
		volumes = append(volumes, CreateTlsSecretPvc(tlsSecretClass, clusterConfig.Authentication.Tls.JksPassword))
	}
	return volumes

}

func getEnvNameByContainerComponent(container ContainerComponent) string {
	switch string(container) {
	case string(NameNode):
		return "HDFS_NAMENODE_OPTS"
	case string(DataNode):
		return "HDFS_DATANODE_OPTS"
	case string(JournalNode):
		return "HDFS_JOURNALNODE_OPTS"
	default:
		return ""
	}
}

func getSubDirByContainerComponent(container ContainerComponent) string {
	switch string(container) {
	case string(NameNode):
		return "namenode"
	case string(DataNode):
		return "datanode"
	case string(JournalNode):
		return "journalnode"
	default:
		panic(fmt.Sprintf("unsupported container component for get sub dir: %s", container))
	}
}

func GetCommonCommand() []string {
	return []string{"/bin/bash", "-x", "-euo", "pipefail", "-c"}
}

func GetCommonVolumeMounts(clusterConfig *hdfsv1alpha1.ClusterConfigSpec) []corev1.VolumeMount {
	mounts := []corev1.VolumeMount{
		{
			Name:      hdfsv1alpha1.KubedoopLogVolumeMountName,
			MountPath: constants.KubedoopLogDir,
		},
	}
	if IsKerberosEnabled(clusterConfig) {
		mounts = append(mounts, SecurityVolumeMounts()...)
	}
	if IsTlsEnabled(clusterConfig) {
		mounts = append(mounts, TlsVolumeMounts()...)
	}
	return mounts
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

func AffinityDefault(role Role, crName string) *corev1.Affinity {
	return &corev1.Affinity{
		PodAffinity: &corev1.PodAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{
					Weight: 20,
					PodAffinityTerm: corev1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								LabelCrName: crName,
							},
						},
						TopologyKey: corev1.LabelHostname,
					},
				},
			},
		},
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{
					Weight: 70,
					PodAffinityTerm: corev1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								LabelCrName:    crName,
								LabelComponent: string(role),
							},
						},
						TopologyKey: corev1.LabelHostname,
					},
				},
			},
		},
	}
}
