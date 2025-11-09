package common

import (
	"fmt"
	"path"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	"github.com/zncdatadev/hdfs-operator/internal/util"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/constants"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
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
		if resources.CPU != nil && !resources.CPU.Min.IsZero() {
			request[corev1.ResourceCPU] = resources.CPU.Min
		}
		if resources.CPU != nil && !resources.CPU.Max.IsZero() {
			limit[corev1.ResourceCPU] = resources.CPU.Max
		}
		if resources.Memory != nil && !resources.Memory.Limit.IsZero() {
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
	res := make([]util.XmlNameValuePair, 0, replicas)
	for _, kv := range CreateKvContentByReplicas(replicas, keyTemplate, valueTemplate) {
		res = append(res, util.XmlNameValuePair{Name: kv[0], Value: kv[1]})
	}
	return res
}

func GetCommonContainerEnv(
	clusterConfig *hdfsv1alpha1.ClusterConfigSpec,
	container constant.ContainerComponent,
	role *constant.Role,
) []corev1.EnvVar {
	envs := []corev1.EnvVar{
		{
			Name:  "HADOOP_CONF_DIR",
			Value: path.Join(constants.KubedoopConfigDir, getSubDirByContainerComponent(container)),
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
					Key: constant.ZookeeperHdfsDiscoveryKey,
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

		securityConfigEnValue := fmt.Sprintf("-Djava.security.properties=%s", path.Join(constants.KubedoopConfigDir, securityDir, "security.properties"))
		if role != nil {
			if metricPort, err := GetMetricsPort(*role); err == nil {
				jvmArgs = append(jvmArgs, "-javaagent:"+path.Join(constants.KubedoopJmxDir, "jmx_prometheus_javaagent.jar")+"="+fmt.Sprintf("%d", metricPort)+":"+path.Join(constants.KubedoopConfigDir, fmt.Sprintf("%s.yaml", strings.ToLower(string(container)))))
			} else {
				panic(err) // TODO: handle error
			}
		}
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

func GetCommonVolumes(clusterConfig *hdfsv1alpha1.ClusterConfigSpec, instanceName string, roleGroupInfo *reconciler.RoleGroupInfo) []corev1.Volume {
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
		role := constant.Role(roleGroupInfo.GetRoleName())
		volumes = append(volumes, CreateKerberosSecretPvc(secretClass, instanceName, role))
	}
	if IsTlsEnabled(clusterConfig) {
		tlsSecretClass := clusterConfig.Authentication.Tls.SecretClass
		volumes = append(volumes, CreateTlsSecretPvc(tlsSecretClass, clusterConfig.Authentication.Tls.JksPassword, roleGroupInfo))
	}
	return volumes

}

// Get metrics Port
func GetMetricsPort(role constant.Role) (int32, error) {
	var metricsPort int32
	switch role {
	case constant.NameNode:
		metricsPort = hdfsv1alpha1.NameNodeMetricPort
	case constant.DataNode:
		metricsPort = hdfsv1alpha1.DataNodeMetricPort
	case constant.JournalNode:
		metricsPort = hdfsv1alpha1.JournalNodeMetricPort
	default:
		return 0, fmt.Errorf("unknown role: %s", role)
	}
	return metricsPort, nil
}

func getEnvNameByContainerComponent(container constant.ContainerComponent) string {
	switch string(container) {
	case string(constant.NameNodeComponent):
		return "HDFS_NAMENODE_OPTS"
	case string(constant.DataNodeComponent):
		return "HDFS_DATANODE_OPTS"
	case string(constant.JournalNodeComponent):
		return "HDFS_JOURNALNODE_OPTS"
	default:
		return ""
	}
}

func getSubDirByContainerComponent(container constant.ContainerComponent) string {
	return string(container)
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

// Log4j logging configuration structures
type LogBuilderLoggers struct {
	logger string
	level  string
}

type LogBuilderAppender struct {
	appenderName string
	level        string
}

type Log4jLoggingDataBuilder struct {
	Loggers []LogBuilderLoggers
	Console *LogBuilderAppender
	File    *LogBuilderAppender
}

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

func AffinityDefault(role constant.Role, crName string) *corev1.Affinity {
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

// Create service metrics Name
func CreateServiceMetricsName(roleGroupInfo *reconciler.RoleGroupInfo) string {
	return roleGroupInfo.GetFullName() + "-metrics"
}
