package container

import (
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/util"
	corev1 "k8s.io/api/core/v1"
)

// zkfc container builder
type ZkfcContainerBuilder struct {
	common.ContainerBuilder
	zookeeperDiscoveryZNode string
	clusterConfig           *hdfsv1alpha1.ClusterConfigSpec
}

func NewZkfcContainerBuilder(
	instance *hdfsv1alpha1.HdfsCluster,
	resource corev1.ResourceRequirements,
) *ZkfcContainerBuilder {
	imageSpec := instance.Spec.Image
	image := util.ImageRepository(imageSpec.Repository, imageSpec.Tag)
	imagePullPolicy := imageSpec.PullPolicy
	clusterConfig := instance.Spec.ClusterConfigSpec
	zookeeperDiscoveryZNode := clusterConfig.ZookeeperConfigMapName
	return &ZkfcContainerBuilder{
		ContainerBuilder:        *common.NewContainerBuilder(image, imagePullPolicy, resource),
		zookeeperDiscoveryZNode: zookeeperDiscoveryZNode,
		clusterConfig:           clusterConfig,
	}
}

func (z *ZkfcContainerBuilder) ContainerName() string {
	return string(Zkfc)
}

// CommandArgs zookeeper fail-over controller command args
func (z *ZkfcContainerBuilder) CommandArgs() []string {
	return common.ParseKerberosScript(`mkdir -p /stackable/config/zkfc
cp /stackable/mount/config/zkfc/*.xml /stackable/config/zkfc
cp /stackable/mount/config/zkfc/zkfc.log4j.properties /stackable/config/zkfc/log4j.properties

{{ if .kerberosEnabled }}
{{- .kerberosEnv }}
{{- end }}

/stackable/hadoop/bin/hdfs zkfc
`, common.CreateExportKrbRealmEnvData(z.clusterConfig))
}

func (z *ZkfcContainerBuilder) ContainerEnv() []corev1.EnvVar {
	return common.GetCommonContainerEnv(z.clusterConfig, Zkfc)
}

func (z *ZkfcContainerBuilder) VolumeMount() []corev1.VolumeMount {
	mounts := common.GetCommonVolumeMounts(z.clusterConfig)
	zkfcMounts := []corev1.VolumeMount{
		{
			Name:      ZkfcVolumeName(),
			MountPath: "/stackable/mount/config/zkfc",
		},
		{
			Name:      ZkfcLogVolumeName(),
			MountPath: "/stackable/mount/log/zkfc",
		},
	}
	return append(mounts, zkfcMounts...)
}

func (z *ZkfcContainerBuilder) Command() []string {
	return common.GetCommonCommand()
}
