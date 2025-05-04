package container

import (
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/operator-go/pkg/constants"
	"github.com/zncdatadev/operator-go/pkg/util"
	corev1 "k8s.io/api/core/v1"
)

// zkfc container builder
type ZkfcContainerBuilder struct {
	common.ContainerBuilder
	zookeeperConfigMapName string
	clusterConfig          *hdfsv1alpha1.ClusterConfigSpec
}

func NewZkfcContainerBuilder(
	instance *hdfsv1alpha1.HdfsCluster,
	resource corev1.ResourceRequirements,
	image *util.Image,
) *ZkfcContainerBuilder {
	clusterConfig := instance.Spec.ClusterConfig
	zookeeperConfigMapName := clusterConfig.ZookeeperConfigMapName
	return &ZkfcContainerBuilder{
		ContainerBuilder:       *common.NewContainerBuilder(image.String(), image.GetPullPolicy(), resource),
		zookeeperConfigMapName: zookeeperConfigMapName,
		clusterConfig:          clusterConfig,
	}
}

func (z *ZkfcContainerBuilder) ContainerName() string {
	return string(Zkfc)
}

// CommandArgs zookeeper fail-over controller command args
func (z *ZkfcContainerBuilder) CommandArgs() []string {
	return common.ParseTemplate(`mkdir -p /kubedoop/config/zkfc
cp /kubedoop/mount/config/zkfc/*.xml /kubedoop/config/zkfc
cp /kubedoop/mount/config/zkfc/zkfc.log4j.properties /kubedoop/config/zkfc/log4j.properties

{{ if .kerberosEnabled }}
{{- .kerberosEnv }}
{{- end }}

/kubedoop/hadoop/bin/hdfs zkfc
`, common.CreateExportKrbRealmEnvData(z.clusterConfig))
}

func (z *ZkfcContainerBuilder) ContainerEnv() []corev1.EnvVar {
	return common.GetCommonContainerEnv(z.clusterConfig, Zkfc)
}

func (z *ZkfcContainerBuilder) VolumeMount() []corev1.VolumeMount {
	mounts := common.GetCommonVolumeMounts(z.clusterConfig)
	zkfcMounts := []corev1.VolumeMount{
		{
			Name:      hdfsv1alpha1.HdfsConfigVolumeMountName,
			MountPath: constants.KubedoopConfigDirMount + "/" + z.ContainerName(),
		},
		{
			Name:      hdfsv1alpha1.HdfsConfigVolumeMountName,
			MountPath: constants.KubedoopLogDirMount + "/" + z.ContainerName(),
		},
	}
	return append(mounts, zkfcMounts...)
}

func (z *ZkfcContainerBuilder) Command() []string {
	return common.GetCommonCommand()
}
