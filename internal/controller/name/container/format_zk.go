package container

import (
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/operator-go/pkg/constants"
	corev1 "k8s.io/api/core/v1"
)

// FormatZookeeperContainerBuilder container builder
type FormatZookeeperContainerBuilder struct {
	common.ContainerBuilder
	zookeeperConfigMapName string
	namespace              string
	clusterConfig          *hdfsv1alpha1.ClusterConfigSpec
}

func NewFormatZookeeperContainerBuilder(
	instance *hdfsv1alpha1.HdfsCluster,
	resource corev1.ResourceRequirements,
	zookeeperConfigMapName string,
) *FormatZookeeperContainerBuilder {
	imageSpec := instance.Spec.Image
	image := hdfsv1alpha1.TransformImage(imageSpec)

	return &FormatZookeeperContainerBuilder{
		ContainerBuilder:       *common.NewContainerBuilder(image.String(), *image.GetPullPolicy(), resource),
		zookeeperConfigMapName: zookeeperConfigMapName,
		namespace:              instance.Namespace,
		clusterConfig:          instance.Spec.ClusterConfigSpec,
	}
}

func (z *FormatZookeeperContainerBuilder) ContainerEnv() []corev1.EnvVar {
	return common.GetCommonContainerEnv(z.clusterConfig, FormatZookeeper)
}

func (z *FormatZookeeperContainerBuilder) VolumeMount() []corev1.VolumeMount {
	mounts := common.GetCommonVolumeMounts(z.clusterConfig)
	fzMounts := []corev1.VolumeMount{
		{
			Name:      hdfsv1alpha1.FormatZookeeperConfigVolumeMountName,
			MountPath: constants.KubedoopConfigDirMount + "/" + z.ContainerName(),
		},
		{
			Name:      hdfsv1alpha1.FormatZookeeperLogVolumeMountName,
			MountPath: constants.KubedoopLogDirMount + "/" + z.ContainerName(),
		},
	}
	return append(mounts, fzMounts...)
}

func (z *FormatZookeeperContainerBuilder) ContainerName() string {
	return string(FormatZookeeper)
}

func (z *FormatZookeeperContainerBuilder) Command() []string {
	return common.GetCommonCommand()
}
func (z *FormatZookeeperContainerBuilder) CommandArgs() []string {
	tmpl := `mkdir -p /kubedoop/config/format-zookeeper
cp /kubedoop/mount/config/format-zookeeper/*.xml /kubedoop/config/format-zookeeper
cp /kubedoop/mount/config/format-zookeeper/format-zookeeper.log4j.properties /kubedoop/config/format-zookeeper/log4j.properties

{{ if .kerberosEnabled }}
{{- .kerberosEnv }}
{{- end }}

echo "Attempt to format ZooKeeper..."
if [[ "0" -eq "$(echo $POD_NAME | sed -e 's/.*-//')" ]] ; then
    set +e
    /kubedoop/hadoop/bin/hdfs zkfc -formatZK -nonInteractive
    EXITCODE=$?
    set -e
    if [[ $EXITCODE -eq 0 ]]; then
        echo "Successfully formatted"
    elif [[ $EXITCODE -eq 2 ]]; then
        echo "ZNode already existed, did nothing"
    else
        echo "Zookeeper format failed with exit code $EXITCODE"
        exit $EXITCODE
    fi

else
    echo "ZooKeeper already formatted!"
fi
`
	return common.ParseTemplate(tmpl, common.CreateExportKrbRealmEnvData(z.clusterConfig))
}
