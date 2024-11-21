package container

import (
	"fmt"
	"maps"
	"strings"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/constants"

	"github.com/zncdatadev/hdfs-operator/internal/common"
	corev1 "k8s.io/api/core/v1"
)

// FormatNameNodeContainerBuilder container builder
type FormatNameNodeContainerBuilder struct {
	common.ContainerBuilder
	zookeeperConfigMapName string
	nameNodeReplicates     int32
	statefulSetName        string
	instanceName           string
	namespace              string
	clusterConfig          *hdfsv1alpha1.ClusterConfigSpec
}

func NewFormatNameNodeContainerBuilder(
	instance *hdfsv1alpha1.HdfsCluster,
	resource corev1.ResourceRequirements,
	nameNodeReplicates int32,
	statefulSetName string,
) *FormatNameNodeContainerBuilder {
	imageSpec := instance.Spec.Image
	image := hdfsv1alpha1.TransformImage(imageSpec)
	clusterConfig := instance.Spec.ClusterConfig
	return &FormatNameNodeContainerBuilder{
		ContainerBuilder:       *common.NewContainerBuilder(image.String(), image.GetPullPolicy(), resource),
		zookeeperConfigMapName: clusterConfig.ZookeeperConfigMapName,
		nameNodeReplicates:     nameNodeReplicates,
		statefulSetName:        statefulSetName,
		instanceName:           instance.Name,
		namespace:              instance.Namespace,
		clusterConfig:          clusterConfig,
	}
}

func (f *FormatNameNodeContainerBuilder) ContainerEnv() []corev1.EnvVar {
	return common.GetCommonContainerEnv(f.clusterConfig, FormatNameNode)
}

func (f *FormatNameNodeContainerBuilder) VolumeMount() []corev1.VolumeMount {
	mounts := common.GetCommonVolumeMounts(f.clusterConfig)
	fnMounts := []corev1.VolumeMount{
		{
			Name:      hdfsv1alpha1.FormatNamenodesConfigVolumeMountName,
			MountPath: constants.KubedoopConfigDirMount + "/" + f.ContainerName(),
		},
		{
			Name:      hdfsv1alpha1.FormatNamenodesLogVolumeMountName,
			MountPath: constants.KubedoopLogDirMount + "/" + f.ContainerName(),
		},
		{
			Name:      hdfsv1alpha1.DataVolumeMountName,
			MountPath: constants.KubedoopDataDir,
		},
	}
	return append(mounts, fnMounts...)
}

func (f *FormatNameNodeContainerBuilder) ContainerName() string {
	return string(FormatNameNode)
}

func (f *FormatNameNodeContainerBuilder) Command() []string {
	return common.GetCommonCommand()
}
func (f *FormatNameNodeContainerBuilder) CommandArgs() []string {
	namenodeIds := strings.Join(f.PodNames(), " ")
	tmpl := `mkdir -p /kubedoop/config/format-namenodes
cp /kubedoop/mount/config/format-namenodes/*.xml /kubedoop/config/format-namenodes
cp /kubedoop/mount/config/format-namenodes/format-namenodes.log4j.properties /kubedoop/config/format-namenodes/log4j.properties

{{ if .kerberosEnabled }}
{{- .kerberosEnv }}

{{- .kinitScript }}

{{- end }}


echo "Start formatting namenode $POD_NAME. Checking for active namenodes:"
` + fmt.Sprintf("for namenode_id in %s", namenodeIds) + "\n" +
		`do
    echo -n "Checking pod $namenode_id... "
    SERVICE_STATE=$(/kubedoop/hadoop/bin/hdfs haadmin -getServiceState $namenode_id | tail -n1 || true)
    if [ "$SERVICE_STATE" == "active" ]
    then
        ACTIVE_NAMENODE=$namenode_id
        echo "active"
        break
    fi
    echo ""
done

if [ ! -f "/kubedoop/data/namenode/current/VERSION" ]
then
    if [ -z ${ACTIVE_NAMENODE+x} ]
    then
        echo "Create pod $POD_NAME as active namenode."
        /kubedoop/hadoop/bin/hdfs namenode -format -noninteractive
    else
        echo "Create pod $POD_NAME as standby namenode."
        /kubedoop/hadoop/bin/hdfs namenode -bootstrapStandby -nonInteractive
    fi
else
    cat "/kubedoop/data/namenode/current/VERSION"
    echo "Pod $POD_NAME already formatted. Skipping..."
fi
`
	data := common.CreateExportKrbRealmEnvData(f.clusterConfig)
	principal := common.CreateKerberosPrincipal(f.instanceName, f.namespace, GetRole())
	maps.Copy(data, common.CreateGetKerberosTicketData(principal))
	return common.ParseTemplate(tmpl, data)
}

func (f *FormatNameNodeContainerBuilder) PodNames() []string {
	return common.CreatePodNamesByReplicas(f.nameNodeReplicates, f.statefulSetName)
}
