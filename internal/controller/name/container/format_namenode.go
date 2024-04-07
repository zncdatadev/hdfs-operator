package container

import (
	"fmt"
	"github.com/zncdata-labs/hdfs-operator/internal/common"
	corev1 "k8s.io/api/core/v1"
	"strings"
)

// FormatNameNodeContainerBuilder container builder
type FormatNameNodeContainerBuilder struct {
	common.ContainerBuilder
	zookeeperDiscoveryZNode string
	nameNodeReplicates      int32
	statefulSetName         string
}

func NewFormatNameNodeContainerBuilder(
	image string,
	imagePullPolicy corev1.PullPolicy,
	resource corev1.ResourceRequirements,
	zookeeperDiscoveryZNode string,
	nameNodeReplicates int32,
	statefulSetName string,
) *FormatNameNodeContainerBuilder {
	return &FormatNameNodeContainerBuilder{
		ContainerBuilder:        *common.NewContainerBuilder(image, imagePullPolicy, resource),
		zookeeperDiscoveryZNode: zookeeperDiscoveryZNode,
		nameNodeReplicates:      nameNodeReplicates,
		statefulSetName:         statefulSetName,
	}
}

func (f *FormatNameNodeContainerBuilder) ContainerEnv() []corev1.EnvVar {
	return common.GetCommonContainerEnv(f.zookeeperDiscoveryZNode, FormatNameNode)
}

func (f *FormatNameNodeContainerBuilder) VolumeMount() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:      LogVolumeName(),
			MountPath: "/stackable/log",
		},
		{
			Name:      FormatNameNodeVolumeName(),
			MountPath: "/stackable/mount/config/format-namenodes",
		},
		{
			Name:      FormatNameNodeLogVolumeName(),
			MountPath: "/stackable/mount/log/format-namenodes",
		},
		{
			Name:      DataVolumeName(),
			MountPath: "/stackable/data",
		},
	}
}

func (f *FormatNameNodeContainerBuilder) ContainerName() string {
	return string(FormatNameNode)
}

func (f *FormatNameNodeContainerBuilder) Command() []string {
	return common.GetCommonCommand()
}
func (f *FormatNameNodeContainerBuilder) CommandArgs() []string {
	namenodeIds := strings.Join(f.PodNames(), " ")
	return []string{`mkdir -p /stackable/config/format-namenodes
cp /stackable/mount/config/format-namenodes/*.xml /stackable/config/format-namenodes
cp /stackable/mount/config/format-namenodes/format-namenodes.log4j.properties /stackable/config/format-namenodes/log4j.properties
echo "Start formatting namenode $POD_NAME. Checking for active namenodes:"
` + fmt.Sprintf("for namenode_id in %s", namenodeIds) + "\n" +
		`do
    echo -n "Checking pod $namenode_id... "
    SERVICE_STATE=$(/stackable/hadoop/bin/hdfs haadmin -getServiceState $namenode_id | tail -n1 || true)
    if [ "$SERVICE_STATE" == "active" ]
    then
        ACTIVE_NAMENODE=$namenode_id
        echo "active"
        break
    fi
    echo ""
done

if [ ! -f "/stackable/data/namenode/current/VERSION" ]
then
    if [ -z ${ACTIVE_NAMENODE+x} ]
    then
        echo "Create pod $POD_NAME as active namenode."
        /stackable/hadoop/bin/hdfs namenode -format -noninteractive
    else
        echo "Create pod $POD_NAME as standby namenode."
        /stackable/hadoop/bin/hdfs namenode -bootstrapStandby -nonInteractive
    fi
else
    cat "/stackable/data/namenode/current/VERSION"
    echo "Pod $POD_NAME already formatted. Skipping..."
fi
`}
}

func (f *FormatNameNodeContainerBuilder) PodNames() []string {
	return common.CreatePodNamesByReplicas(f.nameNodeReplicates, f.statefulSetName)
}
