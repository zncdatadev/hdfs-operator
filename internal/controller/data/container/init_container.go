package container

import (
	"github.com/zncdata-labs/hdfs-operator/internal/common"
	corev1 "k8s.io/api/core/v1"
	"strings"
)

type WaitNameNodeContainerBuilder struct {
	common.ContainerBuilder
	zookeeperDiscoveryZNode string
	instanceName            string
	groupName               string
}

func NewWaitNameNodeContainerBuilder(
	image string,
	imagePullPolicy corev1.PullPolicy,
	resource corev1.ResourceRequirements,
	zookeeperDiscoveryZNode string,
	instanceName string,
	groupName string,
) *WaitNameNodeContainerBuilder {
	return &WaitNameNodeContainerBuilder{
		ContainerBuilder:        *common.NewContainerBuilder(image, imagePullPolicy, resource),
		zookeeperDiscoveryZNode: zookeeperDiscoveryZNode,
		instanceName:            instanceName,
		groupName:               groupName,
	}
}

func (w *WaitNameNodeContainerBuilder) ContainerEnv() []corev1.EnvVar {
	return common.GetCommonContainerEnv(w.zookeeperDiscoveryZNode, WaitNameNode)
}

func (w *WaitNameNodeContainerBuilder) VolumeMount() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:      LogVolumeName(),
			MountPath: "/stackable/log",
		},
		{
			Name:      WaitNameNodeConfigVolumeName(),
			MountPath: "/stackable/mount/config/wait-for-namenodes",
		},
		{
			Name:      WaitNameNodeLogVolumeName(),
			MountPath: "/stackable/mount/log/wait-for-namenodes",
		},
	}
}

func (w *WaitNameNodeContainerBuilder) ContainerName() string {
	return string(WaitNameNode)
}

func (w *WaitNameNodeContainerBuilder) Command() []string {
	return common.GetCommonCommand()
}
func (w *WaitNameNodeContainerBuilder) CommandArgs() []string {
	return []string{`mkdir -p /stackable/config/wait-for-namenodes
cp /stackable/mount/config/wait-for-namenodes/*.xml /stackable/config/wait-for-namenodes
cp /stackable/mount/config/wait-for-namenodes/wait-for-namenodes.log4j.properties /stackable/config/wait-for-namenodes/log4j.properties
echo "Waiting for namenodes to get ready:"
n=0
while [ ${n} -lt 12 ]; 
do
    ALL_NODES_READY=true
    for namenode_id in ` + w.nameNodeIds() + `; 
    do
        echo -n "Checking pod $namenode_id... "
        SERVICE_STATE=$(/stackable/hadoop/bin/hdfs haadmin -getServiceState $namenode_id | tail -n1 || true)
        if [ "$SERVICE_STATE" = "active" ] || [ "$SERVICE_STATE" = "standby" ]; then
            echo "$SERVICE_STATE"
        else
            echo "not ready"
            ALL_NODES_READY=false
        fi
    done
    if [ "$ALL_NODES_READY" == "true" ]; then
        echo "All namenodes ready!"
        break
    fi
    echo ""
    n=$((n + 1))
    sleep 5
done
`}
}

func (w *WaitNameNodeContainerBuilder) nameNodeIds() string {
	return strings.Join(common.NameNodePodNames(w.instanceName, w.groupName), " ")
}
