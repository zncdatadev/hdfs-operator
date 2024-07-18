package container

import (
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/util"
	"maps"
	"strings"

	"github.com/zncdatadev/hdfs-operator/internal/common"
	corev1 "k8s.io/api/core/v1"
)

type WaitNameNodeContainerBuilder struct {
	common.ContainerBuilder
	zookeeperConfigMapName string
	instanceName           string
	groupName              string
	namespace              string
	clusterConfig          *hdfsv1alpha1.ClusterConfigSpec
}

func NewWaitNameNodeContainerBuilder(
	instance *hdfsv1alpha1.HdfsCluster,
	resource corev1.ResourceRequirements,
	groupName string,
) *WaitNameNodeContainerBuilder {
	imageSpec := instance.Spec.Image
	clusterConfigSpec := instance.Spec.ClusterConfigSpec
	image := util.ImageRepository(imageSpec.Repository, imageSpec.Tag)
	return &WaitNameNodeContainerBuilder{
		ContainerBuilder:       *common.NewContainerBuilder(image, imageSpec.PullPolicy, resource),
		zookeeperConfigMapName: clusterConfigSpec.ZookeeperConfigMapName,
		instanceName:           instance.Name,
		groupName:              groupName,
		namespace:              instance.Namespace,
		clusterConfig:          clusterConfigSpec,
	}
}

func (w *WaitNameNodeContainerBuilder) ContainerEnv() []corev1.EnvVar {
	return common.GetCommonContainerEnv(w.clusterConfig, WaitNameNode)
}

func (w *WaitNameNodeContainerBuilder) VolumeMount() []corev1.VolumeMount {
	mounts := common.GetCommonVolumeMounts(w.clusterConfig)
	waitNameNodeMounts := []corev1.VolumeMount{
		{
			Name:      WaitNameNodeConfigVolumeName(),
			MountPath: "/stackable/mount/config/wait-for-namenodes",
		},
		{
			Name:      WaitNameNodeLogVolumeName(),
			MountPath: "/stackable/mount/log/wait-for-namenodes",
		},
	}
	return append(mounts, waitNameNodeMounts...)
}

func (w *WaitNameNodeContainerBuilder) ContainerName() string {
	return string(WaitNameNode)
}

func (w *WaitNameNodeContainerBuilder) Command() []string {
	return common.GetCommonCommand()
}
func (w *WaitNameNodeContainerBuilder) CommandArgs() []string {
	tmpl := `mkdir -p /stackable/config/wait-for-namenodes
cp /stackable/mount/config/wait-for-namenodes/*.xml /stackable/config/wait-for-namenodes
cp /stackable/mount/config/wait-for-namenodes/wait-for-namenodes.log4j.properties /stackable/config/wait-for-namenodes/log4j.properties

{{ if .kerberosEnabled }}
{{- .kerberosEnv }}

{{- .kinitScript }}
{{- end }}

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
`
	data := common.CreateExportKrbRealmEnvData(w.clusterConfig)
	principal := common.CreateKerberosPrincipal(w.instanceName, w.namespace, GetRole())
	maps.Copy(data, common.CreateGetKerberosTicketData(principal))
	return common.ParseKerberosScript(tmpl, data)
}

func (w *WaitNameNodeContainerBuilder) nameNodeIds() string {
	return strings.Join(common.NameNodePodNames(w.instanceName, w.groupName), " ")
}
