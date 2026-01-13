package container

import (
	"maps"
	"path"
	"strings"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/constants"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	oputil "github.com/zncdatadev/operator-go/pkg/util"
	corev1 "k8s.io/api/core/v1"
)

// WaitForNameNodesContainerBuilder builds wait-for-namenodes init containers
type WaitForNameNodesContainerBuilder struct {
	instance        *hdfsv1alpha1.HdfsCluster
	roleGroupInfo   *reconciler.RoleGroupInfo
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec
	image           *oputil.Image
}

// NewWaitForNameNodesContainerBuilder creates a new wait-for-namenodes container builder
func NewWaitForNameNodesContainerBuilder(
	instance *hdfsv1alpha1.HdfsCluster,
	roleGroupInfo *reconciler.RoleGroupInfo,
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec,
	image *oputil.Image,
) *WaitForNameNodesContainerBuilder {
	return &WaitForNameNodesContainerBuilder{
		instance:        instance,
		roleGroupInfo:   roleGroupInfo,
		roleGroupConfig: roleGroupConfig,
		image:           image,
	}
}

// Build builds the wait-for-namenodes container
func (b *WaitForNameNodesContainerBuilder) Build() *corev1.Container {
	// Create the common container builder
	builder := common.NewHdfsContainerBuilder(
		constant.WaitForNameNodesComponent,
		b.image,
		b.instance.Spec.ClusterConfig.ZookeeperConfigMapName,
		b.roleGroupInfo,
		b.roleGroupConfig,
	)

	// Create wait-for-namenodes component and build container
	component := newWaitForNameNodesComponent(b.instance, b.roleGroupInfo)

	return builder.BuildWithComponent(component)
}

// WaitForNameNodesComponent implements the component interface for wait-for-namenodes
type WaitForNameNodesComponent struct {
	instance      *hdfsv1alpha1.HdfsCluster
	roleGroupInfo *reconciler.RoleGroupInfo
}

// Compile-time check to ensure WaitForNameNodesComponent implements ContainerComponentInterface
var _ common.ContainerComponentInterface = &WaitForNameNodesComponent{}

func newWaitForNameNodesComponent(instance *hdfsv1alpha1.HdfsCluster, roleGroupInfo *reconciler.RoleGroupInfo) *WaitForNameNodesComponent {
	return &WaitForNameNodesComponent{
		instance:      instance,
		roleGroupInfo: roleGroupInfo,
	}
}

func (c *WaitForNameNodesComponent) GetContainerName() string {
	return string(constant.WaitForNameNodesComponent)
}

func (c *WaitForNameNodesComponent) GetCommand() []string {
	return []string{"/bin/bash", "-x", "-euo", "pipefail", "-c"}
}

func (c *WaitForNameNodesComponent) GetArgs() []string {
	tmpl := `mkdir -p /kubedoop/config/wait-for-namenodes
cp /kubedoop/mount/config/wait-for-namenodes/*.xml /kubedoop/config/wait-for-namenodes
cp /kubedoop/mount/config/wait-for-namenodes/wait-for-namenodes.log4j.properties /kubedoop/config/wait-for-namenodes/log4j.properties

{{ if .kerberosEnabled }}
{{- .kerberosEnv }}

{{- .kinitScript }}
{{- end }}

echo "Waiting for namenodes to get ready:"
n=0
while [ ${n} -lt 12 ];
do
    ALL_NODES_READY=true
    for namenode_id in ` + c.nameNodeIds() + `;
    do
        echo -n "Checking pod $namenode_id... "
        SERVICE_STATE=$(/kubedoop/hadoop/bin/hdfs haadmin -getServiceState $namenode_id | tail -n1 || true)
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
	data := common.CreateExportKrbRealmEnvData(c.instance.Spec.ClusterConfig)
	principal := common.CreateKerberosPrincipal(c.instance.Name, c.instance.Namespace, constant.DataNode)
	maps.Copy(data, common.CreateGetKerberosTicketData(principal))
	return common.ParseTemplate(tmpl, data)
}

func (c *WaitForNameNodesComponent) GetEnvVars() []corev1.EnvVar {
	return common.GetCommonContainerEnv(c.instance.Spec.ClusterConfig, constant.WaitForNameNodesComponent)
}

func (c *WaitForNameNodesComponent) GetVolumeMounts() []corev1.VolumeMount {
	mounts := common.GetCommonVolumeMounts(c.instance.Spec.ClusterConfig)
	waitNameNodeMounts := []corev1.VolumeMount{
		{
			Name:      hdfsv1alpha1.WaitForNamenodesConfigVolumeMountName,
			MountPath: path.Join(constants.KubedoopConfigDirMount, c.GetContainerName()),
		},
		{
			Name:      hdfsv1alpha1.WaitForNamenodesLogVolumeMountName,
			MountPath: path.Join(constants.KubedoopLogDirMount, c.GetContainerName()),
		},
	}
	return append(mounts, waitNameNodeMounts...)
}

func (c *WaitForNameNodesComponent) nameNodeIds() string {
	// Get namenode role group info from the cluster
	nameNodeRoleGroups := c.instance.Spec.NameNode.RoleGroups
	podNames := make([]string, 0, len(nameNodeRoleGroups))
	clusteInfo := c.roleGroupInfo.ClusterInfo
	for groupName, roleGroupSpec := range nameNodeRoleGroups {
		nnRoleGroupInfo := reconciler.RoleGroupInfo{
			RoleInfo: reconciler.RoleInfo{
				ClusterInfo: clusteInfo,
				RoleName:    string(constant.NameNode),
			},
			RoleGroupName: groupName,
		}
		statefulSetName := nnRoleGroupInfo.GetFullName()
		replicas := *roleGroupSpec.Replicas
		groupPodNames := common.CreatePodNamesByReplicas(replicas, statefulSetName)
		podNames = append(podNames, groupPodNames...)
	}
	return strings.Join(podNames, " ")
}
