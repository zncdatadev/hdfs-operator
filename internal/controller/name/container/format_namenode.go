package container

import (
	"fmt"
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

// FormatNameNodeContainerBuilder builds format namenode containers
type FormatNameNodeContainerBuilder struct {
	instance           *hdfsv1alpha1.HdfsCluster
	roleGroupInfo      *reconciler.RoleGroupInfo
	roleGroupConfig    *commonsv1alpha1.RoleGroupConfigSpec
	image              *oputil.Image
	nameNodeReplicates int32
	statefulSetName    string
}

// NewFormatNameNodeContainerBuilder creates a new format namenode container builder
func NewFormatNameNodeContainerBuilder(
	instance *hdfsv1alpha1.HdfsCluster,
	roleGroupInfo *reconciler.RoleGroupInfo,
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec,
	image *oputil.Image,
	nameNodeReplicates int32,
	namenodeStsName string,
) *FormatNameNodeContainerBuilder {
	return &FormatNameNodeContainerBuilder{
		instance:           instance,
		roleGroupInfo:      roleGroupInfo,
		roleGroupConfig:    roleGroupConfig,
		image:              image,
		nameNodeReplicates: nameNodeReplicates,
		statefulSetName:    namenodeStsName,
	}
}

// Build builds the format namenode container
func (b *FormatNameNodeContainerBuilder) Build() *corev1.Container {
	// Create the common container builder
	builder := common.NewHdfsContainerBuilder(
		constant.FormatNameNodeComponent,
		b.image,
		b.instance.Spec.ClusterConfig.ZookeeperConfigMapName,
		b.roleGroupInfo,
		b.roleGroupConfig,
	)

	// Create format namenode component and build container
	component := newFormatNameNodeComponent(b.instance, b.nameNodeReplicates, b.statefulSetName)

	return builder.BuildWithComponent(component)
}

// formatNameNodeComponent implements ContainerComponentInterface for FormatNameNode
type formatNameNodeComponent struct {
	instance           *hdfsv1alpha1.HdfsCluster
	nameNodeReplicates int32
	statefulSetName    string
}

// Only implement the required interface - no ports or health checks needed
var _ common.ContainerComponentInterface = &formatNameNodeComponent{}

func newFormatNameNodeComponent(instance *hdfsv1alpha1.HdfsCluster, nameNodeReplicates int32, statefulSetName string) *formatNameNodeComponent {
	return &formatNameNodeComponent{
		instance:           instance,
		nameNodeReplicates: nameNodeReplicates,
		statefulSetName:    statefulSetName,
	}
}

func (c *formatNameNodeComponent) GetContainerName() string {
	return string(constant.FormatNameNodeComponent)
}

func (c *formatNameNodeComponent) GetCommand() []string {
	return []string{"/bin/bash", "-x", "-euo", "pipefail", "-c"}
}

// todo: container name must be referenced
func (c *formatNameNodeComponent) GetArgs() []string {
	namenodeIds := strings.Join(c.podNames(), " ")
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
	data := common.CreateExportKrbRealmEnvData(c.instance.Spec.ClusterConfig)
	principal := common.CreateKerberosPrincipal(c.instance.Name, c.instance.Namespace, constant.NameNode)
	maps.Copy(data, common.CreateGetKerberosTicketData(principal))
	return common.ParseTemplate(tmpl, data)
}

// func (f *FormatNameNodeContainerBuilder) CommandArgs() []string {
// 	namenodeIds := strings.Join(f.PodNames(), " ")
// 	tmpl := `mkdir -p /kubedoop/config/format-namenodes
// cp /kubedoop/mount/config/format-namenodes/*.xml /kubedoop/config/format-namenodes
// cp /kubedoop/mount/config/format-namenodes/format-namenodes.log4j.properties /kubedoop/config/format-namenodes/log4j.properties

// {{ if .kerberosEnabled }}
// {{- .kerberosEnv }}

// {{- .kinitScript }}

// {{- end }}

// echo "Start formatting namenode $POD_NAME. Checking for active namenodes:"
// ` + fmt.Sprintf("for namenode_id in %s", namenodeIds) + "\n" +
// 		`do
//     echo -n "Checking pod $namenode_id... "
//     SERVICE_STATE=$(/kubedoop/hadoop/bin/hdfs haadmin -getServiceState $namenode_id | tail -n1 || true)
//     if [ "$SERVICE_STATE" == "active" ]
//     then
//         ACTIVE_NAMENODE=$namenode_id
//         echo "active"
//         break
//     fi
//     echo ""
// done

// if [ ! -f "/kubedoop/data/namenode/current/VERSION" ]
// then
//     if [ -z ${ACTIVE_NAMENODE+x} ]
//     then
//         echo "Create pod $POD_NAME as active namenode."
//         /kubedoop/hadoop/bin/hdfs namenode -format -noninteractive
//     else
//         echo "Create pod $POD_NAME as standby namenode."
//         /kubedoop/hadoop/bin/hdfs namenode -bootstrapStandby -nonInteractive
//     fi
// else
//     cat "/kubedoop/data/namenode/current/VERSION"
//     echo "Pod $POD_NAME already formatted. Skipping..."
// fi
// `
// 	data := common.CreateExportKrbRealmEnvData(f.clusterConfig)
// 	principal := common.CreateKerberosPrincipal(f.instanceName, f.namespace, GetRole())
// 	maps.Copy(data, common.CreateGetKerberosTicketData(principal))
// 	return common.ParseTemplate(tmpl, data)
// }

func (c *formatNameNodeComponent) GetEnvVars() []corev1.EnvVar {
	return common.GetCommonContainerEnv(c.instance.Spec.ClusterConfig, constant.FormatNameNodeComponent)
}

func (c *formatNameNodeComponent) GetVolumeMounts() []corev1.VolumeMount {
	mounts := common.GetCommonVolumeMounts(c.instance.Spec.ClusterConfig)
	formatNameNodeMounts := []corev1.VolumeMount{
		{
			Name:      hdfsv1alpha1.FormatNamenodesConfigVolumeMountName,
			MountPath: path.Join(constants.KubedoopConfigDirMount, c.GetContainerName()),
		},
		{
			Name:      hdfsv1alpha1.FormatNamenodesLogVolumeMountName,
			MountPath: path.Join(constants.KubedoopLogDirMount, c.GetContainerName()), // todo: 所有path 标准化
		},
		{
			Name:      hdfsv1alpha1.DataVolumeMountName,
			MountPath: constants.KubedoopDataDir,
		},
	}
	return append(mounts, formatNameNodeMounts...)
}

func (c *formatNameNodeComponent) podNames() []string {
	return common.CreatePodNamesByReplicas(c.nameNodeReplicates, c.statefulSetName)
}
