package name

import (
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/controller/name/container"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func NewNameNodeLogging(
	scheme *runtime.Scheme,
	instance *hdfsv1alpha1.HdfsCluster,
	client client.Client,
	groupName string,
	mergedLabels map[string]string,
	mergedCfg *hdfsv1alpha1.NameNodeRoleGroupSpec,
	currentConfigMap *corev1.ConfigMap,
) *common.OverrideExistLoggingRecociler[*hdfsv1alpha1.HdfsCluster, any] {
	logDataBuilder := LogDataBuilder{
		cfg:              mergedCfg,
		currentConfigMap: currentConfigMap,
	}
	return common.NewOverrideExistLoggingRecociler(
		scheme,
		instance,
		client,
		groupName,
		mergedLabels,
		mergedCfg,
		&logDataBuilder,
	)
}

type LogDataBuilder struct {
	cfg              *hdfsv1alpha1.NameNodeRoleGroupSpec
	currentConfigMap *corev1.ConfigMap
}

func (l *LogDataBuilder) MakeContainerLogData() map[string]string {
	cmData := &l.currentConfigMap.Data
	if logging := l.cfg.Config.Logging; logging != nil {
		if nameNode := logging.NameNode; nameNode != nil {
			l.OverrideConfigMapData(cmData, container.NameNode, nameNode)
		}
		if zkfc := logging.Zkfc; zkfc != nil {
			l.OverrideConfigMapData(cmData, container.Zkfc, zkfc)
		}
		if formatNameNode := logging.FormatNameNode; formatNameNode != nil {
			l.OverrideConfigMapData(cmData, container.FormatNameNode, formatNameNode)
		}
		if formatZookeeper := logging.FormatZookeeper; formatZookeeper != nil {
			l.OverrideConfigMapData(cmData, container.FormatZookeeper, formatZookeeper)
		}
	}
	return *cmData
}

// OverrideConfigMapData override log4j properties and update the configmap
func (l *LogDataBuilder) OverrideConfigMapData(cmData *map[string]string, container common.ContainerComponent,
	containerLogSpec *hdfsv1alpha1.LoggingConfigSpec) {
	log4jBuilder := common.CreateLog4jBuilder(containerLogSpec, common.HdfsConsoleLogAppender, common.HdfsFileLogAppender)
	log4jConfigMapKey := common.CreateComponentLog4jPropertiesName(container)
	override := log4jBuilder.MakeContainerLogProperties((*cmData)[log4jConfigMapKey])
	(*cmData)[log4jConfigMapKey] = override
}
