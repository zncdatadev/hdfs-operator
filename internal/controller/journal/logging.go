package journal

import (
	hdfsv1alpha1 "github.com/zncdata-labs/hdfs-operator/api/v1alpha1"
	"github.com/zncdata-labs/hdfs-operator/internal/common"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func NewJournalNodeLogging(
	scheme *runtime.Scheme,
	instance *hdfsv1alpha1.HdfsCluster,
	client client.Client,
	groupName string,
	mergedLabels map[string]string,
	mergedCfg *hdfsv1alpha1.JournalNodeRoleGroupSpec,
	currentConfigMap *corev1.ConfigMap,
) *common.OverrideExistLoggingRecociler[*hdfsv1alpha1.HdfsCluster, any] {
	logDataBuilder := LogDataBuilder{
		cfg:              mergedCfg,
		currentConfigMap: currentConfigMap,
	}
	return common.NewOverrideExistLoggingRecociler[*hdfsv1alpha1.HdfsCluster](
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
	cfg              *hdfsv1alpha1.JournalNodeRoleGroupSpec
	currentConfigMap *corev1.ConfigMap
}

func (l *LogDataBuilder) MakeContainerLogData() map[string]string {
	cmData := &l.currentConfigMap.Data
	if logging := l.cfg.Config.Logging; logging != nil {
		if journalNode := logging.JournalNode; journalNode != nil {
			l.OverrideConfigMapData(cmData, ContainerJournalNode, journalNode)
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
