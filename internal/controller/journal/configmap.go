package journal

import (
	"context"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/controller/data/container"
	"github.com/zncdatadev/hdfs-operator/internal/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ConfigMapReconciler struct {
	common.ConfigurationStyleReconciler[*hdfsv1alpha1.HdfsCluster, *hdfsv1alpha1.JournalNodeRoleGroupSpec]
}

// NewConfigMap new a ConfigMapReconciler
func NewConfigMap(
	scheme *runtime.Scheme,
	instance *hdfsv1alpha1.HdfsCluster,
	client client.Client,
	groupName string,
	labels map[string]string,
	mergedCfg *hdfsv1alpha1.JournalNodeRoleGroupSpec,
) *ConfigMapReconciler {
	return &ConfigMapReconciler{
		ConfigurationStyleReconciler: *common.NewConfigurationStyleReconciler(
			scheme,
			instance,
			client,
			groupName,
			labels,
			mergedCfg,
		),
	}
}
func (c *ConfigMapReconciler) ConfigurationOverride(resource client.Object) {
	cm := resource.(*corev1.ConfigMap)
	overrides := c.MergedCfg.ConfigOverrides
	if overrides != nil {
		common.OverrideConfigurations(cm, overrides)
		// only name node log4j,other component log4j not override, I think it is not necessary
		if override := overrides.Log4j; override != nil {
			origin := cm.Data[common.CreateComponentLog4jPropertiesName(container.DataNode)]
			overrideContent := util.MakePropertiesFileContent(override)
			cm.Data[common.CreateComponentLog4jPropertiesName(container.DataNode)] = util.OverrideConfigFileContent(origin,
				overrideContent)
		}
	}
	c.LoggingOverride(cm)
}

func (c *ConfigMapReconciler) Build(ctx context.Context) (client.Object, error) {
	data := map[string]string{
		hdfsv1alpha1.CoreSiteFileName:     c.makeCoreSiteData(),
		hdfsv1alpha1.HdfsSiteFileName:     c.makeHdfsSiteData(),
		hdfsv1alpha1.HadoopPolicyFileName: common.MakeHadoopPolicyData(),
		hdfsv1alpha1.SecurityFileName:     common.MakeSecurityPropertiesData(),
		hdfsv1alpha1.SslClientFileName:    common.MakeSslClientData(c.Instance.Spec.ClusterConfig),
		hdfsv1alpha1.SslServerFileName:    common.MakeSslServerData(c.Instance.Spec.ClusterConfig),
		// log4j
		common.CreateComponentLog4jPropertiesName(ContainerJournalNode): common.MakeLog4jPropertiesData(ContainerJournalNode),
	}

	if isVectorEnabled, err := common.IsVectorEnable(c.MergedCfg.Config.Logging); err != nil {
		return nil, err
	} else if isVectorEnabled {
		common.ExtendConfigMapByVector(
			ctx,
			common.VectorConfigParams{
				Client:        c.Client,
				ClusterConfig: c.Instance.Spec.ClusterConfig,
				Namespace:     c.Instance.GetNamespace(),
				InstanceName:  c.Instance.GetName(),
				Role:          string(common.JournalNode),
				GroupName:     c.GroupName,
			},
			data)
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      createConfigName(c.Instance.GetName(), c.GroupName),
			Namespace: c.Instance.GetNamespace(),
			Labels:    c.MergedLabels,
		},
		Data: data,
	}, nil
}

// make core-site.xml data
func (c *ConfigMapReconciler) makeCoreSiteData() string {
	generator := &common.CoreSiteXmlGenerator{InstanceName: c.Instance.GetName()}
	return generator.EnableKerberos(c.Instance.Spec.ClusterConfig, c.Instance.Namespace).HaZookeeperQuorum().Generate()
}

// make hdfs-site.xml data
func (c *ConfigMapReconciler) makeHdfsSiteData() string {
	clusterSpec := c.Instance.Spec.ClusterConfig
	generator := common.NewNameNodeHdfsSiteXmlGenerator(
		c.Instance.GetName(), c.GroupName, c.getNameNodeReplicas(), c.Instance.Namespace,
		c.Instance.Spec.ClusterConfig, clusterSpec.ClusterDomain, clusterSpec.DfsReplication)
	return generator.EnablerKerberos(clusterSpec).EnableHttps().Generate()
}

func (c *ConfigMapReconciler) getNameNodeReplicas() int32 {
	cfg := common.GetMergedRoleGroupCfg(common.NameNode, c.Instance.GetName(), c.GroupName)
	nameNodeCfg := cfg.(*hdfsv1alpha1.NameNodeRoleGroupSpec)
	return nameNodeCfg.Replicas
}
func (c *ConfigMapReconciler) LoggingOverride(current *corev1.ConfigMap) {
	logging := NewJournalNodeLogging(c.Scheme, c.Instance, c.Client, c.GroupName, c.MergedLabels, c.MergedCfg, current)
	logging.OverrideExist(current)
}
