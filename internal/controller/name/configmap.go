package name

import (
	"context"
	hdfsv1alpha1 "github.com/zncdata-labs/hdfs-operator/api/v1alpha1"
	"github.com/zncdata-labs/hdfs-operator/internal/common"
	"github.com/zncdata-labs/hdfs-operator/internal/controller/name/container"
	"github.com/zncdata-labs/hdfs-operator/internal/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ConfigMapReconciler struct {
	common.ConfigurationStyleReconciler[*hdfsv1alpha1.HdfsCluster, *hdfsv1alpha1.RoleGroupSpec]
}

// NewConfigMap new a ConfigMapReconciler
func NewConfigMap(
	scheme *runtime.Scheme,
	instance *hdfsv1alpha1.HdfsCluster,
	client client.Client,
	groupName string,
	labels map[string]string,
	mergedCfg *hdfsv1alpha1.RoleGroupSpec,
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
	common.OverrideConfigurations(cm, overrides)
	// only name node log4j,other component log4j not override, I think it is not necessary
	if override := overrides.Log4j; override != nil {
		origin := cm.Data[common.CreateComponentLog4jPropertiesName(container.NameNode)]
		overrideContent := util.MakePropertiesFileContent(override)
		cm.Data[common.CreateComponentLog4jPropertiesName(container.NameNode)] = util.OverrideConfigFileContent(origin,
			overrideContent)
	}

}

func (c *ConfigMapReconciler) Build(_ context.Context) (client.Object, error) {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      createConfigName(c.Instance.GetName(), c.GroupName),
			Namespace: c.Instance.GetNamespace(),
			Labels:    c.MergedLabels,
		},
		Data: map[string]string{
			hdfsv1alpha1.CoreSiteFileName:     c.makeCoreSiteData(),
			hdfsv1alpha1.HdfsSiteFileName:     c.makeHdfsSiteData(),
			hdfsv1alpha1.HadoopPolicyFileName: common.MakeHadoopPolicyData(),
			hdfsv1alpha1.SecurityFileName:     common.MakeSecurityPropertiesData(),
			hdfsv1alpha1.SslClientFileName:    common.MakeSslClientData(),
			hdfsv1alpha1.SslServerFileName:    common.MakeSslServerData(),
			//log4j
			common.CreateComponentLog4jPropertiesName(container.NameNode):        common.MakeLog4jPropertiesData(container.NameNode),
			common.CreateComponentLog4jPropertiesName(container.Zkfc):            common.MakeLog4jPropertiesData(container.Zkfc),
			common.CreateComponentLog4jPropertiesName(container.FormatNameNode):  common.MakeLog4jPropertiesData(container.FormatNameNode),
			common.CreateComponentLog4jPropertiesName(container.FormatZookeeper): common.MakeLog4jPropertiesData(container.FormatZookeeper),
		},
	}, nil
}

// make core-site.xml data
func (c *ConfigMapReconciler) makeCoreSiteData() string {
	generator := &common.CoreSiteXmlGenerator{InstanceName: c.Instance.GetName()}
	return generator.Generate()
}

// make hdfs-site.xml data
func (c *ConfigMapReconciler) makeHdfsSiteData() string {
	clusterSpec := c.Instance.Spec.ClusterConfigSpec
	generator := common.NewNameNodeHdfsSiteXmlGenerator(c.Instance.GetName(), c.GroupName,
		c.MergedCfg.Replicas, c.Instance.Namespace, clusterSpec.ClusterDomain,
		clusterSpec.DfsReplication)
	return generator.Generate()
}
