package name

import (
	"context"
	hdfsv1alpha1 "github.com/zncdata-labs/hdfs-operator/api/v1alpha1"
	"github.com/zncdata-labs/hdfs-operator/internal/common"
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
	//TODO implement me
	panic("implement me")
}

func (c *ConfigMapReconciler) Build(ctx context.Context) (client.Object, error) {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      createConfigName(c.Instance.GetName(), c.GroupName),
			Namespace: c.Instance.GetNamespace(),
			Labels:    c.MergedLabels,
		},
		Data: map[string]string{
			hdfsv1alpha1.CoreSiteFileName: c.makeCoreSiteData(),
			hdfsv1alpha1.HdfsSiteFileName: c.makeHdfsSiteData(),
			//hdfsv1alpha1.ser
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
	generator := &common.NameNodeHdfsSiteXmlGenerator{
		InstanceName: c.Instance.GetName(),
		GroupName:    c.GroupName,
	}
	return generator.Generate()
}

// make hadoop-policy.xml data
func (c *ConfigMapReconciler) makeHadoopPolicyData() string {
	return `<?xml version="1.0"?>
<configuration>
</configuration>`
}

// make security.properties data
func (c *ConfigMapReconciler) makeSecurityPropertiesData() string {
	return `networkaddress.cache.negative.ttl=0
networkaddress.cache.ttl=30`
}

// make ssl-client.xml data
func (c *ConfigMapReconciler) makeSslClientData() string {
	return `<?xml version="1.0"?>
<configuration>
</configuration>`
}

// make ssl-server.xml data
func (c *ConfigMapReconciler) makeSslServerData() string {
	return `<?xml version="1.0"?>
<configuration>
</configuration>`
}
