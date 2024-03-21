package name

import (
	"context"
	hdfsv1alpha1 "github.com/zncdata-labs/hdfs-operator/api/v1alpha1"
	"github.com/zncdata-labs/hdfs-operator/internal/common"
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
	//TODO implement me
	panic("implement me")
}

// make core-site.xml data
func (c *ConfigMapReconciler) makeCoreSiteData() map[string]string {
	//TODO implement me
	panic("implement me")
}

// make hdfs-site.xml data
func (c *ConfigMapReconciler) makeHdfsSiteData() map[string]string {
	//TODO implement me
	panic("implement me")
}

// make log4j.properties data
func (c *ConfigMapReconciler) makeLog4jData() map[string]string {
	//TODO implement me
	panic("implement me")
}
