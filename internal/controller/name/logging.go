package name

import (
	hdfsv1alpha1 "github.com/zncdata-labs/hdfs-operator/api/v1alpha1"
	"github.com/zncdata-labs/hdfs-operator/internal/common"
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
	logDataBuilder common.RoleLoggingDataBuilder,
	role common.Role,
) *common.LoggingRecociler[*hdfsv1alpha1.HdfsCluster, any] {
	return common.NewLoggingReconciler[*hdfsv1alpha1.HdfsCluster](
		scheme,
		instance,
		client,
		groupName,
		mergedLabels,
		mergedCfg,
		logDataBuilder,
		role,
	)
}
